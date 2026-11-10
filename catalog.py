#!/usr/bin/env python3
"""Build and publish a shared, read-only NAS video-duration catalog.

The working database remains on the NAS's local POSIX filesystem. Completed
catalogs are published as immutable SQLite snapshots. Clients never write to
the shared database.
"""

from __future__ import annotations

import argparse
import concurrent.futures
import datetime as dt
import fcntl
import hashlib
import json
import os
import signal
import shutil
import sqlite3
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator, Optional


SCHEMA_VERSION = 1
VIDEO_EXTENSIONS = {
    ".3gp",
    ".avi",
    ".flv",
    ".m2ts",
    ".m4v",
    ".mkv",
    ".mov",
    ".mp4",
    ".mpeg",
    ".mpg",
    ".mts",
    ".ogv",
    ".vob",
    ".webm",
    ".wmv",
}
EXCLUDED_DIRECTORIES = {
    "#recycle",
    ".Trash-1000",
    ".cache",
    ".config",
    ".git",
    ".local",
    ".ssh",
}
ERROR_RETRY_SECONDS = 24 * 60 * 60


@dataclass(frozen=True)
class VideoFile:
    share: str
    relative_path: str
    path: Path
    size: int
    mtime_ns: int


@dataclass(frozen=True)
class ProbeResult:
    duration_ms: Optional[int]
    duration_text: str
    error: str


def utc_now() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds")


def format_duration(duration_ms: int) -> str:
    total_seconds = max(0, int((duration_ms + 500) // 1000))
    hours, remainder = divmod(total_seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    return f"{hours:02d}:{minutes:02d}:{seconds:02d}"


def connect_state(path: Path) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    database = sqlite3.connect(path)
    database.execute("PRAGMA journal_mode=WAL")
    database.execute("PRAGMA synchronous=NORMAL")
    database.execute(
        """
        CREATE TABLE IF NOT EXISTS videos (
            share TEXT NOT NULL,
            relative_path TEXT NOT NULL,
            size INTEGER NOT NULL,
            mtime_ns INTEGER NOT NULL,
            duration_ms INTEGER,
            duration_text TEXT NOT NULL DEFAULT '',
            last_probe_ns INTEGER NOT NULL DEFAULT 0,
            last_seen_generation INTEGER NOT NULL DEFAULT 0,
            error TEXT NOT NULL DEFAULT '',
            PRIMARY KEY (share, relative_path)
        )
        """
    )
    database.execute(
        """
        CREATE TABLE IF NOT EXISTS metadata (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        )
        """
    )
    database.commit()
    return database


def iter_video_files(share: str, root: Path) -> Iterator[VideoFile]:
    stack = [root]
    while stack:
        directory = stack.pop()
        try:
            with os.scandir(directory) as entries:
                for entry in entries:
                    try:
                        if entry.is_dir(follow_symlinks=False):
                            if entry.name not in EXCLUDED_DIRECTORIES:
                                stack.append(Path(entry.path))
                            continue
                        if not entry.is_file(follow_symlinks=False):
                            continue
                        path = Path(entry.path)
                        if path.suffix.casefold() not in VIDEO_EXTENSIONS:
                            continue
                        stat_result = entry.stat(follow_symlinks=False)
                        yield VideoFile(
                            share=share.casefold(),
                            relative_path=path.relative_to(root).as_posix(),
                            path=path,
                            size=int(stat_result.st_size),
                            mtime_ns=int(stat_result.st_mtime_ns),
                        )
                    except FileNotFoundError:
                        continue
        except FileNotFoundError:
            if directory == root:
                raise


def probe_duration(video: VideoFile, timeout_seconds: int) -> ProbeResult:
    command = [
        "ffprobe",
        "-v",
        "error",
        "-show_entries",
        "format=duration:stream=duration",
        "-of",
        "json",
        str(video.path),
    ]
    try:
        completed = subprocess.run(
            command,
            check=False,
            capture_output=True,
            text=True,
            timeout=timeout_seconds,
        )
    except subprocess.TimeoutExpired:
        return ProbeResult(None, "", f"timeout after {timeout_seconds}s")
    except OSError as error:
        return ProbeResult(None, "", f"{type(error).__name__}: {error}")
    if completed.returncode != 0:
        error = completed.stderr.strip().replace("\n", " ")[:500]
        return ProbeResult(None, "", error or f"ffprobe exit {completed.returncode}")
    try:
        data = json.loads(completed.stdout)
    except json.JSONDecodeError as error:
        return ProbeResult(None, "", f"invalid ffprobe JSON: {error}")

    candidates = [data.get("format", {}).get("duration")]
    candidates.extend(stream.get("duration") for stream in data.get("streams", []))
    durations = []
    for candidate in candidates:
        try:
            value = float(candidate)
        except (TypeError, ValueError):
            continue
        if value > 0:
            durations.append(value)
    if not durations:
        return ProbeResult(None, "", "duration unavailable")
    duration_ms = int(round(max(durations) * 1000))
    return ProbeResult(duration_ms, format_duration(duration_ms), "")


def scan_and_update(
    database: sqlite3.Connection,
    roots: tuple[tuple[str, Path], ...],
    workers: int,
    timeout_seconds: int,
) -> tuple[int, int, int]:
    generation = time.time_ns()
    now_ns = time.time_ns()
    retry_cutoff_ns = now_ns - ERROR_RETRY_SECONDS * 1_000_000_000
    existing = {
        (row[0], row[1]): row[2:]
        for row in database.execute(
            """
            SELECT share, relative_path, size, mtime_ns, duration_text,
                   last_probe_ns
            FROM videos
            """
        )
    }

    discovered = 0
    reused = 0
    tasks: list[VideoFile] = []
    seen_updates = []
    for share, root in roots:
        if not root.is_dir():
            raise FileNotFoundError(f"Media directory unavailable: {root}")
        for video in iter_video_files(share, root):
            discovered += 1
            previous = existing.get((video.share, video.relative_path))
            if previous:
                old_size, old_mtime_ns, old_duration, old_probe_ns = previous
                unchanged = (
                    int(old_size) == video.size and int(old_mtime_ns) == video.mtime_ns
                )
                reusable = bool(old_duration) or int(old_probe_ns) >= retry_cutoff_ns
                if unchanged and reusable:
                    reused += 1
                    seen_updates.append(
                        (generation, video.share, video.relative_path)
                    )
                else:
                    tasks.append(video)
            else:
                tasks.append(video)

            if len(seen_updates) >= 1000:
                database.executemany(
                    """
                    UPDATE videos SET last_seen_generation=?
                    WHERE share=? AND relative_path=?
                    """,
                    seen_updates,
                )
                database.commit()
                seen_updates.clear()
    if seen_updates:
        database.executemany(
            """
            UPDATE videos SET last_seen_generation=?
            WHERE share=? AND relative_path=?
            """,
            seen_updates,
        )
        database.commit()

    print(
        f"scan discovered={discovered} reused={reused} to_probe={len(tasks)}",
        flush=True,
    )
    completed_count = 0
    successful_count = 0
    last_report = time.monotonic()
    worker_count = max(1, workers)
    with concurrent.futures.ThreadPoolExecutor(max_workers=worker_count) as pool:
        task_iterator = iter(tasks)
        pending: dict[concurrent.futures.Future[ProbeResult], VideoFile] = {}

        def fill_queue() -> None:
            while len(pending) < worker_count * 4:
                try:
                    next_video = next(task_iterator)
                except StopIteration:
                    return
                pending[pool.submit(probe_duration, next_video, timeout_seconds)] = (
                    next_video
                )

        fill_queue()
        while pending:
            completed, _ = concurrent.futures.wait(
                pending, return_when=concurrent.futures.FIRST_COMPLETED
            )
            for future in completed:
                video = pending.pop(future)
                fill_queue()
                try:
                    result = future.result()
                except Exception as error:
                    result = ProbeResult(None, "", f"{type(error).__name__}: {error}")
                database.execute(
                    """
                    INSERT INTO videos(
                        share, relative_path, size, mtime_ns, duration_ms,
                        duration_text, last_probe_ns, last_seen_generation, error
                    ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(share, relative_path) DO UPDATE SET
                        size=excluded.size,
                        mtime_ns=excluded.mtime_ns,
                        duration_ms=excluded.duration_ms,
                        duration_text=excluded.duration_text,
                        last_probe_ns=excluded.last_probe_ns,
                        last_seen_generation=excluded.last_seen_generation,
                        error=excluded.error
                    """,
                    (
                        video.share,
                        video.relative_path,
                        video.size,
                        video.mtime_ns,
                        result.duration_ms,
                        result.duration_text,
                        time.time_ns(),
                        generation,
                        result.error,
                    ),
                )
                completed_count += 1
                if result.duration_text:
                    successful_count += 1
                if completed_count % 100 == 0:
                    database.commit()
                if (
                    completed_count % 500 == 0
                    or time.monotonic() - last_report >= 30
                    or completed_count == len(tasks)
                ):
                    print(
                        f"probe progress={completed_count}/{len(tasks)} "
                        f"successful={successful_count}",
                        flush=True,
                    )
                    last_report = time.monotonic()
    database.commit()

    database.execute(
        "DELETE FROM videos WHERE last_seen_generation != ?", (generation,)
    )
    database.execute(
        "INSERT OR REPLACE INTO metadata(key, value) VALUES('last_full_scan', ?)",
        (utc_now(),),
    )
    database.execute(
        "INSERT OR REPLACE INTO metadata(key, value) VALUES('generation', ?)",
        (str(generation),),
    )
    database.commit()
    return discovered, reused, successful_count


def fsync_path(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def publish_snapshot(
    state: sqlite3.Connection,
    publish_dir: Path,
) -> dict[str, object]:
    publish_dir.mkdir(parents=True, exist_ok=True)
    generation = str(time.time_ns())
    filename = f"catalog-{generation}.sqlite3"
    final_path = publish_dir / filename
    temporary_path = publish_dir / f".{filename}.tmp"
    temporary_path.unlink(missing_ok=True)

    snapshot = sqlite3.connect(temporary_path)
    snapshot.execute("PRAGMA journal_mode=DELETE")
    snapshot.execute("PRAGMA synchronous=FULL")
    snapshot.execute(
        """
        CREATE TABLE durations (
            share TEXT NOT NULL COLLATE NOCASE,
            relative_path TEXT NOT NULL,
            size INTEGER NOT NULL,
            mtime_ns INTEGER NOT NULL,
            duration_ms INTEGER NOT NULL,
            duration_text TEXT NOT NULL,
            indexed_ns INTEGER NOT NULL,
            PRIMARY KEY (share, relative_path)
        ) WITHOUT ROWID
        """
    )
    rows = state.execute(
        """
        SELECT share, relative_path, size, mtime_ns, duration_ms,
               duration_text, last_probe_ns
        FROM videos
        WHERE duration_text != '' AND duration_ms IS NOT NULL
        ORDER BY share, relative_path
        """
    )
    snapshot.executemany(
        "INSERT INTO durations VALUES(?, ?, ?, ?, ?, ?, ?)", rows
    )
    snapshot.execute("CREATE TABLE metadata (key TEXT PRIMARY KEY, value TEXT NOT NULL)")
    entries = int(snapshot.execute("SELECT COUNT(*) FROM durations").fetchone()[0])
    metadata = {
        "schema_version": str(SCHEMA_VERSION),
        "generation": generation,
        "published_at": utc_now(),
        "entries": str(entries),
    }
    snapshot.executemany(
        "INSERT INTO metadata(key, value) VALUES(?, ?)", metadata.items()
    )
    snapshot.commit()
    check = snapshot.execute("PRAGMA quick_check").fetchone()[0]
    snapshot.close()
    if check != "ok":
        temporary_path.unlink(missing_ok=True)
        raise RuntimeError(f"snapshot integrity check failed: {check}")

    os.chmod(temporary_path, 0o644)
    fsync_path(temporary_path)
    os.replace(temporary_path, final_path)
    digest = hashlib.sha256(final_path.read_bytes()).hexdigest()
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "generation": generation,
        "database": filename,
        "sha256": digest,
        "published_at": metadata["published_at"],
        "entries": entries,
    }
    manifest_temp = publish_dir / ".current.json.tmp"
    manifest_temp.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    os.chmod(manifest_temp, 0o644)
    fsync_path(manifest_temp)
    os.replace(manifest_temp, publish_dir / "current.json")

    snapshots = sorted(
        publish_dir.glob("catalog-*.sqlite3"), key=lambda path: path.stat().st_mtime_ns
    )
    for old_snapshot in snapshots[:-3]:
        if old_snapshot != final_path:
            old_snapshot.unlink(missing_ok=True)
    print(f"published generation={generation} entries={entries}", flush=True)
    return manifest


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, required=True, help="Directory containing the media to index")
    parser.add_argument("--state", type=Path, required=True)
    parser.add_argument("--publish-dir", type=Path, required=True)
    parser.add_argument("--workers", type=int, default=2)
    parser.add_argument("--timeout", type=int, default=20)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if shutil.which("ffprobe") is None:
        sys.exit("Install FFmpeg to provide the ffprobe command.")
    lock_path = args.state.with_suffix(".lock")
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with lock_path.open("w", encoding="utf-8") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            print("catalog update already running", flush=True)
            return 0

        database = connect_state(args.state)
        scan_and_update(database, (("media", args.root.resolve()),), args.workers, args.timeout)
        publish_snapshot(database, args.publish_dir)
        database.close()
    return 0


if __name__ == "__main__":
    signal.signal(signal.SIGPIPE, signal.SIG_DFL)
    sys.exit(main())
