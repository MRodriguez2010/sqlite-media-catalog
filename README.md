# SQLite Media Catalog

A media library can have many readers while only one machine needs to inspect its files. This Python pipeline scans a directory, measures video duration with FFprobe, and publishes a read-only SQLite snapshot for downstream queries. Each snapshot has a generation number, a row count, and a SHA-256 checksum.

## Try it with synthetic media

On Linux with Python 3.10+ and FFmpeg installed, run the integration test from the repository root.

```sh
python3 -m unittest -v
```

The test generates a two-second blue frame sequence, indexes it, and queries the snapshot. It checks that a second scan reuses the measurement, that removing the file drops the row from the next snapshot, and that a missing input directory leaves the last publication intact.

## Index a directory

```sh
python3 catalog.py --root ./media --state ./work/state.sqlite3 --publish-dir ./work/snapshots
```

A single writer keeps a working database and reuses measurements when file size and modification time match. A bounded pool probes changed files. A successful publication runs SQLite’s integrity check, writes a new snapshot, and atomically replaces the current manifest. The publisher retains the three most recent snapshots.

Read `work/snapshots/current.json` to locate the current database. Consumers can query it with ordinary SQL.

```sql
SELECT relative_path, duration_text
FROM durations
ORDER BY duration_ms DESC;
```

This repository contains the indexing and publication code from an independent file-catalog project. The test creates all sample media in a temporary directory. A catalog made from your own files contains their relative names and should be stored accordingly.
