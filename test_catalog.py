import hashlib
import json
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

SCRIPT = Path(__file__).with_name("catalog.py")


class CatalogTest(unittest.TestCase):
    def test_incremental_publication(self):
        with tempfile.TemporaryDirectory() as temp:
            work = Path(temp)
            media = work / "input"
            media.mkdir()
            clip = media / "sample.mp4"
            subprocess.run([
                "ffmpeg", "-v", "error", "-f", "lavfi", "-i",
                "color=c=blue:s=32x32:r=10", "-t", "2", "-c:v", "mpeg4",
                str(clip),
            ], check=True)
            output = work / "snapshots"
            command = [
                sys.executable, str(SCRIPT), "--root", str(media),
                "--state", str(work / "state.sqlite3"), "--publish-dir", str(output),
            ]
            subprocess.run(command, check=True, capture_output=True)
            manifest_file = output / "current.json"
            manifest = json.loads(manifest_file.read_text())
            snapshot = output / manifest["database"]
            self.assertEqual(manifest["entries"], 1)
            digest = hashlib.sha256(snapshot.read_bytes()).hexdigest()
            self.assertEqual(manifest["sha256"], digest)
            with sqlite3.connect(snapshot.as_uri() + "?mode=ro", uri=True) as db:
                self.assertEqual(db.execute(
                    "SELECT relative_path, duration_ms FROM durations"
                ).fetchall(), [("sample.mp4", 2000)])
            rerun = subprocess.run(command, check=True, capture_output=True, text=True)
            self.assertIn("reused=1 to_probe=0", rerun.stdout)
            clip.unlink()
            subprocess.run(command, check=True, capture_output=True)
            self.assertEqual(json.loads(manifest_file.read_text())["entries"], 0)
            self.assertEqual(hashlib.sha256(snapshot.read_bytes()).hexdigest(), digest)

            last_manifest = manifest_file.read_bytes()
            media.rmdir()
            self.assertNotEqual(subprocess.run(command, capture_output=True).returncode, 0)
            self.assertEqual(manifest_file.read_bytes(), last_manifest)


if __name__ == "__main__":
    unittest.main()
