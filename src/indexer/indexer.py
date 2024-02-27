from __future__ import annotations

from queue import Queue
import threading
from pathlib import Path
import pickle
import concurrent.futures
from REST import ROOT

import hashlib
import threading
from pathlib import Path

current_dir = Path(__file__).resolve().parent

setup_sql = """
CREATE TABLE IF NOT EXISTS files (hash TEXT PRIMARY KEY, filename TEXT, timestamp INTEGER);
CREATE TABLE IF NOT EXISTS signals (data BLOB NOT NULL, event_id INTEGER, signal_id INTEGER, timestamp INTEGER, hash TEXT, FOREIGN KEY (hash) REFERENCES files(hash), PRIMARY KEY (event_id, signal_id, hash));
""".strip()


def compute_file_hash(filepath: str | Path, hash_algorithm: str = 'sha256') -> str:
    """
    Compute the hash of a file's contents.

    :param filepath: The path to the file.
    :param hash_algorithm: The hashing algorithm to use (default is SHA-256).
    :return: The computed hash digest.
    """
    # Create a hash object based on the specified algorithm
    hasher = hashlib.new(hash_algorithm)

    # Read the file in binary mode and update the hash object
    with open(filepath, 'rb') as file:
        # Read the file in chunks to handle large files efficiently
        for chunk in iter(lambda: file.read(4096), b''):
            hasher.update(chunk)

    # Return the hexadecimal representation of the digest
    return hasher.hexdigest()


class Indexer:
    def __init__(self):
        self._unfiltered_files = Queue()  # files with .root extension and read permissions
        self._filtered_files = Queue()  # files that have the object we are looking for

        self._scan_thread = None
        self._cache_dir = current_dir.parent.parent / ".cache"

        self._filtered_files_set = set()

        self._load_cache()

    def _load_cache(self):
        if self._cache_dir.exists():
            if (self._cache_dir / "filtered_files.pkl").exists():
                with open(self._cache_dir / "filtered_files.pkl", "rb") as f:
                    self._filtered_files_set = pickle.load(f)

    def scan_for_files(self, *, root_dir: str = "/", extension: str = "root"):
        # spawn a daemon thread that scans for files with a given extension and where we have read permissions
        if self._scan_thread and self._scan_thread.is_alive():
            raise RuntimeError('Scan thread already running')

        def scan():
            root_path = Path(root_dir)
            for filepath in root_path.rglob(f'*.{extension}'):
                # check read permissions
                if filepath.is_file() and filepath.stat().st_mode & 0o400:
                    self._unfiltered_files.put(str(filepath))
                    print(f"Found {filepath}")

        self._scan_thread = threading.Thread(target=scan, daemon=True)
        self._scan_thread.start()

    def _process_file(self, filepath: str | Path):
        print(f"Processing {filepath}")

        try:
            run = ROOT.TRestRun(str(filepath))
            event = run.GetInputEvent()
            is_valid = str(event.GetName()) != "TRestRawSignalEvent"
            print(f"Processing {filepath} - {is_valid}")
            if is_valid:
                self._filtered_files.put(filepath)

        except Exception as e:
            print(f"Error processing {filepath}: {e}")

    def process_files(self):
        with concurrent.futures.ThreadPoolExecutor() as executor:
            while True:
                if not self._unfiltered_files.empty():
                    print("Number of filtered files:", self._filtered_files.qsize())

                    filepath = self._unfiltered_files.get()

                    try:
                        executor.submit(self._process_file, filepath)
                    except Exception as e:
                        print(f"Error submitting {filepath}: {e}")
