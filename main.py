from __future__ import annotations

import hashlib

from REST import ROOT

import sqlite3
from pathlib import Path

setup_sql = """
CREATE TABLE IF NOT EXISTS files (hash TEXT PRIMARY KEY, filename TEXT, timestamp INTEGER);
CREATE TABLE IF NOT EXISTS signals (data BLOB NOT NULL, event_id INTEGER, signal_id INTEGER, timestamp INTEGER, hash TEXT, FOREIGN KEY (hash) REFERENCES files(hash), PRIMARY KEY (event_id, signal_id, hash));
""".strip()

database_file = Path("/tmp/signals.db")

conn = sqlite3.connect(database_file)

conn.execute('PRAGMA foreign_keys = ON')
conn.executescript(setup_sql)
conn.commit()
conn.close()

filename = Path(
    "/storage/trex-dm/Canfranc/quickAnalysis/data/R01997_00000_RawData_Background_Alphas_cronTREX_V2.3.15.root")


def compute_file_hash(filepath, hash_algorithm='sha256'):
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


def process_file(filename: str | Path):
    run = ROOT.TRestRun(str(filename))
    event = run.GetInputEvent()
    if str(event.GetName()) != "TRestRawSignalEvent":
        raise ValueError(f"Unexpected event type: {event.GetName()}")

    file_hash = compute_file_hash(filename)

    for event_index in range(run.GetEntries()):
        run.GetEntry(event_index)
        for signal_index in range(event.GetNumberOfSignals()):
            signal = event.GetSignal(signal_index)


process_file(filename)

if __name__ == "__main__":
    ...
