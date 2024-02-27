from __future__ import annotations

import hashlib

from REST import ROOT

import sqlite3
from pathlib import Path
import numpy as np

setup_sql = """
CREATE TABLE IF NOT EXISTS files (hash TEXT PRIMARY KEY, filename TEXT, timestart INTEGER, timeend INTEGER, detector TEXT, driftfield INTEGER, amplificationfield INTEGER, pressure DOUBLE, gain TEXT, clock TEXT, shaping TEXT);
CREATE TABLE IF NOT EXISTS signals (data BLOB NOT NULL, event_id INTEGER, signal_id INTEGER, timestamp INTEGER, hash TEXT, FOREIGN KEY (hash) REFERENCES files(hash), PRIMARY KEY (event_id, signal_id, hash));
""".strip()

database_file = Path("/tmp/signals.db")

filename = Path(
    "/storage/trex-dm/Canfranc/quickAnalysis/data/R01997_00000_RawData_Background_Alphas_cronTREX_V2.3.15.root")

conn = None


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
    data = np.zeros(512, dtype=np.int16)
    time_start = run.GetStartTimestamp()
    time_end = run.GetEndTimestamp()
    detector_value = run.GetMetadataClass("TRestDetector").GetDataMemberValue("fDetectorName")
    drift_value = run.GetMetadataClass("TRestDetector").GetDataMemberValue("fDriftField")
    amplification_value = run.GetMetadataClass("TRestDetector").GetDataMemberValue("fAmplificationVoltage")
    pressure_value = run.GetMetadataClass("TRestDetector").GetDataMemberValue("fPressure")
    gain_value = run.GetMetadataClass("TRestDetector").GetDataMemberValue("fElectronicsGain")
    clock_value = run.GetMetadataClass("TRestDetector").GetDataMemberValue("fElectronicsClock")
    shaping_value = run.GetMetadataClass("TRestDetector").GetDataMemberValue("fElectronicsShaping")
    
    conn.execute('INSERT INTO files (hash, filename, timestart, timeend, detector, driftfield, amplificationfield, pressure, gain, clock, shaping) VALUES (?, ?, ?)',
                 (file_hash, str(filename), time_start, time_end, detector_value, drift_value, amplification_value, pressure_value, gain_value, clock_value, shaping_value))

    entries = run.GetEntries()
    for event_index in range(entries):
        print(f"file: {filename}, event: {event_index}/{entries}")
        run.GetEntry(event_index)
        event_id = event.GetID()
        for signal_index in range(event.GetNumberOfSignals()):
            signal = event.GetSignal(signal_index)
            signal_id = signal.GetID()
            timestamp = event.GetTime()  # TODO: get timestamp for the signal
            if signal.GetNumberOfPoints() != len(data):
                raise ValueError(f"Unexpected number of points: {signal.GetNumberOfPoints()}")
            for data_index in range(len(data)):
                data[data_index] = signal.GetData(data_index)

            conn.execute('INSERT INTO signals (data, event_id, signal_id, timestamp, hash) VALUES (?, ?, ?, ?, ?)',
                         (data.tobytes(), event_id, signal_id, timestamp, file_hash))

    conn.commit()


conn = sqlite3.connect(database_file)
conn.execute('PRAGMA foreign_keys = ON')
conn.executescript(setup_sql)
conn.commit()

process_file(filename)

conn.close()

if __name__ == "__main__":
    ...
