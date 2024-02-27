import sys
from pathlib import Path

current_dir = Path(__file__).resolve().parent

sys.path.append(str(current_dir / "src" / "indexer"))

from indexer import Indexer

indexer = Indexer()

indexer.scan_for_files(root_dir="/storage/trex-dm/Canfranc/")

indexer.process_files()
