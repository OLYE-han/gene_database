"""Reprocess captured JSON shards after a benchmark; never runs a search."""
from pathlib import Path
import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[1]/'python'))
from dhdgs.benchmark_process import main

if __name__ == '__main__':
    main()
