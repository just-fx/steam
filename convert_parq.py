import pandas as pd
import logging
from datetime import datetime
from pathlib import Path

# Constants
today = datetime.today().strftime("%Y-%m-%d")

JSON_DIR = Path("/home/ec2-user/data/json")
PARQUET_DIR = Path("/home/ec2-user/data/parquet")
LOG_FILE = f"/home/ec2-user/logs/parquet_convert_{today}.log"

# Set up logging
logging.basicConfig(filename=LOG_FILE, level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger()

def convert_json(source: Path, target: Path):
    try:
        with pd.read_json(source, lines=True, chunksize=10000) as reader:
            for i, chunk in enumerate(reader):
                if not target.exists() or i == 0:
                    chunk.to_parquet(target, engine='fastparquet')
                else:
                    chunk.to_parquet(target, engine='fastparquet', append=True)
        logger.info(f"Successfully converted {source.name} to {target.name}")
    except Exception as e:
        logger.error(f"Error processing {source.name}: {e}")

def main():
    # Make sure target directory exists
    PARQUET_DIR.mkdir(parents=True, exist_ok=True)

    # Get all JSONL files that match the naming pattern
    jsonl_files = sorted(JSON_DIR.glob("steam_data_*.jsonl"))

    if not jsonl_files:
        logger.warning("No JSONL files found to process.")
        return

    for jsonl_file in jsonl_files:
        target_file = PARQUET_DIR / f"{jsonl_file.stem}.parquet"
        convert_json(jsonl_file, target_file)

if __name__ == "__main__":
    main()
