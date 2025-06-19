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
        first_chunk = True
        with open(source, 'rb') as f:
            reader = pd.read_json(f, lines=True, chunksize=10000)
            for i, chunk in enumerate(reader):
                logger.info(f"Processing chunk {i+1} with {len(chunk)} rows")
                chunk.to_parquet(
                    target,
                    engine='fastparquet',
                    append=not first_chunk,
                    compression='snappy',
                )
                first_chunk = False
        logger.info(f"Successfully converted {source.name} to {target.name}")
    except Exception as e:
        logger.error(f"Error processing {source.name}: {e}", exc_info=True)


def main():
    # Make sure target directory exists
    PARQUET_DIR.mkdir(parents=True, exist_ok=True)

    # Get all JSONL files
    jsonl_files = sorted(JSON_DIR.glob("steam_data_*.jsonl"))

    if not jsonl_files:
        logger.warning("No JSONL files found to process.")
        return

    # Filter only files that haven't been converted yet
    files_to_process = [
        f for f in jsonl_files if not (PARQUET_DIR / f"{f.stem}.parquet").exists()
    ]

    if not files_to_process:
        logger.info("All JSONL files have already been converted. Exiting.")
        return

    for jsonl_file in files_to_process:
        target_file = PARQUET_DIR / f"{jsonl_file.stem}.parquet"
        convert_json(jsonl_file, target_file)

if __name__ == "__main__":
    main()
