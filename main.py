import requests
import time
import pandas as pd
import json
from requests.exceptions import HTTPError
import logging
from datetime import datetime
import os
import boto3


today = datetime.today().strftime("%Y-%m-%d")

REGION = "ph"
LANGUAGE = "english"
OUTPUT_DIR = "/home/ec2-user/data"
JSONL_FILE = f"{OUTPUT_DIR}/json/steam_data_{today}.jsonl"
LOG_DIR = "/home/ec2-user/logs"
LOG_FILE = f"/home/ec2-user/logs/steam_scraper_{today}.log"
S3_BUCKET = "dlsu-steam-data-bucket"
SLEEP_TIME = 1
MAX_RETRIES = 5

logging.basicConfig(filename=LOG_FILE, level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger()


def get_all_app_ids():
    try:
        resp = requests.get("https://api.steampowered.com/ISteamApps/GetAppList/v2/")
        resp.raise_for_status()
        data = resp.json()
        apps = data["applist"]["apps"]
        return [app["appid"] for app in apps if app["name"]]
    except Exception as e:
        log.error("Failed to fetch app list", exc_info=True)
        return []


def get_app_details(app_id, max_retries=3):
    url = "https://store.steampowered.com/api/appdetails"
    params = {"appids": app_id, "cc": REGION, "l": LANGUAGE}
    for attempt in range(max_retries):
        try:
            resp = requests.get(url, params=params)
            resp.raise_for_status()
            data = resp.json()
            return data[str(app_id)]["data"]
        except HTTPError as e:
            if resp.status_code == 429:
                wait_time = 2**attempt
                log.warning(f"429 Too Many Requests for app {app_id}. Sleeping for {wait_time}s before retrying...")
                time.sleep(wait_time)
            else:
                log.warning(f"HTTP error for app {app_id}: {e}")
                break
        except Exception as e:
            log.warning(f"App ID {app_id} failed.", exc_info=True)
            return None
    log.error(f"App ID {app_id} failed after {max_retries}")
    return None


def main():
    start_time = time.time()
    log.info("Starting Steam data scrape")
    app_ids = get_all_app_ids()

    if not app_ids:
        log.error("No app IDs found. Aborting.")
        return

    for i, app_id in enumerate(app_ids):
        log.info(f"Processing app {i + 1}/{len(app_ids)}: {app_id}")
        details = get_app_details(app_id, max_retries=MAX_RETRIES)
        if details:
            with open(JSONL_FILE, "a", encoding="utf-8") as f:
                f.write(json.dumps(details) + "\n")

        time.sleep(SLEEP_TIME)

    duration = time.time() - start_time
    size_mb = os.path.getsize(JSONL_FILE) / (1024**2)
    log.info(f"Scraping completed in {duration / 60:.2f} minutes. Size: {size_mb:.2f} MB")

    log.info(f"Uploading jsonl to s3 bucket {S3_BUCKET}")
    s3 = boto3.client("s3")
    s3.upload_file(JSONL_FILE, S3_BUCKET, JSONL_FILE)


if __name__ == "__main__":
    main()
