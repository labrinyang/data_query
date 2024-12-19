import requests
import csv
import time
import os
import sys
import logging
from logging.handlers import RotatingFileHandler

ASSET = 'BTC-27DEC24'
API_URL = f"https://www.deribit.com/api/v2/public/ticker?instrument_name={ASSET}"
CSV_FILE = f"{ASSET}.csv"
LOG_FILE = f"{ASSET}_collector.log"
FIELDS = ["usOut", "index_price", "max_price", "best_ask_price", "best_bid_price"]

# Configure logging with rotating file handler
logger = logging.getLogger()
logger.setLevel(logging.INFO)
handler = RotatingFileHandler(LOG_FILE, maxBytes=5*1024*1024, backupCount=5)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

def initialize_csv(file_name):
    if not os.path.isfile(file_name):
        try:
            with open(file_name, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=FIELDS)
                writer.writeheader()
            logger.info("CSV file created and headers written.")
        except Exception as e:
            logger.error(f"Error initializing CSV file: {e}")
            sys.exit(1)
    else:
        logger.info("CSV file exists. Appending data.")

def fetch_data(retries=3, delay=5):
    for attempt in range(retries):
        try:
            response = requests.get(API_URL, timeout=10)
            response.raise_for_status()
            data = response.json()

            us_out = data.get("usOut") or data.get("result", {}).get("usOut")
            index_price = data.get("result", {}).get("index_price")
            max_price = data.get("result", {}).get("max_price")
            best_ask_price = data.get("result", {}).get("best_ask_price")
            best_bid_price = data.get("result", {}).get("best_bid_price")

            if None in [us_out, index_price, max_price, best_ask_price, best_bid_price]:
                logger.warning(f"Missing fields. Response: {data}")
                return None

            return {
                "usOut": us_out,
                "index_price": index_price,
                "max_price": max_price,
                "best_ask_price": best_ask_price,
                "best_bid_price": best_bid_price
            }

        except requests.RequestException as e:
            logger.error(f"Request error (Attempt {attempt + 1}/{retries}): {e}")
            time.sleep(delay)
        except ValueError:
            logger.error("JSON decoding failed.")
            return None
    logger.error("All retry attempts failed.")
    return None

def save_to_csv(file_name, data):
    try:
        with open(file_name, 'a', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=FIELDS)
            writer.writerow(data)
        logger.info(f"Data saved: {data}")
    except Exception as e:
        logger.error(f"Error saving data to CSV: {e}")

def main():
    initialize_csv(CSV_FILE)
    logger.info("Starting data collection every 30 seconds. Press Ctrl+C to stop.")
    print("Starting data collection every 30 seconds. Press Ctrl+C to stop.")

    while True:
        data = fetch_data()
        if data:
            save_to_csv(CSV_FILE, data)
        time.sleep(30)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.info("Program stopped by user.")
        print("\nProgram stopped by user.")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        print(f"Unexpected error: {e}")
