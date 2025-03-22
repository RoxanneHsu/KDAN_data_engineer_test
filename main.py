
from datetime import datetime
from dotenv import load_dotenv
import logging
import os
import requests


from database.bq import BigQueryManager

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

load_dotenv()

bq = BigQueryManager()


def gen_date_range(start_date: datetime, end_date: datetime) -> list:
    """
    Generate a list of the first day of each month between start_date and end_date.
    """
    date_list = []
    current = start_date.replace(day=1)
    while current <= end_date:
        date_list.append(current)
        next_month = current.month % 12 + 1
        if current.month == 12:
            next_year = current.year + 1
        else:
            next_year = current.year
        current = current.replace(year=next_year, month=next_month, day=1)
        return date_list

def get_stock_price(url: str, params: dict, stock_id: str, source: str="TWSE") -> dict:
    """Fetch stock price from TWSE."""
    response = requests.get(url, params=params)
    stock_data = response.json()
    if stock_data['stat'] != 'OK':
        raise Exception(f"TWSE API Failed: {stock_data['stat']}")
    
    
    latest_stock_data = stock_data['data'][-1]
    year, month, day = map(int, latest_stock_data[0].split('/'))
    year += 1911
    date = f"{year}-{month:02d}-{day:02d}"
    # close_price is at column 7
    close_price = float(latest_stock_data[6].replace(',', ''))

    return {
        "date": date,
        "stock_id": stock_id,
        "close_price": close_price,
        "source": source
    }

def fetch_twse_price(stock_id: str, start_at: datetime = datetime.today(), end_at: datetime = None) -> list:
    """
    Crawl TWSE stock and return a list of dict ready for BigQuery
    """
    results = []
    url = "https://www.twse.com.tw/exchangeReport/STOCK_DAY"

    if end_at:
        date_range = gen_date_range(start_at, end_at)
    else:
        date_range = [start_at]

    for date_obj in date_range:
        date_param = date_obj.strftime("%Y%m%d")
        params = {
            'response': 'json',
            'date': date_param,
            'stockNo': stock_id
        }
        try:
            stock_data = get_stock_price(url, params, stock_id)
            results.append(stock_data)
        except Exception as e:
            logger.warning(f"Error fetching {stock_id} for {date_param}: {e}")
            continue

    return results

def save_to_bigquery(stock_records) -> None:
    if not stock_records:
        logger.info("No data to insert.")
        return
    try:
        bq.insert_if_not_exists(stock_records)
        logger.info("Insert successful")
    except Exception as e:
        logger.error(f"BigQuery Insert Error: {e}")
        raise

def run(stock_ids: list) -> None:
    """Run stock crawler

    Args:
        stock_ids (list): stock id to crawl
    """
    bq.check_if_dataset_exists()
    bq.check_if_table_exists()
    for stock_id in stock_ids:
        try:
            stock_records = fetch_twse_price(stock_id=stock_id)
            save_to_bigquery(stock_records)
        except Exception as e:
            logger.error(f"Failed processing stock {stock_id}: {e}")

# Cloud Function Entrypoint
def main_entry_point(request):
    try:
        run(['2330', '0050'])
        return 'Success'
    except Exception as e:
        logger.error(f"Cloud Function failed: {e}")
        return 'Failed'

if __name__ == "__main__":
    try:
        run(['2330', '0050'])  # Run locally
    except Exception as e:
        logger.error(f"Local execution failed: {e}")

