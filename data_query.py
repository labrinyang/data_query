import datetime
import time
import pandas as pd
import requests
import random
import os
from datetime import datetime

# Binance Futures Endpoints
BASE_URL_BINANCE_INDEX = "https://fapi.binance.com/fapi/v1/indexPriceKlines"
BASE_URL_BINANCE_MARK = "https://fapi.binance.com/fapi/v1/markPriceKlines"
BASE_URL_BINANCE_KLINE = "https://fapi.binance.com/fapi/v1/klines"
BASE_URL_BINANCE_PREMIUM = "https://fapi.binance.com/fapi/v1/premiumIndexKlines"

# OKX Endpoints
BASE_URL_OKX_INDEX = "https://www.okx.com/api/v5/market/index-candles"
BASE_URL_OKX_MARK = "https://www.okx.com/api/v5/market/mark-price-candles"

def interval_to_seconds(interval: str) -> int:
    """
    Convert interval strings like '1m', '1h', '1d' into seconds.
    """
    number = int(''.join(filter(str.isdigit, interval)))
    if 'm' in interval:
        return number * 60
    elif 'h' in interval.lower():
        return number * 3600
    elif 'd' in interval.lower():
        return number * 86400
    else:
        raise ValueError(f"Unsupported interval format: {interval}")
    
def datetime_to_timestamp_ms(dt_str: str) -> int:
    """
    Convert a datetime string 'YYYY-MM-DD HH:MM:SS' to a millisecond timestamp.
    """
    return int(time.mktime(datetime.strptime(dt_str, "%Y-%m-%d %H:%M:%S").timetuple()) * 1000)

def filename_friendly_date(date_str: str) -> str:
    """
    Convert a date string into a filename-friendly format.
    """
    return date_str.replace(':', '-').replace(' ', '_')

def clean_binance_klines_data(data: list) -> pd.DataFrame:
    """
    Clean raw Binance kline data into a DataFrame.
    """
    if not data:
        return pd.DataFrame()
    df = pd.DataFrame(data, columns=[
        'open_time', 'open', 'high', 'low', 'close', 'ignore1',
        'close_time', 'ignore2', 'ignore3', 'ignore4', 'ignore5', 'ignore6'
    ])
    df['open'] = df['open'].astype(float)
    df['high'] = df['high'].astype(float)
    df['low'] = df['low'].astype(float)
    df['close'] = df['close'].astype(float)
    df.drop(columns=['ignore1', 'ignore2', 'ignore3', 'ignore4', 'ignore5', 'ignore6'], inplace=True)
    return df

def fetch_klines(
    base_url: str,
    params: dict,
    start_timestamp: int,
    end_timestamp: int,
    interval_seconds: int,
    desc_str: str,
    limit: int = 1000
) -> list:
    """
    Generic function to fetch kline data from Binance endpoints.
    Uses startTime/endTime pagination.
    """
    all_data = []
    total_intervals = (end_timestamp - start_timestamp) / (interval_seconds * 1000)
    temp_timestamp = start_timestamp

    with requests.Session() as s:
        while temp_timestamp < end_timestamp:
            params.update({
                'startTime': temp_timestamp,
                'endTime': end_timestamp,
                'limit': limit
            })
            response = s.get(base_url, params=params)
            print(f"Requesting URL: {response.url}")
            data = response.json()

            if isinstance(data, dict) and 'code' in data:
                print(f"Error fetching data: {data.get('msg', 'Unknown Error')}")
                break

            if not data:
                break

            all_data.extend(data)

            if len(data) < limit:
                break

            temp_timestamp = data[-1][0] + interval_seconds * 1000
            time.sleep(random.uniform(0.1, 0.2))
    return all_data

def get_index_price_klines(
    pair: str,
    interval: str,
    start_time: str,
    end_time: str,
    limit: int = 1000,
    clean: bool = True
) -> pd.DataFrame:
    start_timestamp = datetime_to_timestamp_ms(start_time)
    end_timestamp = datetime_to_timestamp_ms(end_time)
    interval_seconds = interval_to_seconds(interval)
    params = {'pair': pair, 'interval': interval}
    data = fetch_klines(BASE_URL_BINANCE_INDEX, params, start_timestamp, end_timestamp, interval_seconds, f"Downloading Index Klines ({pair})", limit)
    print(f'==== Finished fetching Index Price data: {pair} ====')
    return clean_binance_klines_data(data) if clean else pd.DataFrame(data)

def get_mark_price_klines(
    symbol: str,
    interval: str,
    start_time: str,
    end_time: str,
    limit: int = 1000,
    clean: bool = True
) -> pd.DataFrame:
    start_timestamp = datetime_to_timestamp_ms(start_time)
    end_timestamp = datetime_to_timestamp_ms(end_time)
    interval_seconds = interval_to_seconds(interval)
    params = {'symbol': symbol, 'interval': interval}
    data = fetch_klines(BASE_URL_BINANCE_MARK, params, start_timestamp, end_timestamp, interval_seconds, f"Downloading Mark Klines ({symbol})", limit)
    print(f'==== Finished fetching Mark Price data: {symbol} ====')
    return clean_binance_klines_data(data) if clean else pd.DataFrame(data)

def get_price_klines(
    symbol: str,
    interval: str,
    start_time: str,
    end_time: str,
    limit: int = 1000,
    clean: bool = True
) -> pd.DataFrame:
    start_timestamp = datetime_to_timestamp_ms(start_time)
    end_timestamp = datetime_to_timestamp_ms(end_time)
    interval_seconds = interval_to_seconds(interval)
    params = {'symbol': symbol, 'interval': interval}
    data = fetch_klines(BASE_URL_BINANCE_KLINE, params, start_timestamp, end_timestamp, interval_seconds, f"Downloading Klines ({symbol})", limit)
    print(f'==== Finished fetching Price data: {symbol} ====')
    return clean_binance_klines_data(data) if clean else pd.DataFrame(data)

def get_premium_index_klines(
    symbol: str,
    interval: str,
    start_time: str,
    end_time: str,
    limit: int = 500,
    clean: bool = True
) -> pd.DataFrame:
    start_timestamp = datetime_to_timestamp_ms(start_time)
    end_timestamp = datetime_to_timestamp_ms(end_time)
    interval_seconds = interval_to_seconds(interval)
    params = {'symbol': symbol, 'interval': interval}
    data = fetch_klines(BASE_URL_BINANCE_PREMIUM, params, start_timestamp, end_timestamp, interval_seconds, f"Downloading Premium Index Klines ({symbol})", limit)
    print(f'==== Finished fetching Premium Index data: {symbol} ====')
    return clean_binance_klines_data(data) if clean else pd.DataFrame(data)

#########################

def getsave_data_kline(data_func, filename_prefix: str, start_time: str, end_time: str, interval: str, **kwargs) -> pd.DataFrame:
    """
    General function to fetch and save kline data.
    """
    if not os.path.exists('data'):
        os.makedirs('data')
        
    friendly_start_time = filename_friendly_date(start_time)
    friendly_end_time = filename_friendly_date(end_time)
    filename = f'{filename_prefix}_{friendly_start_time}_{friendly_end_time}_{interval}.csv'
    filepath = os.path.join('data', filename)

    data = data_func(interval=interval, start_time=start_time, end_time=end_time, **kwargs)
    if not data.empty:
        print(f'{filename} successfully downloaded, saving data...')
        data.to_csv(filepath, index=False)
    else:
        print("No valid data downloaded. Please check the API request and network connection.")
    return data

if __name__ == '__main__':
    getsave_data_kline(
        get_premium_index_klines, 
        filename_prefix='BTCUSDT_premiumIndex', 
        symbol='BTCUSDT_241227', 
        start_time='2022-02-10 00:00:00',
        end_time='2024-12-15 00:00:00',
        interval='1d'
    )

    getsave_data_kline(
        get_index_price_klines, 
        filename_prefix='BTCUSD_indexPrice', 
        pair='BTCUSD', 
        start_time='2022-02-10 00:00:00',
        end_time='2024-12-15 00:00:00',
        interval='1d'
    )

    getsave_data_kline(
        get_mark_price_klines, 
        filename_prefix='BTCUSDT_241227_markPrice', 
        symbol='BTCUSDT_241227', 
        start_time='2022-02-10 00:00:00',
        end_time='2024-12-15 00:00:00',
        interval='1d'
    )
