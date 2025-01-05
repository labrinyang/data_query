import datetime
import time
import pandas as pd
import requests
import random
import os
from datetime import datetime, timezone

# Binance Futures Endpoints
BASE_URL_BINANCE_INDEX = "https://fapi.binance.com/fapi/v1/indexPriceKlines"
BASE_URL_BINANCE_MARK = "https://fapi.binance.com/fapi/v1/markPriceKlines"
BASE_URL_BINANCE_KLINE = "https://fapi.binance.com/fapi/v1/klines"
BASE_URL_BINANCE_FUNDING_RATE = "https://fapi.binance.com/fapi/v1/fundingRate"

# Deribit Endpoints
BASE_URL_DERIBIT_TRADES = "https://history.deribit.com/api/v2/public/get_last_trades_by_instrument_and_time"

# OKX Endpoints
BASE_URL_OKX_MARK_PRICE_CANDLES = "https://www.okx.com/api/v5/market/history-mark-price-candles"

# Bybit
BASE_URL_BYBIT_MARK_PRICE_KLINE = "https://api.bybit.com/v5/market/mark-price-kline"

# ================== Helper Functions ==================

def format_expiry(expiry: str, exchange: str) -> str:
    """
    Convert the expiry date from 'YYMMDD' format to the format required by each exchange.

    Parameters:
        expiry (str): Expiry date in 'YYMMDD' format, e.g., '250328'
        exchange (str): Name of the exchange, e.g., 'deribit', 'bybit'

    Returns:
        str: Formatted expiry date string
    """
    from datetime import datetime

    try:
        # Parse 'YYMMDD' format
        expiry_date = datetime.strptime(expiry, "%y%m%d")
    except ValueError:
        raise ValueError(f"Invalid expiry format: {expiry}. Expected 'YYMMDD'.")

    if exchange.lower() in ['deribit', 'bybit']:
        # Deribit and Bybit require 'DDMMMYY' format, e.g., '28MAR25'
        return expiry_date.strftime("%d%b%y").upper()
    elif exchange.lower() == 'binance':
        # Binance uses 'YYMMDD' directly
        return expiry
    elif exchange.lower() == 'okx':
        # OKX also uses 'YYMMDD' directly
        return expiry
    else:
        raise ValueError(f"Unsupported exchange for expiry formatting: {exchange}")

def convert_symbol(exchange: str, symbol: str, expiry: str) -> str:
    """
    Convert a generic symbol to the specific symbol format required by each exchange.

    Parameters:
        exchange (str): Name of the exchange, e.g., 'binance', 'deribit', 'okx', 'bybit'
        symbol (str): Base symbol, e.g., 'BTC'
        expiry (str): Expiry date in 'YYMMDD' format, e.g., '250328'

    Returns:
        str: Exchange-specific symbol format
    """
    exchange = exchange.lower()
    symbol = symbol.upper()

    # Format expiry based on exchange
    formatted_expiry = format_expiry(expiry, exchange)

    if exchange == 'binance':
        # Binance symbol format: BTCUSDT_250328
        return f"{symbol}USDT_{formatted_expiry}"
    elif exchange == 'deribit':
        # Deribit symbol format: BTC-28MAR25
        return f"{symbol}-{formatted_expiry}"
    elif exchange == 'okx':
        # OKX symbol format: BTC-USD-250328
        return f"{symbol}-USD-{formatted_expiry}"
    elif exchange == 'bybit':
        # Bybit symbol format: BTC-28MAR25
        return f"{symbol}-{formatted_expiry}"
    else:
        raise ValueError(f"Unsupported exchange: {exchange}")

def timestamp_ms_to_datetime(ts: int) -> datetime:
    """
    Convert a millisecond timestamp to a datetime object.

    Parameters:
        ts (int): Timestamp in milliseconds

    Returns:
        datetime: Corresponding datetime object in UTC
    """
    return datetime.fromtimestamp(ts / 1000, tz=timezone.utc)

def interval_to_seconds(interval: str) -> int:
    """
    Convert interval strings like '1m', '1h', '1d' into seconds.

    Parameters:
        interval (str): Interval string, e.g., '1m', '1h', '1d'

    Returns:
        int: Interval in seconds
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
    Convert a datetime string 'YYYY-MM-DD HH:MM:SS' to a millisecond timestamp in UTC.

    Parameters:
        dt_str (str): Datetime string in 'YYYY-MM-DD HH:MM:SS' format

    Returns:
        int: Timestamp in milliseconds
    """
    dt = datetime.strptime(dt_str, "%Y-%m-%d %H:%M:%S")
    dt_utc = dt.replace(tzinfo=timezone.utc)
    return int(dt_utc.timestamp() * 1000)

def filename_friendly_date(date_str: str) -> str:
    """
    Convert a date string into a filename-friendly format.

    Parameters:
        date_str (str): Date string

    Returns:
        str: Filename-friendly date string
    """
    return date_str.replace(':', '-').replace(' ', '_')

def clean_binance_klines_data(data: list) -> pd.DataFrame:
    """
    Clean raw Binance kline data into a DataFrame.

    Parameters:
        data (list): Raw kline data from Binance API

    Returns:
        pd.DataFrame: Cleaned kline data
    """
    if not data:
        return pd.DataFrame()
    df = pd.DataFrame(data, columns=[
        'open_time', 'open', 'high', 'low', 'close', 'ignore1',
        'close_time', 'ignore2', 'ignore3', 'ignore4', 'ignore5', 'ignore6'
    ])
    df[['open', 'high', 'low', 'close']] = df[['open', 'high', 'low', 'close']].astype(float)
    
    # Convert timestamps to datetime
    df['open_time'] = pd.to_datetime(df['open_time'], unit='ms')
    df['close_time'] = pd.to_datetime(df['close_time'], unit='ms')
    
    # Drop unused columns
    df.drop(columns=['ignore1', 'ignore2', 'ignore3', 'ignore4', 'ignore5', 'ignore6'], inplace=True)
    return df

def fetch_klines(
    base_url: str,
    params: dict,
    start_timestamp: int,
    end_timestamp: int,
    interval_seconds: int,
    desc_str: str,
    limit: int = 1000,
    max_retries: int = 5,
    backoff_factor: float = 0.5
) -> list:
    """
    Generic function to fetch klines or similar data from Binance endpoints with pagination and retries.

    Parameters:
        base_url (str): API base URL
        params (dict): Initial request parameters
        start_timestamp (int): Start time in ms
        end_timestamp (int): End time in ms
        interval_seconds (int): Interval in seconds
        desc_str (str): Description for logging
        limit (int): Number of items per request
        max_retries (int): Maximum retry attempts
        backoff_factor (float): Backoff factor for retries

    Returns:
        list: Raw data list
    """
    all_data = []
    temp_timestamp = start_timestamp
    session = requests.Session()
    
    while temp_timestamp < end_timestamp:
        current_limit = min(limit, 1000)  # Binance API max limit=1000
        request_params = params.copy()
        request_params.update({
            'startTime': temp_timestamp,
            'endTime': end_timestamp,
            'limit': current_limit
        })
        
        retries = 0
        while retries < max_retries:
            try:
                response = session.get(base_url, params=request_params, timeout=10)
                response.raise_for_status()
                data = response.json()
                
                if isinstance(data, dict) and 'code' in data:
                    print(f"[Error] {desc_str}: {data.get('msg', 'Unknown error')}")
                    if data.get('code') == -1003:  # Rate limit exceeded
                        wait_time = backoff_factor * (2 ** retries)
                        print(f"[Warning] Rate limit exceeded. Retrying in {wait_time} seconds...")
                        time.sleep(wait_time)
                        retries += 1
                        continue
                    else:
                        break
                
                if not data:
                    print(f"[Info] {desc_str}: No more data available.")
                    temp_timestamp = end_timestamp
                    break
                
                all_data.extend(data)
                print(f"[Info] {desc_str}: Fetched {len(data)} records. Total: {len(all_data)}")
                
                if len(data) < current_limit:
                    print(f"[Info] {desc_str}: All data fetched.")
                    temp_timestamp = end_timestamp
                    break
                
                # Determine last timestamp based on data format
                if isinstance(data[-1], list):
                    last_open_time = data[-1][0]
                elif isinstance(data[-1], dict) and 'timestamp' in data[-1]:
                    last_open_time = data[-1]['timestamp']
                else:
                    print("[Warning] Unrecognized data format. Pagination may fail.")
                    temp_timestamp = end_timestamp
                    break
                
                temp_timestamp = last_open_time + interval_seconds * 1000

                # Sleep to avoid rate limits
                time.sleep(random.uniform(0.1, 0.3))
                break  # Successful request
            except requests.exceptions.RequestException as e:
                wait_time = backoff_factor * (2 ** retries)
                print(f"[Error] {desc_str}: Request failed: {e}. Retrying in {wait_time} seconds...")
                time.sleep(wait_time)
                retries += 1
        else:
            # Exceeded max retries, stop fetching
            print(f"[Error] {desc_str}: Max retries reached. Stopping data fetch.")
            break
    
    session.close()
    return all_data

# ============== Specific Fetch Functions ==============

def get_index_price_klines(
    pair: str,
    interval: str,
    start_time: str,
    end_time: str,
    limit: int = 1000,
    clean: bool = True
) -> pd.DataFrame:
    """
    Fetch index price klines.

    Parameters:
        pair (str): Trading pair, e.g., 'BTCUSD'
        interval (str): Time interval, e.g., '1m', '1h', '1d'
        start_time (str): Start time 'YYYY-MM-DD HH:MM:SS'
        end_time (str): End time 'YYYY-MM-DD HH:MM:SS'
        limit (int): Number of records per request
        clean (bool): Whether to clean data into DataFrame

    Returns:
        pd.DataFrame
    """
    start_timestamp = datetime_to_timestamp_ms(start_time)
    end_timestamp = datetime_to_timestamp_ms(end_time)
    interval_seconds = interval_to_seconds(interval)
    params = {'pair': pair, 'interval': interval}

    data = fetch_klines(
        base_url=BASE_URL_BINANCE_INDEX,
        params=params,
        start_timestamp=start_timestamp,
        end_timestamp=end_timestamp,
        interval_seconds=interval_seconds,
        desc_str=f"Downloading Index Klines ({pair})",
        limit=limit
    )
    print(f"==== Finished fetching Index Price data: {pair} ====")
    return clean_binance_klines_data(data) if clean else pd.DataFrame(data)

def get_mark_price_klines(
    symbol: str,
    interval: str,
    start_time: str,
    end_time: str,
    limit: int = 1000,
    clean: bool = True
) -> pd.DataFrame:
    """
    Fetch mark price klines.

    Parameters:
        symbol (str): Trading symbol, e.g., 'BTCUSDT'
        interval (str): Time interval
        start_time (str): Start time
        end_time (str): End time
        limit (int): Number of records per request
        clean (bool): Whether to clean data

    Returns:
        pd.DataFrame
    """
    start_timestamp = datetime_to_timestamp_ms(start_time)
    end_timestamp = datetime_to_timestamp_ms(end_time)
    interval_seconds = interval_to_seconds(interval)
    params = {'symbol': symbol, 'interval': interval}

    data = fetch_klines(
        base_url=BASE_URL_BINANCE_MARK,
        params=params,
        start_timestamp=start_timestamp,
        end_timestamp=end_timestamp,
        interval_seconds=interval_seconds,
        desc_str=f"Downloading Mark Klines ({symbol})",
        limit=limit
    )
    print(f"==== Finished fetching Mark Price data: {symbol} ====")
    return clean_binance_klines_data(data) if clean else pd.DataFrame(data)

def get_price_klines(
    symbol: str,
    interval: str,
    start_time: str,
    end_time: str,
    limit: int = 1000,
    clean: bool = True
) -> pd.DataFrame:
    """
    Fetch standard price klines (USDⓈ-M contracts).

    Parameters:
        symbol (str): Trading symbol
        interval (str): Time interval
        start_time (str): Start time
        end_time (str): End time
        limit (int): Number of records per request
        clean (bool): Whether to clean data

    Returns:
        pd.DataFrame
    """
    start_timestamp = datetime_to_timestamp_ms(start_time)
    end_timestamp = datetime_to_timestamp_ms(end_time)
    interval_seconds = interval_to_seconds(interval)
    params = {'symbol': symbol, 'interval': interval}

    data = fetch_klines(
        base_url=BASE_URL_BINANCE_KLINE,
        params=params,
        start_timestamp=start_timestamp,
        end_timestamp=end_timestamp,
        interval_seconds=interval_seconds,
        desc_str=f"Downloading Klines ({symbol})",
        limit=limit
    )
    print(f"==== Finished fetching Price data: {symbol} ====")
    return clean_binance_klines_data(data) if clean else pd.DataFrame(data)


# ============== Funding Rate ==============

def clean_funding_rate_data(data: list) -> pd.DataFrame:
    """
    Clean Funding Rate data into DataFrame.

    Parameters:
        data (list): Raw funding rate data from Binance API

    Returns:
        pd.DataFrame: Cleaned funding rate data
    """
    if not data:
        return pd.DataFrame()
    
    df = pd.DataFrame(data, columns=['symbol', 'fundingRate', 'fundingTime', 'markPrice'])
    df[['fundingRate', 'markPrice']] = df[['fundingRate', 'markPrice']].astype(float)
    df['fundingTime'] = pd.to_datetime(df['fundingTime'], unit='ms')
    return df

def get_funding_rate_history(
    symbol: str = None,
    start_time: str = None,
    end_time: str = None,
    limit: int = 100,
    clean: bool = True
) -> pd.DataFrame:
    """
    Fetch Funding Rate history.
    
    Parameters:
        symbol (str): Trading pair, e.g., 'BTCUSDT'
        start_time (str): Start time
        end_time (str): End time
        limit (int): Number of records per request (default 100, max 1000)
        clean (bool): Whether to clean data into DataFrame

    Returns:
        pd.DataFrame
    """
    params = {}
    if symbol:
        params['symbol'] = symbol
    if start_time:
        params['startTime'] = datetime_to_timestamp_ms(start_time)
    if end_time:
        params['endTime']   = datetime_to_timestamp_ms(end_time)
    params['limit'] = min(limit, 1000)

    desc_str = f"Downloading Funding Rate History{' for ' + symbol if symbol else ''}"
    
    all_data = []
    session = requests.Session()
    retries = 0
    max_retries = 5
    backoff_factor = 0.5
    
    while True:
        try:
            response = session.get(BASE_URL_BINANCE_FUNDING_RATE, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()
            
            if isinstance(data, dict) and 'code' in data:
                print(f"[Error] {desc_str}: {data.get('msg', 'Unknown error')}")
                if data.get('code') == -1003:  # Rate limit exceeded
                    wait_time = backoff_factor * (2 ** retries)
                    print(f"[Warning] Rate limit exceeded. Retrying in {wait_time} seconds...")
                    time.sleep(wait_time)
                    retries += 1
                    if retries >= max_retries:
                        print(f"[Error] {desc_str}: Max retries reached. Stopping data fetch.")
                        break
                    continue
                else:
                    break
            
            if not data:
                print(f"[Info] {desc_str}: No more data available.")
                break
            
            all_data.extend(data)
            print(f"[Info] {desc_str}: Fetched {len(data)} records. Total: {len(all_data)}")
            
            if len(data) < params.get('limit', 100):
                print(f"[Info] {desc_str}: All data fetched.")
                break
            
            # Update startTime for pagination
            last_funding_time = data[-1]['fundingTime']
            params['startTime'] = last_funding_time + 1

            time.sleep(random.uniform(0.1, 0.3))
        except requests.exceptions.RequestException as e:
            wait_time = backoff_factor * (2 ** retries)
            print(f"[Error] {desc_str}: Request failed: {e}. Retrying in {wait_time} seconds...")
            time.sleep(wait_time)
            retries += 1
            if retries >= max_retries:
                print(f"[Error] {desc_str}: Max retries reached. Stopping data fetch.")
                break
    
    session.close()
    
    if clean:
        return clean_funding_rate_data(all_data)
    else:
        return pd.DataFrame(all_data)

#=======Volume Klines Data=======

def get_price_klines_volume(
    symbol: str,
    interval: str,
    start_time: str,
    end_time: str,
    limit: int = 500,
    clean: bool = True
) -> pd.DataFrame:
    """
    Fetch Binance futures klines focusing on 'volume'.

    Parameters:
        symbol (str): Trading pair, e.g., 'BTCUSDT_241227'
        interval (str): Time interval, e.g., '1m','5m','1h','1d'
        start_time (str): Start time 'YYYY-MM-DD HH:MM:SS'
        end_time (str): End time 'YYYY-MM-DD HH:MM:SS'
        limit (int): Number of records per request (max 1500, default 500)
        clean (bool): Whether to clean data into DataFrame

    Returns:
        pd.DataFrame: Kline data with 'volume' column
    """
    # Convert time
    start_timestamp = datetime_to_timestamp_ms(start_time)
    end_timestamp   = datetime_to_timestamp_ms(end_time)
    interval_seconds = interval_to_seconds(interval)

    # Build request params
    params = {
        'symbol': symbol,
        'interval': interval,
    }

    # Fetch data
    data = fetch_klines(
        base_url=BASE_URL_BINANCE_KLINE,  
        params=params,
        start_timestamp=start_timestamp,
        end_timestamp=end_timestamp,
        interval_seconds=interval_seconds,
        desc_str=f"Downloading Klines Volume ({symbol})",
        limit=limit
    )
    print(f"==== Finished fetching Price Klines Volume data: {symbol} ====")

    if not clean:
        return pd.DataFrame(data)

    # Convert to DataFrame
    df = pd.DataFrame(data, columns=[
        'open_time',       
        'open',            
        'high',            
        'low',            
        'close',           
        'volume',         
        'close_time',     
        'quote_volume',    
        'trades_count',   
        'taker_base_vol',  
        'taker_quote_vol', 
        'ignore'           
    ])

    # Convert timestamps
    df['open_time']  = pd.to_datetime(df['open_time'],  unit='ms')
    df['close_time'] = pd.to_datetime(df['close_time'], unit='ms')

    # Convert numerical columns
    numeric_cols = ['open','high','low','close','volume','quote_volume','taker_base_vol','taker_quote_vol']
    df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric, errors='coerce')
    df['trades_count'] = pd.to_numeric(df['trades_count'], errors='coerce').fillna(0).astype(int)

    # Drop 'ignore' column if not needed
    df.drop(columns=['ignore'], inplace=True)
    
    # Select relevant columns
    df = df[['open_time','close_time','volume']]

    return df

# ============== General Kline Data Fetch and Save ==============

def getsave_data_kline(
    data_func,
    filename_prefix: str,
    start_time: str,
    end_time: str,
    interval: str = None,
    period: str = None,
    time_key: str = 'interval',
    **kwargs
) -> pd.DataFrame:
    """
    General function to fetch and save data (Klines, Taker Long Short, etc.).
    
    Parameters:
        data_func: Function to fetch data
        filename_prefix (str): Prefix for filename
        start_time (str): Start time 'YYYY-MM-DD HH:MM:SS'
        end_time (str): End time 'YYYY-MM-DD HH:MM:SS'
        interval (str): If time_key='interval', pass to data_func
        period (str): If time_key='period', pass to data_func
        time_key (str): 'interval' or 'period'
        **kwargs: Additional parameters (e.g., symbol='BTCUSDT', limit=500)

    Returns:
        pd.DataFrame
    """
    if not os.path.exists('data'):
        os.makedirs('data')
    
    friendly_start_time = filename_friendly_date(start_time)
    friendly_end_time   = filename_friendly_date(end_time)
    # Use interval or period based on time_key
    freq = interval if interval else period
    filename = f'{filename_prefix}_{friendly_start_time}_{friendly_end_time}_{freq}.csv'
    filepath = os.path.join('data', filename)

    params = {}
    if time_key == 'interval' and interval is not None:
        params['interval'] = interval
    elif time_key == 'period' and period is not None:
        params['period'] = period
    else:
        raise ValueError("Must provide a valid time parameter (interval or period)")

    params.update(kwargs)

    df = data_func(
        start_time=start_time,
        end_time=end_time,
        **params
    )

    if not df.empty:
        print(f"{filename} downloaded successfully. Saving data...")
        df.to_csv(filepath, index=False)
    else:
        print("No data downloaded. Check API or time range.")
    return df

def clean_deribit_trades_data(data: dict) -> pd.DataFrame:
    """
    Clean Deribit trade data into a DataFrame, extracting timestamp, index_price, and mark_price.
    
    Parameters:
        data (dict): Raw JSON data from Deribit API.
    
    Returns:
        pd.DataFrame
    """
    if not data or 'result' not in data:
        return pd.DataFrame()
    
    trades = data['result'].get('trades', [])
    records = []
    for trade in trades:
        timestamp = trade.get('timestamp')
        index_price = trade.get('index_price')
        mark_price = trade.get('mark_price')
        if timestamp and index_price and mark_price:
            records.append({
                'timestamp': timestamp_ms_to_datetime(timestamp),
                'index_price': float(index_price),
                'mark_price': float(mark_price)
            })
    
    df = pd.DataFrame(records)
    return df

# ============== Deribit Trade Fetching ==============

def fetch_deribit_trades(
    instrument_name: str,
    start_time: str,
    end_time: str,
    count: int = 1000,  # Maximum 1000
    sorting: str = 'asc',  # Use 'asc' for pagination
    max_retries: int = 5,
    backoff_factor: float = 0.5
) -> pd.DataFrame:
    """
    Fetch Deribit's trade data and extract index_price and mark_price.
    
    Parameters:
        instrument_name (str): Instrument name, e.g., 'BTC-28MAR25'
        start_time (str): Start time 'YYYY-MM-DD HH:MM:SS'
        end_time (str): End time 'YYYY-MM-DD HH:MM:SS'
        count (int): Number of trades per request (maximum 1000)
        sorting (str): Sorting method 'asc' or 'desc', 'asc' is recommended
        max_retries (int): Maximum number of retry attempts
        backoff_factor (float): Backoff factor for retries
    
    Returns:
        pd.DataFrame
    """
    # 1) Convert start and end times to datetime and millisecond timestamps
    start_dt = datetime.strptime(start_time, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
    end_dt = datetime.strptime(end_time, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)

    start_ts = int(start_dt.timestamp() * 1000)
    end_ts = int(end_dt.timestamp() * 1000)

    all_data = []
    current_start_ts = start_ts
    session = requests.Session()

    while current_start_ts <= end_ts:
        params = {
            'instrument_name': instrument_name,
            'start_timestamp': current_start_ts,
            'end_timestamp': end_ts,
            'count': count,
            'sorting': sorting  # Use 'asc'
        }

        retries = 0
        while retries < max_retries:
            try:
                response = session.get(BASE_URL_DERIBIT_TRADES, params=params, timeout=10)
                # Print the requested URL for debugging
                print(response.url)
                response.raise_for_status()
                data = response.json()

                if 'result' not in data or 'trades' not in data['result']:
                    print(f"[Warning] Unexpected response format: {data}")
                    break

                trades = data['result']['trades']
                if not trades:
                    print("[Info] No more trades found.")
                    session.close()
                    return pd.concat(all_data, ignore_index=True) if all_data else pd.DataFrame()

                df = clean_deribit_trades_data(data)
                if df.empty:
                    print("[Info] No valid trade data found in this batch.")
                    session.close()
                    return pd.concat(all_data, ignore_index=True) if all_data else pd.DataFrame()

                # Filter trades within the time range
                df = df[(df['timestamp'] >= start_dt) & (df['timestamp'] <= end_dt)]

                if df.empty:
                    print("[Info] No trades within the specified time range in this batch.")
                    session.close()
                    return pd.concat(all_data, ignore_index=True) if all_data else pd.DataFrame()

                all_data.append(df)
                print(f"[Info] Fetched {len(df)} trades. Total so far: {len(all_data)*count}")

                # Find the last trade timestamp in this batch
                last_trade_ts = int(df['timestamp'].max().timestamp() * 1000)

                # Check for progress to prevent infinite loop
                if last_trade_ts <= current_start_ts:
                    print("[Info] No progress in pagination. Stopping.")
                    session.close()
                    return pd.concat(all_data, ignore_index=True) if all_data else pd.DataFrame()

                # Update start_timestamp for the next request
                current_start_ts = last_trade_ts + 1

                # Avoid triggering rate limits
                time.sleep(random.uniform(0.1, 0.3))
                break  # Successful request, exit retry loop
            except requests.exceptions.RequestException as e:
                wait_time = backoff_factor * (2 ** retries)
                print(f"[Error] Fetching trades failed: {e}. Retrying in {wait_time} seconds...")
                time.sleep(wait_time)
                retries += 1
        else:
            # Exceeded maximum retries, stop fetching data
            print("[Error] Max retries reached. Stopping data fetch.")
            break

    session.close()

    if all_data:
        return pd.concat(all_data, ignore_index=True)
    else:
        return pd.DataFrame()

# ============== K-Line Aggregation ==============

def aggregate_trades_to_kline(df: pd.DataFrame, interval: str) -> pd.DataFrame:
    """
    Aggregate trade data into K-line data.
    
    Parameters:
        df (pd.DataFrame): DataFrame containing trade data, must include 'timestamp', 'index_price', 'mark_price' columns.
        interval (str): Time interval, e.g., '1T', '5T', '1H'.
    
    Returns:
        pd.DataFrame: Aggregated K-line data
    """
    if df.empty:
        return pd.DataFrame()
    
    # Set timestamp as index
    df.set_index('timestamp', inplace=True)
    
    # Define resample frequency
    resample_rule = interval  # Use Pandas frequency string, e.g., '1T', '5T', '1H'
    
    # Aggregate index_price
    index_ohlc = df['index_price'].resample(resample_rule).agg(['first', 'max', 'min', 'last']).rename(columns={
        'first': 'index_open',
        'max': 'index_high',
        'min': 'index_low',
        'last': 'index_close'
    })
    
    # Aggregate mark_price
    mark_ohlc = df['mark_price'].resample(resample_rule).agg(['first', 'max', 'min', 'last']).rename(columns={
        'first': 'mark_open',
        'max': 'mark_high',
        'min': 'mark_low',
        'last': 'mark_close'
    })
    
    # Combine the two OHLC data
    kline_df = pd.concat([index_ohlc, mark_ohlc], axis=1).dropna().reset_index()
    
    # Rename the time column
    kline_df.rename(columns={'timestamp': 'open_time'}, inplace=True)
    kline_df['close_time'] = kline_df['open_time'] + pd.to_timedelta(interval_to_seconds(interval), unit='s')
    
    return kline_df

# ============== Save K-Line Data ==============

def save_deribit_kline_data(
    instrument_name: str,
    filename_prefix: str,
    start_time: str,
    end_time: str,
    interval: str = '1T',  # '1T' means 1 minute, '5T' means 5 minutes, '1H' means 1 hour
    count: int = 1000,  # Updated to 1000
    sorting: str = 'asc',  # Use 'asc'
    **kwargs
) -> pd.DataFrame:
    """
    Fetch and save Deribit's K-line data (including index and mark prices).
    
    Parameters:
        instrument_name (str): Instrument name, e.g., 'BTC-28MAR25'
        filename_prefix (str): Filename prefix
        start_time (str): Start time 'YYYY-MM-DD HH:MM:SS'
        end_time (str): End time 'YYYY-MM-DD HH:MM:SS'
        interval (str): Time interval, e.g., '1T', '5T', '1H'
        count (int): Number of trades per request
        sorting (str): Sorting method 'asc' or 'desc'
        **kwargs: Additional parameters
    
    Returns:
        pd.DataFrame
    """
    if not os.path.exists('data'):
        os.makedirs('data')
    
    friendly_start_time = filename_friendly_date(start_time)
    friendly_end_time = filename_friendly_date(end_time)
    filename = f'{filename_prefix}_{instrument_name}_{friendly_start_time}_{friendly_end_time}_{interval}.csv'
    filepath = os.path.join('data', filename)
    
    # Fetch trade data
    trades_df = fetch_deribit_trades(
        instrument_name=instrument_name,
        start_time=start_time,
        end_time=end_time,
        count=count,
        sorting=sorting
    )
    
    if trades_df.empty:
        print("No trade data downloaded. Please check the API or time range.")
        return pd.DataFrame()
    
    # Aggregate into K-line data
    kline_df = aggregate_trades_to_kline(trades_df, interval)
    
    if not kline_df.empty:
        print(f"{filename} downloaded successfully. Saving data...")
        kline_df.to_csv(filepath, index=False)
    else:
        print("No K-line data generated. Possibly due to insufficient trade data or unreasonable interval settings.")
    
    return kline_df

# ================== OKX Mark Price Fetching ==================
def clean_okx_mark_price_candles(data: dict) -> pd.DataFrame:
    """
    Clean OKX mark price K-line data into a DataFrame, extracting ts, open, high, low, close.
    
    Parameters:
        data (dict): Raw JSON data from OKX API.
    
    Returns:
        pd.DataFrame
    """
    if not data or 'data' not in data:
        return pd.DataFrame()
    
    records = []
    for item in data['data']:
        ts, o, h, l, c, confirm = item
        if confirm == '1':  # Only process completed K-lines
            records.append({
                'timestamp': timestamp_ms_to_datetime(int(ts)),
                'open': float(o),
                'high': float(h),
                'low': float(l),
                'close': float(c),
                'confirm': int(confirm)
            })
    
    df = pd.DataFrame(records)
    return df

def fetch_okx_mark_price_candles(
    inst_id: str,
    bar: str = '4H',  # Use 4-hour granularity
    after= None,
    before = None,
    limit: int = 100,
    max_retries: int = 5,
    backoff_factor: float = 0.5
) -> pd.DataFrame:
    """
    Fetch OKX's historical mark price K-line data.
    
    Parameters:
        inst_id (str): Product ID, e.g., 'BTC-USD-20241227-SWAP'
        bar (str): Time granularity, e.g., '1m', '3m', '5m', '15m', '30m', '1H', '2H', '4H'
        after (int, optional): Request data before this timestamp
        before (int, optional): Request data after this timestamp
        limit (int): Number of data points per request, maximum 100
        max_retries (int): Maximum number of retry attempts
        backoff_factor (float): Backoff factor for retries
    
    Returns:
        pd.DataFrame: Cleaned K-line data
    """
    params = {
        'instId': inst_id,
        'bar': bar,
        'limit': limit
    }
    if after:
        params['after'] = str(after)
    if before:
        params['before'] = str(before)
    
    session = requests.Session()
    retries = 0
    
    while retries < max_retries:
        try:
            response = session.get(BASE_URL_OKX_MARK_PRICE_CANDLES, params=params, timeout=10)
            # Print the requested URL for debugging
            print(response.url)
            response.raise_for_status()
            data = response.json()
            
            if 'code' in data and data['code'] != '0':
                print(f"[Error] {data.get('msg', 'Unknown error')}")
                break  # Do not retry on other errors
            
            df = clean_okx_mark_price_candles(data)
            if df.empty:
                print("[Info] No mark price candles data found in this batch.")
                break
            
            return df
        except requests.exceptions.RequestException as e:
            wait_time = backoff_factor * (2 ** retries)
            print(f"[Error] Fetching mark price candles failed: {e}. Retrying in {wait_time} seconds...")
            time.sleep(wait_time)
            retries += 1
    
    print("[Error] Max retries reached. Failed to fetch mark price candles data.")
    return pd.DataFrame()

# ================== OKX K-Line Aggregation ==================

def aggregate_candles_to_8H(df: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregate 4-hour K-line data into 8-hour K-line data.
    
    Parameters:
        df (pd.DataFrame): DataFrame containing 4-hour K-line data, must include 'timestamp', 'open', 'high', 'low', 'close' columns.
    
    Returns:
        pd.DataFrame
    """
    if df.empty:
        return pd.DataFrame()
    
    # Ensure data is sorted in ascending order by time
    df = df.sort_values('timestamp')
    
    # Set timestamp as index
    df.set_index('timestamp', inplace=True)
    
    # Aggregate into 8-hour K-lines
    aggregated_df = df.resample('8H').agg({
        'open': 'first',
        'high': 'max',
        'low': 'min',
        'close': 'last'
    }).dropna().reset_index()
    
    # Add close_time column
    aggregated_df['close_time'] = aggregated_df['timestamp'] + pd.to_timedelta(8, unit='H')
    
    return aggregated_df

# ================== Save OKX Mark Price Data ==================

def save_okx_mark_price_data(
    inst_id: str,
    start_time: str,
    end_time: str,
    interval: str = '8H',  # Target aggregation interval
    bar: str = '4H',  # Minimum granularity supported by API
    filename_prefix: str = 'okx_mark_price',
    limit: int = 100,  # Maximum 100 data points per request
    max_retries: int = 5,
    backoff_factor: float = 0.5
):
    """
    Fetch and save OKX's historical mark price K-line data.
    
    Parameters:
        inst_id (str): Product ID, e.g., 'BTC-USD-20241227-SWAP'
        start_time (str): Start time 'YYYY-MM-DD HH:MM:SS'
        end_time (str): End time 'YYYY-MM-DD HH:MM:SS'
        interval (str): Target aggregation time interval, e.g., '8H'
        bar (str): Time granularity supported by API, e.g., '1m', '3m', '5m', '15m', '30m', '1H', '2H', '4H'
        filename_prefix (str): Filename prefix
        limit (int): Number of data points per request, maximum 100
        max_retries (int): Maximum number of retry attempts
        backoff_factor (float): Backoff factor for retries
    
    Returns:
        None
    """
    # Convert start and end times to millisecond timestamps
    start_ts = datetime_to_timestamp_ms(start_time)
    end_ts = datetime_to_timestamp_ms(end_time)
    
    all_candles = []
    after = None  # No 'after' parameter initially
    
    print(f"[Info] Starting data fetch from {start_time} to {end_time} with {bar} bar.")
    
    while True:
        df = fetch_okx_mark_price_candles(
            inst_id=inst_id,
            bar=bar,
            after=after,
            before=None,
            limit=limit,
            max_retries=max_retries,
            backoff_factor=backoff_factor
        )
        
        if df.empty:
            print("[Info] No more data to fetch.")
            break
        
        # Filter data within the time range
        df = df[(df['timestamp'] >= timestamp_ms_to_datetime(start_ts)) & 
                (df['timestamp'] <= timestamp_ms_to_datetime(end_ts))]
        
        if df.empty:
            print("[Info] No data in the specified time range.")
            break
        
        all_candles.append(df)
        print(f"[Info] Fetched {len(df)} candles.")
        
        # Get the last timestamp for the next pagination
        last_ts = int(df['timestamp'].iloc[-1].timestamp() * 1000)
        
        # Check if the end of the specified time range is reached
        if last_ts >= end_ts:
            print("[Info] Reached the end of the specified time range.")
            break
        
        # Update 'after' parameter for the next request
        after = last_ts
        
        # Adhere to rate limits: 10 requests/2s
        time.sleep(random.uniform(0.2, 0.3))  # Stay below rate limit
    
    # Combine all data
    if all_candles:
        combined_df = pd.concat(all_candles, ignore_index=True)
        # Remove duplicates to prevent overlap
        combined_df.drop_duplicates(subset=['timestamp'], inplace=True)
        combined_df = combined_df.sort_values('timestamp')
        
        # Aggregate into 8-hour K-lines
        aggregated_df = aggregate_candles_to_8H(combined_df)
        
        if not aggregated_df.empty:
            # Create 'data' directory if it doesn't exist
            if not os.path.exists('data'):
                os.makedirs('data')
            
            friendly_start_time = filename_friendly_date(start_time)
            friendly_end_time = filename_friendly_date(end_time)
            filename = f"{filename_prefix}_{inst_id}_{friendly_start_time}_{friendly_end_time}_{interval}.csv"
            filepath = os.path.join('data', filename)
            
            # Save to CSV
            aggregated_df.to_csv(filepath, index=False)
            print(f"[Info] Data saved to {filepath}")
        else:
            print("[Warning] Aggregated DataFrame is empty. No data to save.")
    else:
        print("[Warning] No data fetched. Nothing to save.")

# ============== Bybit =================

def clean_bybit_mark_price_kline(data: dict) -> pd.DataFrame:
    """
    Clean Bybit mark price K-line data into a DataFrame, extracting: timestamp, open, high, low, close

    Bybit returns data in descending order by startTime
    """
    if "result" not in data or "list" not in data["result"]:
        return pd.DataFrame()
    
    kline_list = data["result"]["list"]
    if not kline_list:
        return pd.DataFrame()
    
    records = []
    # Each item in the list: [startTime(ms), openPrice, highPrice, lowPrice, closePrice]
    for item in kline_list:
        if len(item) < 5:
            continue  # Skip incomplete data
        start_time_str, open_str, high_str, low_str, close_str = item[:5]
        try:
            start_time_ms = int(start_time_str)
            records.append({
                'timestamp': timestamp_ms_to_datetime(start_time_ms),
                'open': float(open_str),
                'high': float(high_str),
                'low':  float(low_str),
                'close': float(close_str),
            })
        except (ValueError, TypeError):
            continue  # Skip data that cannot be parsed
    
    df = pd.DataFrame(records)
    return df

def parse_interval(interval_str: str):
    """
    Convert user-input interval strings to seconds.
    Supported formats: '1m', '3m', '5m', '15m', '30m', '1h', '2h', '4h', '6h', '12h', '1d', '1w', '1M'
    """
    unit = interval_str[-1].lower()
    try:
        value = int(interval_str[:-1])
    except ValueError:
        print(f"[Error] Invalid interval format: {interval_str}")
        return None
    
    if unit == 'm':
        return value * 60
    elif unit == 'h':
        return value * 3600
    elif unit == 'd':
        return value * 86400
    elif unit == 'w':
        return value * 604800
    elif unit == 'm':  # Assuming 'M' for month
        return value * 2628000  # Approximate month in seconds
    else:
        print(f"[Error] Unsupported interval unit: {unit}")
        return None

def get_supported_intervals():
    """
    Get all interval strings supported by Bybit.
    """
    return ["1", "3", "5", "15", "30", "60", "120", "240", "360", "720", "D", "W", "M"]

def find_interval_str(seconds: int, interval_to_seconds: dict) -> str:
    """
    Find the corresponding Bybit interval string based on seconds.
    """
    for k, v in interval_to_seconds.items():
        if v == seconds:
            return k
    return str(seconds)  # Default to numeric string

def find_best_base_interval(desired_interval_seconds: int):
    """
    Find the best base interval supported by Bybit to match the desired interval in seconds.
    Returns a tuple of (Bybit interval string, factor), where factor indicates how many base intervals to aggregate.
    
    If a directly supported interval is found, factor is 1.
    If not, returns the closest base interval less than the desired interval and the corresponding factor.
    """
    # Define mapping from Bybit intervals to seconds
    interval_to_seconds_map = {
        '1': 60,
        '3': 180,
        '5': 300,
        '15': 900,
        '30': 1800,
        '60': 3600,
        '120': 7200,
        '240': 14400,
        '360': 21600,
        '720': 43200,
        'D': 86400,
        'W': 604800,
        'M': 2628000,
    }
    
    # Get supported base intervals in seconds and sort
    supported_intervals = get_supported_intervals()
    interval_seconds = [interval_to_seconds_map[interval] for interval in supported_intervals]
    interval_seconds.sort()
    
    # Search from largest to smallest for the largest base interval that divides the desired interval
    for base in reversed(interval_seconds):
        if desired_interval_seconds % base == 0:
            # Found a base interval that divides the desired interval
            return (find_interval_str(base, interval_to_seconds_map), desired_interval_seconds // base)
    
    # If no exact division found, find the largest base interval less than the desired interval
    for base in reversed(interval_seconds):
        if base < desired_interval_seconds:
            factor = desired_interval_seconds // base
            return (find_interval_str(base, interval_to_seconds_map), factor)
    
    # If desired interval is smaller than all supported base intervals, use the smallest base interval
    return (find_interval_str(interval_seconds[0], interval_to_seconds_map), 1)

def aggregate_candles(df: pd.DataFrame, factor: int, desired_interval_str: str) -> pd.DataFrame:
    """
    Aggregate base interval K-line data into the target interval.
    
    Parameters:
        df (pd.DataFrame): DataFrame containing timestamp, open, high, low, close
        factor (int): Aggregation factor, number of base intervals to combine into one target interval
        desired_interval_str (str): Target interval string, e.g., '8h'
    
    Returns:
        pd.DataFrame: Aggregated K-line data
    """
    if df.empty:
        return df
    
    # Sort by time
    df = df.sort_values("timestamp")
    df.set_index("timestamp", inplace=True)
    
    # Build Pandas resample frequency string (e.g., '8h' -> '8H')
    freq_str = desired_interval_str.upper()
    
    try:
        agg_df = df.resample(freq_str).agg({
            "open": "first",
            "high": "max",
            "low":  "min",
            "close":"last",
        }).dropna().reset_index()
    except Exception as e:
        print(f"[Error] Aggregating candles failed: {e}")
        return df
    
    return agg_df

def fetch_bybit_mark_price_kline(
    symbol: str,
    category: str,         # "linear" or "inverse"
    bybit_interval: str,   # Interval to pass to Bybit API, e.g., '240'
    start = None,
    end= None,
    limit: int = 200,
    max_retries: int = 5,
    backoff_factor: float = 0.5
) -> pd.DataFrame:
    """
    Execute an HTTP request to fetch Mark Price K-line data from Bybit.
    
    Parameters:
        symbol (str): Trading symbol
        category (str): "linear" or "inverse"
        bybit_interval (str): Interval to pass to Bybit API, e.g., '240'
        start (int, optional): Start timestamp in ms
        end (int, optional): End timestamp in ms
        limit (int): Number of data points per request
        max_retries (int): Maximum retry attempts
        backoff_factor (float): Backoff factor for retries
    
    Returns:
        pd.DataFrame: Cleaned K-line data
    """
    params = {
        "symbol": symbol.upper(),
        "category": category,
        "interval": bybit_interval,  # Bybit officially supports ["1","3","5","15","30","60","120","240","360","720","D","M","W"]
        "limit": limit
    }
    if start is not None:
        params["start"] = str(start)
    if end is not None:
        params["end"]   = str(end)
    
    session = requests.Session()
    retries = 0
    while retries < max_retries:
        try:
            resp = session.get(BASE_URL_BYBIT_MARK_PRICE_KLINE, params=params, timeout=10)
            print(f"[Debug] Request URL: {resp.url}")
            resp.raise_for_status()
            data = resp.json()
            
            # Check retCode
            if data.get("retCode", -1) != 0:
                print(f"[Error] {data.get('retMsg', 'Unknown error')}")
                return pd.DataFrame()
            
            df = clean_bybit_mark_price_kline(data)
            return df
        
        except requests.exceptions.RequestException as e:
            wait_time = backoff_factor * (2 ** retries)
            print(f"[Error] Fetching Bybit mark price kline failed: {e}. Retrying in {wait_time} seconds...")
            time.sleep(wait_time)
            retries += 1
    
    print("[Error] Max retries reached. Failed to fetch Bybit mark price kline data.")
    return pd.DataFrame()

def save_bybit_mark_price_data(
    symbol: str,
    category: str,
    interval: str,      # User input, e.g., '4h', '8h', '1h'...
    start_time: str,
    end_time: str,
    filename_prefix: str = "bybit_mark_price_kline",
    limit: int = 200,
    max_retries: int = 5,
    backoff_factor: float = 0.5
):
    """
    Fetch Mark Price K-lines from Bybit by paging through [start_time, end_time] and save as CSV.
    If user inputs interval='8h', it first fetches data with '240' frequency internally and then aggregates to 8 hours.
    
    Parameters:
        symbol (str): e.g., "BTC-28MAR25" (ensure it's valid)
        category (str): "linear" or "inverse"
        interval (str): User input K-line period, e.g., "4h", "8h", "1h"
        start_time (str): Start time 'YYYY-MM-DD HH:MM:SS'
        end_time (str): End time 'YYYY-MM-DD HH:MM:SS'
        filename_prefix (str): Filename prefix
        limit (int): Number of data points per request, [1, 1000], default 200
        max_retries (int): Number of retry attempts
        backoff_factor (float): Backoff factor for retries
    """
    # 1) Parse interval
    desired_interval_seconds = parse_interval(interval)
    if desired_interval_seconds is None:
        print(f"[Error] Unable to parse interval '{interval}'. Exiting.")
        return
    
    bybit_supported_intervals = get_supported_intervals()
    
    interval_lower = interval.lower()
    # Default aggregation factor is 1
    aggregate_factor = 1  
    
    # If user's interval is supported by Bybit, convert directly
    if interval_lower in ["1m","3m","5m","15m","30m","1h","2h","4h","6h","12h","1d","1w","1mth"]:
        if interval_lower == "1mth":
            # '1mth' -> Bybit = 'M'
            bybit_interval_code = "M"
        elif interval_lower.endswith('m') and interval_lower != "1mth":
            # '15m' -> '15'
            bybit_interval_code = interval_lower[:-1]
        elif interval_lower.endswith('h'):
            # '4h' -> '4' => '4*60' => '240'
            hours = int(interval_lower[:-1])
            bybit_interval_code = str(hours * 60)   # '4h' -> '240'
        elif interval_lower in ["1d", "1w", "1mth"]:
            # '1d' -> 'D', '1w' -> 'W'
            letter = interval_lower[-1].upper()  # 'd' -> 'D'
            bybit_interval_code = letter
        else:
            # Possibly '30' or similar
            bybit_interval_code = interval_lower
    else:
        # 2) If user's interval is not in Bybit's list, find the best base interval
        base_interval_str, factor = find_best_base_interval(desired_interval_seconds)
        if factor < 1:
            print(f"[Error] Cannot find a suitable base interval for '{interval}'. Exiting.")
            return
        bybit_interval_code = base_interval_str
        aggregate_factor = factor
        print(f"[Info] Desired interval '{interval}' not directly supported. Using base interval '{bybit_interval_code}' with aggregation factor {aggregate_factor}.")
    
    # 3) Validate Bybit's base interval
    if bybit_interval_code not in bybit_supported_intervals:
        print(f"[Error] Bybit does not support interval '{bybit_interval_code}'. Exiting.")
        return

    # 4) Convert timestamps
    start_ts = datetime_to_timestamp_ms(start_time)
    end_ts   = datetime_to_timestamp_ms(end_time)
    if start_ts >= end_ts:
        print("[Error] start_time must be earlier than end_time.")
        return
    
    print(f"[Info] Fetching Bybit Mark Price Kline: symbol={symbol}, interval={interval}, start={start_time}, end={end_time}, limit={limit}")
    
    all_data = []
    current_end = end_ts
    
    # 5) Start paging
    while True:
        df = fetch_bybit_mark_price_kline(
            symbol=symbol,
            category=category,
            bybit_interval=bybit_interval_code,  # Possibly '240'
            start=None,                          # Do not pass
            end=current_end,
            limit=limit,
            max_retries=max_retries,
            backoff_factor=backoff_factor
        )
        
        if df.empty:
            print("[Info] No more data fetched or request failed.")
            break
        
        # Sort in ascending order
        df = df.sort_values("timestamp")

        # Filter [start_ts, end_ts]
        df = df[
            (df["timestamp"] >= timestamp_ms_to_datetime(start_ts)) &
            (df["timestamp"] <= timestamp_ms_to_datetime(end_ts))
        ]
        if df.empty:
            print("[Info] No data in the specified time range after filtering.")
            break
        
        all_data.append(df)
        print(f"[Info] Fetched {len(df)} records. Total so far: {sum(len(x) for x in all_data)}")
        
        earliest_ts = int(df["timestamp"].iloc[0].timestamp() * 1000)
        if earliest_ts <= start_ts:
            print("[Info] Reached the start of specified time range.")
            break
        
        # Next pagination
        current_end = earliest_ts - 1
        
        # Rate limiting
        time.sleep(random.uniform(0.2, 0.3))

    if not all_data:
        print("[Warning] No data fetched. Nothing to save.")
        return

    # 6) Combine
    combined_df = pd.concat(all_data, ignore_index=True)
    combined_df.drop_duplicates(subset=["timestamp"], inplace=True)
    combined_df.sort_values("timestamp", inplace=True)

    # 7) If secondary aggregation is needed (e.g., '8h'), perform aggregation
    if aggregate_factor > 1:
        combined_df = aggregate_candles(combined_df, aggregate_factor, interval)
        print(f"[Info] Aggregated data to interval '{interval}' with factor {aggregate_factor}.")

    if combined_df.empty:
        print("[Warning] Aggregated/combined DataFrame is empty. No data to save.")
        return

    # 8) Save to CSV
    if not os.path.exists("data"):
        os.makedirs("data")
    
    friendly_start_time = filename_friendly_date(start_time)
    friendly_end_time   = filename_friendly_date(end_time)
    filename = f"{filename_prefix}_{symbol.upper()}_{friendly_start_time}_{friendly_end_time}_{interval}.csv"
    filepath = os.path.join("data", filename)

    combined_df.to_csv(filepath, index=False)
    print(f"[Info] Data saved to {filepath}")
    
# ==============Fetch Data ==============

def fetch_all_mark_prices(
    base_symbol: str,
    expiry: str,
    start_time: str,
    end_time: str,
    interval: str = '8h',
    exchanges: list = ['binance', 'deribit', 'okx', 'bybit']
) -> None:
    """
    Iterate through specified exchanges, convert symbols, and fetch Mark Price data.

    Parameters:
        base_symbol (str): Base symbol, e.g., 'BTC'
        expiry (str): Expiry date in 'YYMMDD' format, e.g., '250328'
        start_time (str): Start time 'YYYY-MM-DD HH:MM:SS'
        end_time (str): End time 'YYYY-MM-DD HH:MM:SS'
        interval (str): Time interval, e.g., '8h'
        exchanges (list): List of exchanges, e.g., ['binance', 'deribit', 'okx', 'bybit']
    """
    for exchange in exchanges:
        try:
            exchange_symbol = convert_symbol(exchange, base_symbol, expiry)
            print(f"\n=== Fetching Mark Price for {exchange.capitalize()} with symbol {exchange_symbol} ===")

            if exchange == 'binance':
                mark_price_df = getsave_data_kline(
                    get_mark_price_klines,
                    filename_prefix=f'{exchange_symbol}_Binance_markPrice',
                    symbol=exchange_symbol,
                    start_time=start_time,
                    end_time=end_time,
                    interval=interval
                )
            elif exchange == 'deribit':
                print("Deribit data fetching is slow, please wait...")
                # Deribit's Instrument Name needs to be specific
                instrument = exchange_symbol  # e.g., 'BTC-28MAR25'
                mark_price_df = save_deribit_kline_data(
                    instrument_name=instrument,
                    filename_prefix=f'{exchange_symbol}_deribit_markPrice',
                    start_time=start_time,
                    end_time=end_time,
                    interval=interval  # Adjust as needed
                )
            elif exchange == 'okx':
                inst_id = exchange_symbol  # OKX's Product ID
                mark_price_df = save_okx_mark_price_data(
                    inst_id=inst_id,
                    start_time=start_time,
                    end_time=end_time,
                    interval=interval,  # Target aggregation interval
                    bar="4H",      # API's minimum granularity
                    filename_prefix=f'{exchange_symbol}_OKX_markprice',
                    limit=100,      # API's maximum per request
                    max_retries=5,
                    backoff_factor=0.5
                )
            elif exchange == 'bybit':
                symbol_bybit = exchange_symbol
                category = "linear"  # Default parameter
                mark_price_df = save_bybit_mark_price_data(
                    symbol=symbol_bybit,
                    category=category,
                    interval=interval,
                    start_time=start_time,
                    end_time=end_time,
                    filename_prefix=f"{exchange_symbol}_Bybit_markprice",
                    limit=200,
                    max_retries=5,
                    backoff_factor=0.5
                )
            else:
                print(f"Unsupported exchange: {exchange}")
                continue

            print(f"=== Finished fetching Mark Price for {exchange.capitalize()} ===")
        except Exception as e:
            print(f"[Error] Failed to fetch data for {exchange.capitalize()}: {e}")

# ============== Main Execution ==============

if __name__ == '__main__':

    # ====== Index Price =======
    pair_for_index = 'BTCUSD'
    index_price_df = getsave_data_kline(
        get_index_price_klines,
        filename_prefix=f'{pair_for_index}_indexPrice',
        pair=pair_for_index,
        start_time="2025-01-04 16:00:00",
        end_time="2025-01-05 08:00:00",
        interval='8h'
    )
    
    # == Mark Price == 
    base_symbol = 'BTC'
    expiry = '250328'  # Format like '250328'
    start_time = "2025-01-04 16:00:00"
    end_time = "2025-01-05 08:00:00"
    interval = '8h'  # Time interval > 4H for OKX

    exchanges = ['binance', 'deribit', 'okx', 'bybit']

    
    fetch_all_mark_prices(
        base_symbol=base_symbol,
        expiry=expiry,
        start_time=start_time,
        end_time=end_time,
        interval=interval,
        exchanges=exchanges
    )
