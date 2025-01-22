import os
import calendar
import time
import random
import pytz
from datetime import datetime, timedelta, timezone
from dateutil.relativedelta import relativedelta
import pandas as pd
import requests
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from data_query import getsave_data_kline, fetch_all_mark_prices, get_index_price_klines, get_funding_rate_history_deribit


# ================== Helper Functions ==================

def datetime_to_timestamp_ms(dt_str: str) -> int:
    """
    Convert 'YYYY-MM-DD HH:MM:SS' to UTC timestamp in milliseconds.
    """
    dt = datetime.strptime(dt_str, "%Y-%m-%d %H:%M:%S")
    dt_utc = dt.replace(tzinfo=timezone.utc)
    return int(dt_utc.timestamp() * 1000)

def timestamp_ms_to_datetime(ts: int) -> datetime:
    """
    Convert timestamp (ms) to UTC datetime object.
    """
    return datetime.fromtimestamp(ts / 1000, tz=timezone.utc)

def filename_friendly_date(date_str: str) -> str:
    """
    Convert date string to a file-friendly format.
    """
    return date_str.replace(':', '-').replace(' ', '_')

def clean_funding_rate_data_deribit(data: list) -> pd.DataFrame:
    """
    Clean Deribit funding rate data and convert to DataFrame.
    """
    if not data:
        return pd.DataFrame()
    
    df = pd.DataFrame(data)
    df['timestamp'] = df['timestamp'].apply(timestamp_ms_to_datetime)
    df.rename(columns={
        'index_price': 'Index Price',
        'prev_index_price': 'Previous Index Price',
        'interest_8h': 'Interest 8H',
        'interest_1h': 'Interest 1H'
    }, inplace=True)

    return df

def get_quarter_last_friday_utc(year: int, quarter: int) -> datetime:
    """
    Get the last Friday of a quarter in UTC (08:00).
    """
    if quarter not in [1, 2, 3, 4]:
        raise ValueError("Quarter must be 1, 2, 3, or 4.")

    end_month = quarter * 3
    month_days = calendar.monthrange(year, end_month)[1]
    last_dt = datetime(year, end_month, month_days, 8, 0, 0, tzinfo=timezone.utc)

    while last_dt.weekday() != 4:  # Friday = 4
        last_dt -= timedelta(days=1)
    return last_dt

def get_next_quarter(year: int, quarter: int) -> tuple:
    """
    Get the next quarter.
    """
    if quarter == 4:
        return year + 1, 1
    else:
        return year, quarter + 1

def get_next_n_quarters(year: int, quarter: int, n: int) -> tuple:
    """
    Get the year and quarter after n quarters.
    """
    total_quarters = quarter + n
    new_year = year + (total_quarters - 1) // 4
    new_quarter = ((total_quarters - 1) % 4) + 1
    return new_year, new_quarter

def get_next_n_months(year: int, month: int, n: int) -> tuple:
    """
    Get the year and month after n months.
    """
    new_month = month + n
    new_year = year + (new_month - 1) // 12
    new_month = ((new_month - 1) % 12) + 1
    return new_year, new_month

def get_date_code(date_str: str) -> str:
    """
    Convert 'YYYY-MM-DD HH:MM:SS' to 'YYMMDD'.
    """
    dt = datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S')
    return dt.strftime('%y%m%d')

def minutes_until(now_datetime, target_datetime, tz_info=timezone.utc) -> float:
    """
    Calculate minutes until target_datetime from now_datetime.
    """
    if now_datetime.tzinfo is None:
        now_datetime = now_datetime.replace(tzinfo=timezone.utc)
    else:
        now_datetime = now_datetime.astimezone(tz_info)

    if target_datetime.tzinfo is None:
        target_datetime = target_datetime.replace(tzinfo=timezone.utc)
    else:
        target_datetime = target_datetime.astimezone(tz_info)

    if now_datetime > target_datetime:
        print(f'now_datetime: {now_datetime}, target_datetime: {target_datetime}')
        return None

    delta = target_datetime - now_datetime
    return delta.total_seconds() / 60.0



def get_last_weekday_of_month(year: int, month: int, weekday: int) -> datetime:
    """
    Get the last weekday of a month.
    """
    last_day = calendar.monthrange(year, month)[1]
    last_date = datetime(year, month, last_day, tzinfo=timezone.utc)
    while last_date.weekday() != weekday:
        last_date -= timedelta(days=1)
    return last_date

def get_next_month(year: int, month: int) -> tuple:

    if month == 12:
        return year + 1, 1
    else:
        return year, month + 1

# ================== Delivery Date Calculation ==================

def calculate_delivery_dates(start_time: datetime, end_time: datetime, expiry_type: str) -> list:
    """
    Calculate delivery dates based on expiry type.
    """
    delivery_dates = []
    current = start_time

    while current <= end_time:
        if expiry_type == 'weekly':
            next_friday = current + timedelta((4 - current.weekday()) % 7)
            delivery_date = next_friday.replace(hour=8, minute=0, second=0, microsecond=0)
            if delivery_date < current:
                delivery_date += timedelta(weeks=1)
            if delivery_date > end_time:
                delivery_dates.append(delivery_date)
                break
            delivery_dates.append(delivery_date)
            current = delivery_date + timedelta(seconds=1)

        elif expiry_type == 'bi-weekly':
            next_friday = current + timedelta((4 - current.weekday()) % 7)
            delivery_date = next_friday.replace(hour=8, minute=0, second=0, microsecond=0)
            if delivery_date < current:
                delivery_date += timedelta(weeks=2)
            if delivery_date > end_time:
                delivery_dates.append(delivery_date)
                break
            delivery_dates.append(delivery_date)
            current = delivery_date + timedelta(seconds=1)

        elif expiry_type == 'monthly':
            year = current.year
            month = current.month
            last_friday = get_last_weekday_of_month(year, month, 4)  # Friday=4
            delivery_date = last_friday.replace(hour=8, minute=0, second=0, microsecond=0)
            if delivery_date < current:
                year, month = get_next_month(year, month)
                last_friday = get_last_weekday_of_month(year, month, 4)
                delivery_date = last_friday.replace(hour=8, minute=0, second=0, microsecond=0)
            if delivery_date > end_time:
                delivery_dates.append(delivery_date)
                break
            delivery_dates.append(delivery_date)
            current = delivery_date + timedelta(seconds=1)

        elif expiry_type == 'bi-monthly':
            year = current.year
            month = current.month
            last_friday = get_last_weekday_of_month(year, month, 4)
            delivery_date = last_friday.replace(hour=8, minute=0, second=0, microsecond=0)
            if delivery_date < current:
                year, month = get_next_n_months(year, month, 2)
                last_friday = get_last_weekday_of_month(year, month, 4)
                delivery_date = last_friday.replace(hour=8, minute=0, second=0, microsecond=0)
            if delivery_date > end_time:
                delivery_dates.append(delivery_date)
                break
            delivery_dates.append(delivery_date)
            current = delivery_date + timedelta(seconds=1)

        elif expiry_type == 'quarterly':
            year = current.year
            quarter = (current.month - 1) // 3 + 1
            last_friday = get_quarter_last_friday_utc(year, quarter)
            delivery_date = last_friday
            if delivery_date < current:
                year, quarter = get_next_quarter(year, quarter)
                last_friday = get_quarter_last_friday_utc(year, quarter)
                delivery_date = last_friday
            if delivery_date > end_time:
                delivery_dates.append(delivery_date)
                break
            delivery_dates.append(delivery_date)
            current = delivery_date + timedelta(seconds=1)

        elif expiry_type == 'bi-quarterly':
            year = current.year
            quarter = (current.month - 1) // 3 + 1
            last_friday = get_quarter_last_friday_utc(year, quarter)
            delivery_date = last_friday
            if delivery_date < current:
                year, quarter = get_next_n_quarters(year, quarter, 2)
                last_friday = get_quarter_last_friday_utc(year, quarter)
                delivery_date = last_friday
            if delivery_date > end_time:
                delivery_dates.append(delivery_date)
                break
            delivery_dates.append(delivery_date)
            current = delivery_date + timedelta(seconds=1)

        else:
            raise ValueError(f"Unsupported expiry type: {expiry_type}")
    
    print(f"Found {len(delivery_dates)} delivery dates.")
    print(f'Delivery dates: {delivery_dates}')
    return delivery_dates

def calculate_fixed_rate(merged_df: pd.DataFrame, season_end_dt: datetime) -> pd.DataFrame:
    """
    Calculate fixed rate based on the merged data.

    Parameters:
        merged_df (pd.DataFrame): Merged mark price and funding rate data.
        season_end_dt (datetime): The expiration time for the current record.

    Returns:
        pd.DataFrame: DataFrame with the calculated fixed rate.
    """
    MIN_ONE_YEAR = 60 * 24 * 365  # One year in minutes

    # Compute the remaining minutes until expiry
    def compute_min_til_m(close_time):
        return minutes_until(close_time, season_end_dt)

    # Apply the function to calculate the remaining minutes
    merged_df['min_til_m'] = merged_df['close_time'].apply(compute_min_til_m)
    
    # Filter out rows where the remaining minutes are non-positive
    merged_df = merged_df[merged_df['min_til_m'] > 0]

    # Calculate the fixed rate using the formula
    merged_df['fixed_rate'] = (
        ((merged_df['mark_close'] / merged_df['index_close']) - 1) /
        (merged_df['min_til_m'] / MIN_ONE_YEAR)
    )

    # Convert the funding rate to annualized percentage
    merged_df['fundingrate'] = merged_df['fundingrate'] * 1095

    return merged_df