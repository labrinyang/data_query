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

from data_query import convert_symbol
from data_query import getsave_data_kline, fetch_all_mark_prices, get_index_price_klines, get_funding_rate_history_deribit
from tools import calculate_delivery_dates, get_date_code, filename_friendly_date, calculate_fixed_rate

os.makedirs("data", exist_ok=True)

def get_mark_price_data(symbol, expiry_code, data_start_time, data_fetch_end_str):
    """
    Check if Mark Price data is already saved; if not, fetch and save it.
    """
    exchange = 'deribit'
    exchange_symbol = convert_symbol(exchange, symbol, expiry_code)
    mark_price_filename = f"data/kline/{exchange_symbol}_{exchange}_markPrice_{exchange_symbol}_{filename_friendly_date(data_start_time)}_{filename_friendly_date(data_fetch_end_str)}_8h.csv"
    print(f"Mark Price data file path: {mark_price_filename}")
    
    if os.path.exists(mark_price_filename):
        print(f"Mark Price data exists, loading file: {mark_price_filename}")
        mark_price_df = pd.read_csv(mark_price_filename)
    else:
        print(f"Mark Price data file does not exist, fetching data...")
        mark_price_df = fetch_all_mark_prices(
            base_symbol=symbol,
            expiry=expiry_code,
            start_time=data_start_time,
            end_time=data_fetch_end_str,
            interval='8h',
            exchanges=['deribit']
        )
        
        if mark_price_df.empty:
            print("Mark Price data is empty, skipping this delivery date.")
            return None

        mark_price_df.to_csv(mark_price_filename, index=False)
        print(f"Mark Price data saved to file: {mark_price_filename}")
    
    return mark_price_df

def draw_fixed_rate_funding_rate_volume(symbol, expiry_type, start_time_str, end_time_str):
    print(f"Processing fixed rate, funding rate, and volume data for {symbol} {expiry_type} options...")
    symbol = symbol.upper()
    expiry_type = expiry_type.lower()

    start_time = datetime.strptime(start_time_str, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
    end_time = datetime.strptime(end_time_str, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)

    # ================== Calculate Delivery Dates ==================
    delivery_dates = calculate_delivery_dates(start_time, end_time, expiry_type)
    if not delivery_dates:
        print("No delivery dates found in the specified time range.")
        exit()

    print("Calculated delivery dates:")
    for dt in delivery_dates:
        print(dt.strftime("%Y-%m-%d %H:%M:%S"))
        
    all_data = []  # Store all processed data points
    previous_end_time = None  # Track the previous end time
    
    for i, delivery_date in enumerate(delivery_dates):
        delivery_str = delivery_date.strftime("%Y-%m-%d %H:%M:%S")
        print(f"\nProcessing delivery date: {delivery_str}")
        
        if i == 0:
            current_start_time = start_time
            data_start_time = start_time_str
        else:
            current_start_time = previous_end_time + timedelta(hours=8)
            data_start_time = current_start_time.strftime("%Y-%m-%d %H:%M:%S")
        
        if i == len(delivery_dates) - 1:
            data_fetch_end_time = end_time
        else:
            data_fetch_end_time = delivery_date
        previous_end_time = data_fetch_end_time
        data_fetch_end_str = data_fetch_end_time.strftime("%Y-%m-%d %H:%M:%S")
        
        funding_rate_df = get_funding_rate_history_deribit(
            instrument_name=f"{symbol}-PERPETUAL",
            start_time=data_start_time,
            end_time=data_fetch_end_str,
            clean=True
        )
        
        if funding_rate_df.empty:
            print(f"No funding rate data found from {data_start_time} to {data_fetch_end_str}.")
            continue
        funding_rate_df['fundingrate'] = funding_rate_df['Interest 8H']
        
        mark_price_df = get_mark_price_data(
            symbol=symbol,
            expiry_code=get_date_code(delivery_str),
            data_start_time=data_start_time,
            data_fetch_end_str=data_fetch_end_str
        )
        
        if mark_price_df is None:
            continue
        
        mark_price_df['close_time'] = pd.to_datetime(mark_price_df['close_time'], utc=True)
        funding_rate_df['timestamp'] = pd.to_datetime(funding_rate_df['timestamp'], utc=True)
        
        merged_df = pd.merge(mark_price_df, funding_rate_df, left_on='close_time', right_on='timestamp', how='inner')
        if merged_df.empty:
            print("No overlapping data between Mark Price and funding rate.")
            continue
        
        print('Preparing to calculate fixed rate...')
        merged_df = calculate_fixed_rate(merged_df.copy(), delivery_date)
        
        if merged_df.empty:
            print("No data after fixed rate calculation.")
            continue
        
        all_data.append(merged_df)

    # ================== Combine All Data ==================
    if not all_data:
        print("No data to visualize.")
        exit()
        
    combined_df = pd.concat(all_data, ignore_index=True)

    # ================== Data Visualization ==================
    fig = make_subplots(specs=[[{"secondary_y": True}]])

    fig.add_trace(
        go.Scatter(
            x=combined_df['close_time'],
            y=combined_df['fixed_rate'],
            mode='lines',
            name='Fixed Rate',
            line=dict(color='skyblue'),
            marker=dict(symbol='circle', size=6),
            opacity=0.9
        ),
        secondary_y=False
    )

    fig.add_trace(
        go.Scatter(
            x=combined_df['close_time'],
            y=combined_df['fundingrate'],
            mode='lines',
            name='Funding Rate',
            line=dict(color='lightcoral'),
            marker=dict(symbol='diamond', size=6),
            opacity=0.5
        ),
        secondary_y=False
    )

    fig.add_trace(
        go.Bar(
            x=combined_df['close_time'],
            y=combined_df['volume'],
            name='Trading Volume',
            marker_color='gray',
            opacity=0.4
        ),
        secondary_y=True
    )

    for delivery_date in delivery_dates[:-1]:
        if isinstance(delivery_date, datetime):
            fig.add_vline(
                x=delivery_date.timestamp() * 1000,
                line_dash="dash",
                line_color="red",
                opacity=0.3,
                annotation_text=f"Delivery {delivery_date.strftime('%Y-%m-%d %H:%M')}",
                annotation_position='top left',
                annotation=dict(
                    font=dict(size=10, color="red"),
                    textangle=-90,
                    yref="paper",
                    y=1,
                    opacity=0.3
                )
            )

    fig.update_layout(
        title=f'Deribit_{symbol}_{expiry_type}_from_{start_time_str}_to_{end_time_str} Fixed Rate, Funding Rate, and Volume',
        xaxis_title='Time',
        legend_title='Indicators',
        xaxis_rangeslider_visible=False,
        height=600
    )

    fig.update_yaxes(title_text="Rate", secondary_y=False)
    fig.update_yaxes(title_text="Volume (USD)", secondary_y=True)

    fig.show()
    
    return combined_df, delivery_dates

if __name__ == "__main__":
    symbol = 'BTC'  #
    expiry_type = 'quarterly'  
    start_time_str = "2020-01-01 00:00:00"
    end_time_str = "2021-01-01 00:00:00"

    df_BTCq_2425, delivery_dates_BTCq_2425 = draw_fixed_rate_funding_rate_volume(symbol, expiry_type, start_time_str, end_time_str)
