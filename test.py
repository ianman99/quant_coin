import os
from dotenv import load_dotenv
load_dotenv()
import pyupbit
import pandas as pd
import pandas_ta as ta
import json
import time
from datetime import datetime, timedelta
import requests
import pytz
import numpy as np
import schedule
import pymysql

# DB connection
sql_connection = pymysql.connect(
        host=os.getenv('DB_HOST'),
        user=os.getenv('DB_USER'),
        password=os.getenv('DB_PASSWORD'),
        database=os.getenv('DB_NAME')
    )

# Login
upbit = pyupbit.Upbit(os.getenv("UPBIT_ACCESS_KEY"), os.getenv("UPBIT_SECRET_KEY"))

def execute_buy():
    try:
        mykrw = upbit.get_balance("KRW")
        buykrw = mykrw * 0.25 # buy weight
        if buykrw > 5000:
            result = upbit.buy_market_order("KRW-BTC", buykrw)
            print("Buy order successful:", result)
            # send to mysql
            result['timestamp'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')  
            result['order_type'] = 'buy'  
            with sql_connection.cursor() as cursor:
                sql = """
                INSERT INTO record (timestamp, order_type, uuid, side, ord_type, state, market, created_at, volume, 
                                             remaining_volume, reserved_fee, remaining_fee, paid_fee, locked, 
                                             executed_volume, trades_count)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
                cursor.execute(sql, (
                    result['timestamp'], result['order_type'], result['uuid'], result['side'], result['ord_type'], 
                    result['state'], result['market'], result['created_at'], result['volume'], 
                    result['remaining_volume'], result['reserved_fee'], result['remaining_fee'], 
                    result['paid_fee'], result['locked'], result['executed_volume'], result['trades_count']
                ))
                sql_connection.commit()
    except Exception as e:
        print(f"Failed to execute buy order: {e}")

def execute_sell():
    try:
        mybtc = upbit.get_balance("BTC")
        sellbtc = mybtc * 0.1 # sell weight
        btc_current_price = pyupbit.get_orderbook(ticker="KRW-BTC")['orderbook_units'][0]["ask_price"]
        if btc_current_price*sellbtc > 5000:
            result = upbit.sell_market_order("KRW-BTC", sellbtc)
            print("Sell order successful:", result)
            # send to mysql
            result['timestamp'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')  
            result['order_type'] = 'sell'  
            with sql_connection.cursor() as cursor:
                sql = """
                INSERT INTO record (timestamp, order_type, uuid, side, ord_type, state, market, created_at, volume, 
                                             remaining_volume, reserved_fee, remaining_fee, paid_fee, locked, 
                                             executed_volume, trades_count)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
                cursor.execute(sql, (
                    result['timestamp'], result['order_type'], result['uuid'], result['side'], result['ord_type'], 
                    result['state'], result['market'], result['created_at'], result['volume'], 
                    result['remaining_volume'], result['reserved_fee'], result['remaining_fee'], 
                    result['paid_fee'], result['locked'], result['executed_volume'], result['trades_count']
                ))
                sql_connection.commit()
    except Exception as e:
        print(f"Failed to execute sell order: {e}")

def main():
    # time setting
    utc = pytz.utc
    current_time_utc = datetime.now(utc)
    minutes_ago_utc = current_time_utc - timedelta(minutes=1)
    
    # get volume data
    df_volume = pyupbit.get_ohlcv("KRW-BTC", interval="minute1", count=1440, to=minutes_ago_utc)
    df_volume = df_volume['volume']
    df_volume = df_volume.reset_index()
    df_volume.columns = ["timestamp", "volume"]
    df_volume["ln_volume"] = np.log(df_volume["volume"])
    latest_ln_volume = df_volume.iloc[-1]["ln_volume"]

    # representative value
    mean_ln_volume = df_volume["ln_volume"].mean()
    std_ln_volume = df_volume["ln_volume"].std()

    # 95% confidence interval
    confidence_interval_upper = mean_ln_volume + (1.96 * std_ln_volume)
    confidence_interval_lower = mean_ln_volume - (1.96 * std_ln_volume)

    # trading logic
    if latest_ln_volume >= confidence_interval_upper:
        execute_sell()
            
    elif confidence_interval_lower < latest_ln_volume < confidence_interval_upper:
        print("hold")

    else:
        execute_buy()


# Run every minute one second 
schedule.every().minute.at(":01").do(main)

while True:
    schedule.run_pending()
