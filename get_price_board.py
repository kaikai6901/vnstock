import requests
from utils.helper import *
from utils.config import dbInfor
import numpy as np
import pandas as pd
import pymysql
import sqlalchemy
import sys
from datetime import datetime, timedelta

con = get_mysql_connection()

query_ticker = "select distinct ticker from ticker_overview"
list_ticker = run_sql(con, query_ticker)

today = str(datetime.today().date())
yesterday = str((datetime.today() - timedelta(days=1)).date())
today_timestamp = int(datetime.today().timestamp())
yesterday_timestamp = int((datetime.today() - timedelta(days=1)).timestamp())

for ticker in list(list_ticker['ticker']):
    try:
        intradayURL = f"https://apipubaws.tcbs.com.vn/stock-insight/v1/intraday/{ticker}/his/paging"
        params = {"page": 0, "size": 1}
        response = requests.get(intradayURL, params=params)
        res = response.json()
        ba = 0
        sa = 0
        if res['total'] > 0:
            intraday = pd.DataFrame(res['data'])
            ba = intraday.iloc[0]['ba']
            sa = intraday.iloc[0]['sa']

        priceURL = f"https://apipubaws.tcbs.com.vn/stock-insight/v1/stock/bars-long-term"
        priceParams = {'ticker': ticker, 'type': 'stock', 'from': yesterday_timestamp, 'to': today_timestamp, 'resolution':'D'}
        response = requests.get(priceURL, params=priceParams)
        price = pd.DataFrame(response.json()['data'])
        row = price.iloc[0]
        open = int(row['open'])
        high = int(row['high'])
        low = int(row['low'])
        close = int(row['close'])
        volume = int(row['volume'])
        delete_by_ticker(con, 'price_board', ticker, today)
        query = f"insert into price_board (ticker, date, open, high, low, close, volume, ba, sa) values ('{ticker}', '{today}', {open}, {high},{low},{close},{volume},{ba}, {sa})"
        run_sql(con, query)
    except Exception as e:
        print(ticker)
        print(e)
con.close()
print("DONE!!!")