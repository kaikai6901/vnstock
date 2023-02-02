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
list_ticker = get_data_from_mysql(con, query_ticker)

today = str((datetime.today() - timedelta(hours=5)).date())
yesterday = str((datetime.today() - timedelta(days=2)).date())
today_timestamp = int((datetime.today()).timestamp())
yesterday_timestamp = int((datetime.today()).timestamp())
i = 0
for ticker in list_ticker['ticker']:
    i += 1
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
        priceParams = {'ticker': ticker, 'type': 'stock', 'from': 1675168823, 'to': 1675273223, 'resolution':'D'}
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
    if i % 20 == 0:
        print(i)
con.close()
print("DONE!!!")