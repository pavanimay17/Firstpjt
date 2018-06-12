import numpy as np
import math
import time
import timeit
import random
##DB Instance
# from pymongo import MongoClient
#
# client = MongoClient('localhost', 27017)
# db = client.currencyData
#
# from CURRENCY_FOREX_2018 import curr_name
from IBWrapper import IBWrapper, contract
from ib.ext.EClientSocket import EClientSocket
from ib.opt import ibConnection, message
import warnings

warnings.filterwarnings('ignore')
from ib.ext.ScannerSubscription import ScannerSubscription
import time
import csv
import timeit
import random
import pandas as pd
from datetime import datetime, timedelta
import threading
from threading import Thread
import multiprocessing
from multiprocessing import Process
# from flask import Flask, url_for, request, render_template, redirect
from apscheduler.schedulers.background import BackgroundScheduler


###############################################

callback = IBWrapper()
tws = EClientSocket(callback)
host = ""
port = 7496
clientId = 100
create = contract()
callback.initiate_variables()

def conn():
    status = tws.isConnected()
    if status == False:
        print("######### $$$$$$$$$$$$$$$$$$ RECONNECTING TWS SESSION   $$$$$$$$$$$$$$$$$$############")
        tws.eConnect(host, port, clientId)
        # print("######### $$$$$$$$$$$$$$$$$$$ TWS CONNECTED   $$$$$$$$$$$$$$$$$$############")
    # else:

    # print("######### $$$$$$$$$$$$$$$$$$$ TWS CONNECTION INTACT  $$$$$$$$$$$$$$$$$$############")
    return


def disconn():
    tws.eDisconnect()
    tws.eDisconnect()
    # print("######### $$$$$$$$$$$$$$$$$$$ TWS DISCONNECTED   $$$$$$$$$$$$$$$$$$############")
    return


def contract1(sym, sec, exc, curr, blank, blank2, expiry, mul):
    contract_Details = create.create_contract(sym, sec, exc, curr, blank, blank2, expiry, mul)
    return contract_Details



def tickerId():
    a = random.sample(range(60001, 90000), 2000)
    b = random.sample(range(1, 10000), 40)
    random.shuffle(a)
    random.shuffle(b)
    tickerId = a[0] + b[0]
    return tickerId

def historicalHL(fortime, con, totaldays, Id1):
    date = datetime.now().strftime("%Y%m%d ") + fortime
    df = pd.DataFrame()
    while df.empty:
        # Id1=Id1+1
        tws.reqHistoricalData(Id1, con, date,
                              totaldays,
                              '30 mins',
                              "MIDPOINT",
                              0,
                              1)
        time.sleep(2)
        df = list(callback.historical_Data)
        for k in range(0, len(df)):
            df[k] = tuple(df[k])
        df = pd.DataFrame(df,
                          columns=["reqId", "date", "open",
                                   "high", "low", "close",
                                   "volume", "count", "WAP",
                                   "hasGaps"])
        df = df[df.reqId == Id1]
    # tws.cancelHistoricalData(Id1)
    data_df = pd.DataFrame()
    data_df = df[:-1]
    # data_df=data_df.tail(20)
    tws.cancelHistoricalData(Id1)
    # df=df.tail(31)
    return data_df, Id1

conn()

def ldn_HighLowfun():
    london_list = ["AUD.NZD", "CHF.NOK", "CHF.SEK", "NZD.CHF", "GBP.SEK", "EUR.NZD", "EUR.NOK", "EUR.SEK", "GBP.NOK",
                   "NOK.SEK"]
    round=5
    print("Getting High Low")
    df_common_inputs = pd.read_excel('C:/database/FX_DAILY/FOREX_settings.xlsx', 'common_inputs')
    ldn_closetime = df_common_inputs["CLOSE TIME"].iloc[-1].strftime("%H:%M:%S")

    histId = 4000
    HLlist=[]
    week_day = datetime.now().weekday()
    for curr in london_list:

        contract = create.create_contract(curr[0:3], "CASH", "IDEALPRO", curr[4:7], '', '', '', '')
        asset_hist_data,histId = historicalHL(ldn_closetime, contract, "4 D", histId+1)
        asset_hist_data = asset_hist_data[:-1]
        asset_hist_data.index=asset_hist_data['date']
        if week_day != 0:
            asset_hist_data=asset_hist_data.loc[(datetime.now()-timedelta(days=1)).strftime("%Y%m%d  ")+ldn_closetime:,]
        else:
            asset_hist_data = asset_hist_data.loc[
                              (datetime.now() - timedelta(days=3)).strftime("%Y%m%d  ") + ldn_closetime:,]

        temp_hl=[asset_hist_data['high'].max(),asset_hist_data['low'].min()]
        HLlist.append(temp_hl)

    if datetime.now().weekday != 0:
        datestring = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")
    else:
        datestring = (datetime.now() - timedelta(days=3)).strftime("%Y%m%d")
    path = "C:/Users/Kaushtuv/Downloads/" + datestring + "_LONDON.csv"
    csv_df = pd.read_csv(path)
    for i in range(len(london_list)):
        csv_df.loc[csv_df.CURRENCY == london_list[i], 'HIGH'] = HLlist[i][0]
        csv_df.loc[csv_df.CURRENCY == london_list[i], 'LOW'] = HLlist[i][1]

    csv_df.to_csv(path, index=False)
    return



