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
import time
from datetime import datetime
from IBWrapper import IBWrapper, contract
from ib.ext.EClientSocket import EClientSocket
from ib.ext.ScannerSubscription import ScannerSubscription
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

sched = BackgroundScheduler()
import openpyxl

###############################################

callback = IBWrapper()
tws = EClientSocket(callback)
host = ""
port = 7497
clientId = 100
create = contract()
callback.initiate_variables()

acc = "DU228380"
total_cap = 2000000
Core_port_levrg = 1
LDN_curr_levrg = 16
NY_curr_levrg = 10




commonwealth_curr = ['GBP', 'AUD', 'NZD', 'EUR']


def conn():
    status = tws.isConnected()
    if status == False:
        # print("######### $$$$$$$$$$$$$$$$$$ RECONNECTING TWS SESSION   $$$$$$$$$$$$$$$$$$############")
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




def historical(fortime, con, totaldays, Id1):
    date = (datetime.now() + timedelta(3)).strftime("%Y%m%d") + fortime

    # date = datetime.now().strftime("%Y%m%d") + fortime
    df = pd.DataFrame()
    while df.empty:
        # Id1=Id1+1
        tws.reqHistoricalData(Id1, con, date,
                              totaldays,
                              '1 day',
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
    return data_df


time_ldn=" 00:30:00"
histId=300
lag_val = 1
MA_val = 20

entry_thresh = 0.01
exit_thresh1 = 0.04
exit_thresh2 = 0.03
conn()
##########################################################
# contract = create.create_contract(params[0][0:3], "CASH", "IDEALPRO", params[0][4:7], '', '', '', '')
contract=contract1("NOK","CASH","IDEALPRO","SEK","","","","")
asset_hist_data = pd.DataFrame()
asset_hist_data = historical(time_ldn, contract, "26 D", histId)
# asset_hist_data = asset_hist_data[:-1]
close_data = asset_hist_data["close"].iloc[-1]
close_data = round(float(close_data), 5)

asset_hist_data = asset_hist_data[["open", "high", "low", "close"]]

dat = asset_hist_data
# dat=dat.tail(22)
dat['temp'] = dat['close'].shift(lag_val)

dat['entry_HC_Spread'] = (
            abs((dat['high'] / dat['close'].shift(lag_val) - 1).rolling(MA_val).mean()) * entry_thresh).shift(1)
dat['exit_HC_Spread1'] = (
        abs((dat['high'] / dat['close'].shift(lag_val) - 1).rolling(MA_val).mean()) * exit_thresh1).shift(1)
dat['exit_HC_Spread2'] = (
        abs((dat['high'] / dat['close'].shift(lag_val) - 1).rolling(MA_val).mean()) * exit_thresh2).shift(1)

dat['buy_entry'] = 0
dat['sell_exit1'] = 0
dat['sell_exit2'] = 0

dat = dat.dropna()
#########################################
myindex1 = dat.loc[(dat['open'] > dat['temp'])].index
dat['buy_entry'].loc[myindex1] = dat['open'].loc[myindex1] + (
        dat['open'].loc[myindex1] * dat['entry_HC_Spread'].loc[myindex1])

dat['sell_exit1'].loc[myindex1] = dat['open'].loc[myindex1] + (
        dat['open'].loc[myindex1] * dat['exit_HC_Spread1'].loc[myindex1])

dat['sell_exit2'].loc[myindex1] = dat['open'].loc[myindex1] + (
        dat['open'].loc[myindex1] * dat['exit_HC_Spread2'].loc[myindex1])

#############################################################

myindex1 = dat.loc[(dat['buy_entry'] == 0)].index
dat['buy_entry'].loc[myindex1] = dat['temp'].loc[myindex1] + (
        dat['temp'].loc[myindex1] * dat['entry_HC_Spread'].loc[myindex1])

dat['sell_exit1'].loc[myindex1] = dat['temp'].loc[myindex1] + (
        dat['temp'].loc[myindex1] * dat['exit_HC_Spread1'].loc[myindex1])

dat['sell_exit2'].loc[myindex1] = dat['temp'].loc[myindex1] + (
        dat['temp'].loc[myindex1] * dat['exit_HC_Spread2'].loc[myindex1])

#########################################################
dat = dat.dropna()

buy_entry = (round(dat["buy_entry"].iloc[-1], 4))
sell_exit1 = (round(dat["sell_exit1"].iloc[-1], 4))
sell_exit2 = (round(dat["sell_exit2"].iloc[-1], 4))

print("BUY ENTRY:", buy_entry, "SELL EXIT1:", sell_exit1, "SELL EXIT2:", sell_exit2)
#print(asset_hist_data)

# dat = asset_hist_data
# # dat=dat.tail(22)
# dat['temp'] = dat['close'].shift(lag_val)
# dat['entry_HC_Spread'] = (
#        abs((dat['high'] / dat['close'].shift(lag_val) - 1).rolling(MA_val).mean()) * entry_thresh).shift(1)
# dat['entry_LC_Spread'] = (
#        abs((dat['low'] / dat['close'].shift(lag_val) - 1).rolling(MA_val).mean()) * entry_thresh).shift(1)
# dat['exit_HC_Spread'] = (
#        abs((dat['high'] / dat['close'].shift(lag_val) - 1).rolling(MA_val).mean()) * exit_thresh).shift(1)
# dat['exit_LC_Spread'] = (
#        abs((dat['low'] / dat['close'].shift(lag_val) - 1).rolling(MA_val).mean()) * exit_thresh).shift(1)
# dat['long_entry'] = 0
# dat['long_exit'] = 0
# dat = dat.dropna()
#
# myindex1 = dat.loc[(dat['open'] > dat['temp'])].index
# dat['long_entry'].loc[myindex1] = dat['open'].loc[myindex1] + (
#        dat['open'].loc[myindex1] * dat['entry_HC_Spread'].loc[myindex1])
# dat['long_exit'].loc[myindex1] = dat['open'].loc[myindex1] + (
#        dat['open'].loc[myindex1] * dat['exit_HC_Spread'].loc[myindex1])
#
# myindex1 = dat.loc[(dat['long_entry'] == 0)].index
# dat['long_entry'].loc[myindex1] = dat['temp'].loc[myindex1] + (
#        dat['temp'].loc[myindex1] * dat['entry_HC_Spread'].loc[myindex1])
# dat['long_exit'].loc[myindex1] = dat['temp'].loc[myindex1] + (
#        dat['temp'].loc[myindex1] * dat['exit_HC_Spread'].loc[myindex1])
#
# long_entry = (round(dat["long_entry"].iloc[-1], 4))
# long_exit = (round(dat["long_exit"].iloc[-1], 4))
#
# print("LONG ENTRY:", long_entry, " ||   LONG EXIT:", long_exit)
#
# dat['short_entry'] = 0
# dat['short_exit'] = 0
# myindex1 = dat.loc[(dat['open'] < dat['temp'])].index
# dat['short_entry'].loc[myindex1] = dat['open'].loc[myindex1] - (
#        dat['open'].loc[myindex1] * dat['entry_LC_Spread'].loc[myindex1])
# dat['short_exit'].loc[myindex1] = dat['open'].loc[myindex1] - (
#        dat['open'].loc[myindex1] * dat['exit_LC_Spread'].loc[myindex1])
#
# myindex1 = dat.loc[(dat['short_entry'] == 0)].index
# dat['short_entry'].loc[myindex1] = dat['temp'].loc[myindex1] - (
#        dat['temp'].loc[myindex1] * dat['entry_LC_Spread'].loc[myindex1])
# dat['short_exit'].loc[myindex1] = dat['temp'].loc[myindex1] - (
#        dat['temp'].loc[myindex1] * dat['exit_LC_Spread'].loc[myindex1])
# dat = dat.dropna()
#
# short_entry = (round(dat["short_entry"].iloc[-1], 4))
# short_exit = (round(dat["short_exit"].iloc[-1], 4))
# print(short_entry, short_exit)
#
# print("SHORT ENTRY:", short_entry, " ||   SHORT EXIT:", short_exit)