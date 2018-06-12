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


# def historical(fortime, con, totaldays, Id1):
#     date = (datetime.now()).strftime("%Y%m%d") + fortime
#
#     # date = datetime.now().strftime("%Y%m%d") + fortime
#     df = pd.DataFrame()
#     while df.empty:
#         # Id1=Id1+1
#         tws.reqHistoricalData(Id1, con, date,
#                               totaldays,
#                               '1 day',
#                               "MIDPOINT",
#                               0,
#                               1)
#         time.sleep(2)
#         df = list(callback.historical_Data)
#         for k in range(0, len(df)):
#             df[k] = tuple(df[k])
#         df = pd.DataFrame(df,
#                           columns=["reqId", "date", "open",
#                                    "high", "low", "close",
#                                    "volume", "count", "WAP",
#                                    "hasGaps"])
#         df = df[df.reqId == Id1]
#     # tws.cancelHistoricalData(Id1)
#     data_df = pd.DataFrame()
#     data_df = df[:-1]
#     # data_df=data_df.tail(20)
#     tws.cancelHistoricalData(Id1)
#     # df=df.tail(31)
#     return data_df,Id1

def historical(fortime, con, totaldays, Id1,freq):
    #date = (datetime.now()+timedelta(2)).strftime("%Y%m%d") + fortime
    date = '20180607'+fortime
    # date = datetime.now().strftime("%Y%m%d") + fortime
    df = pd.DataFrame()
    while df.empty:
        # Id1=Id1+1
        tws.reqHistoricalData(Id1, con, date,
                              totaldays,
                              freq,
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
    return data_df,Id1

conn()
tp_buy = 0
tp_buy2 = 0
tp_sell = 0
tp_sell2 = 0

#time_ny = ' 00:30:00'
time_ldn = ' 22:30:00'
#exeprice = 313.120  #######to_update
params = ['AUD.NZD']
round_tp = 5
lookback = 20
tp1= 0.24
tp2=0.22

histId = 20000
contract = create.create_contract(params[0][0:3], "CASH", "IDEALPRO", params[0][4:7], '', '', '', '')
# asset_hist_data,histId = historical(time_ldn, contract, "26 D", histId+1,'1 day')
# asset_hist_data = asset_hist_data[:-1]
# close_data=asset_hist_data['close'].iloc[-1]
# close_data = round(float(close_data), 5)
# print(asset_hist_data)
###########if todays close not available
#######please use 30 mins data to get close

asset_hist_data1,histId = historical(' 22:40:00', contract, "1 D", histId+1,'5 mins')
asset_hist_data1 = asset_hist_data1[:-1]
close_data = asset_hist_data1.loc[pd.to_datetime(asset_hist_data1["date"])== '20180605 '+time_ldn,"close"].iloc[-1]
close_data = round(float(close_data), 5)


#entry_result = [exeprice]



# tp_buy = round(float(((asset_hist_data['close'] - (tp1 * abs((pd.Series.rolling(
#     (asset_hist_data['low'].shift(-1) / asset_hist_data['close'] - 1).shift(1),
#     window=lookback).mean()).dropna()) * asset_hist_data['close'])).dropna()).iloc[-1]),
#                round_tp)
#
# tp_buy2 = round(float(((asset_hist_data['close'] - (tp2 * abs((pd.Series.rolling(
#     (asset_hist_data['low'].shift(-1) / asset_hist_data['close'] - 1).shift(1),
#     window=lookback).mean()).dropna()) * asset_hist_data['close'])).dropna()).iloc[-1]),
#                 round_tp)
#
#
#
# tp_sell = round(float(((asset_hist_data['close'] + (tp1 * abs((pd.Series.rolling(
#     (asset_hist_data['high'].shift(-1) / asset_hist_data['close'] - 1).shift(1),
#     window=lookback).mean()).dropna()) * asset_hist_data['close'])).dropna()).iloc[-1]), round_tp)
#
# tp_sell2 = round(float(((asset_hist_data['close'] + (tp2 * abs((pd.Series.rolling(
#     (asset_hist_data['high'].shift(-1) / asset_hist_data['close'] - 1).shift(1),
#     window=lookback).mean()).dropna()) * asset_hist_data['close'])).dropna()).iloc[-1]), round_tp)
#
# print('close@22:30',close_data)
# print("tp_sell1",tp_sell)
# print("tp buy1",tp_buy)
#print("tp sell2",tp_sell2)

#print("tp_buy2",tp_buy2)



# def mkt_depth(contract, tickerId):
#     ask, bid = 1, 1
#
#     attempts = [0]
#
#     while ask == 1 or bid == 1:
#         data_mktdepth = pd.DataFrame()
#         tickerId = tickerId + 1
#
#         while data_mktdepth.empty:
#             tws.reqMktDepth(tickerId, contract, 5)
#             attempts.append(attempts[-1] + 1)
#             if attempts[-1] > 10:
#                 print("ATTEMPTING HARDER", attempts[-1])
#                 sleeptime = 1
#             else:
#                 sleeptime = 0.1
#             time.sleep(sleeptime)
#             operation_type = {0: "Insert",
#                               1: "Update",
#                               2: "Delete", }
#             side_type = {0: "Ask",

#                          1: "Bid"}
#
#             data_mktdepth = list(callback.update_MktDepth)
#             for k in range(0, len(data_mktdepth)):
#                 data_mktdepth[k] = tuple(data_mktdepth[k])
#             data_mktdepth = pd.DataFrame(data_mktdepth,
#                                          columns=["tickerId", "position",
#                                                   "operation", "side",
#                                                   "price", "size"])
#
#         data_mktdepth["operation_type"] = data_mktdepth["operation"].map(operation_type)
#         data_mktdepth["side_type"] = data_mktdepth["side"].map(side_type)
#         # print(data_mktdepth.tail(8))
#         # print(data_mktdepth[-10:])
#         # ask = data_mktdepth.loc[data_mktdepth["side"] == '1', 'price'].iloc[-1]
#         tws.cancelMktDepth(tickerId)
#         tws.cancelMktData(tickerId)
#         tws.cancelRealTimeBars(tickerId)
#         data_mktdepth = data_mktdepth[data_mktdepth.tickerId == tickerId]
#         status1 = 'Ask' in data_mktdepth.side_type.values
#         status2 = 'Bid' in data_mktdepth.side_type.values
#         if status1 == False or status2 == False:
#             ask = 1
#             bid = 1
#         else:
#             ask = 0
#             bid = 0
#
#     ask, ask_size = data_mktdepth.loc[data_mktdepth["side_type"] == 'Ask', ['price', 'size']].iloc[-1]
#     bid, bid_size = data_mktdepth.loc[data_mktdepth["side_type"] == 'Bid', ['price', 'size']].iloc[-1]
#
#     return ask, bid
# #histId=tickerId()


# for i in range(50):
#     ret_list = mkt_depth(contract, 12)
#     time.sleep(0.5)
#     print(ret_list)

# ret_list=mkt_depth(contract, 1023)
