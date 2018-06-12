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
import openpyxl
import schedule

###############################################

callback = IBWrapper()
tws = EClientSocket(callback)
host = ""
port = 7498
clientId = 100
create = contract()
callback.initiate_variables()

acc = "DU228380"



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


def mkt_depth(contract,start,stop):
    ask, bid = 1, 1

    attempts = [0]

    while ask == 1 or bid == 1:
        conn()
        data_mktdepth = pd.DataFrame()
        tickerId = random.randrange(start, stop)

        while data_mktdepth.empty:
            # conn()
            tws.reqMktDepth(tickerId, contract, 5)
            attempts.append(attempts[-1] + 1)
            if attempts[-1] > 7:
                print("ATTEMPTING HARDER", attempts[-1])
                sleeptime = 1
                # disconn()
                # conn()
            else:
                sleeptime = 0.5
            time.sleep(sleeptime)



            operation_type = {0: "Insert",
                              1: "Update",
                              2: "Delete", }

            side_type = {0: "Ask",
                         1: "Bid"}

            data_mktdepth = list(callback.update_MktDepth)
            for k in range(0, len(data_mktdepth)):
                data_mktdepth[k] = tuple(data_mktdepth[k])
            data_mktdepth = pd.DataFrame(data_mktdepth,
                                         columns=["tickerId", "position",
                                                  "operation", "side",
                                                  "price", "size"])[-20:]

        data_mktdepth["operation_type"] = data_mktdepth["operation"].map(operation_type)
        data_mktdepth["side_type"] = data_mktdepth["side"].map(side_type)
        # print(data_mktdepth.tail(8))
        # print(data_mktdepth[-10:])
        # ask = data_mktdepth.loc[data_mktdepth["side"] == '1', 'price'].iloc[-1]
        tws.cancelMktDepth(tickerId)
        #tws.cancelMktData(tickerId)
        # tws.cancelRealTimeBars(tickerId)
        data_mktdepth = data_mktdepth[data_mktdepth.tickerId == tickerId]
        status1 = 'Ask' in data_mktdepth.side_type.values
        status2 = 'Bid' in data_mktdepth.side_type.values
        if status1 == False or status2 == False:
            ask = 1
            bid = 1
        else:
            ask = 0
            bid = 0

    # pd.set_option('display.height', 1000)
    # pd.set_option('display.max_rows', 500)
    # pd.set_option('display.max_columns', 500)
    # pd.set_option('display.width', 1000)

    df_rt=pd.DataFrame()
    df_ask = data_mktdepth.loc[data_mktdepth["side_type"] == 'Ask', ['price', 'size']]
    df_bid = data_mktdepth.loc[data_mktdepth["side_type"] == 'Bid', ['price', 'size']]

    df_ask=df_ask.reset_index(drop=True)
    df_bid=df_bid.reset_index(drop=True)
    df_rt=pd.concat([df_ask,df_bid],axis=1,ignore_index=True)
    df_rt['Date']= [datetime.now().strftime("%Y%m%d %H:%M:%S")]*len(df_rt)
    df_rt.columns=["ASK","ASK_SIZE","BID","BID_SIZE","DATE"]
    df_rt=df_rt[["DATE","ASK","ASK_SIZE","BID","BID_SIZE"]].fillna(0)
    df_rt[['ASK_SIZE',"BID_SIZE"]] = df_rt[['ASK_SIZE',"BID_SIZE"]].astype("int64")

    #print(df_rt)

    # print(df_rt)


    return df_rt



def streaming_data(symbol,start,stop,df):
    conn()
    contract=create.create_contract(symbol[0:3],"CASH","IDEALPRO",symbol[3:])

    file = pd.read_excel("C:\database\FX_DAILY/FOREX_settings.xlsx", "MKT_DEPTH")
    pause=file["PAUSE"].iloc[-1]

    while pause=="no":
        l2data = mkt_depth(contract, start, stop)
        d = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        # pd.set_option('display.height', 1000)
        # pd.set_option('display.max_rows', 500)
        # pd.set_option('display.max_columns', 500)
        # pd.set_option('display.width', 1000)
        df=df.append(l2data)
        # df = df.append({'DATE': d, "ASK": l2data[0], "BID": l2data[1]}, ignore_index=True)

        # df.loc[-1] = [d, l2data[0],l2data[1], l2data[2],l2data[3]]  # adding a row
        # df.index = df.index + 1  # shifting index
        # df.sort_index(inplace=True)
        # print(df)


        filename = "E:\MKT_DEPTH/" + symbol + ".html"
        df.to_html(filename,col_space=200,justify ="center",index=False)
        file = pd.read_excel("C:\database\FX_DAILY/FOREX_settings.xlsx", "MKT_DEPTH")
        pause = file["PAUSE"].iloc[-1]
        update=file["UPDATE"].iloc[-1]
        if update=="yes":
            d = datetime.now().strftime("%Y%m%d%H%M%S")
            filename="E:\MKT_DEPTH/" + symbol + "_"+d+".csv"
            df.to_csv(filename)


def run_data():
    GBPSEK_df = pd.DataFrame(columns=["DATE", "ASK", "ASK_SIZE", "BID", "BID_SIZE"])
    NOKSEK_df = pd.DataFrame(columns=["DATE", "ASK", "ASK_SIZE", "BID", "BID_SIZE"])
    EURSEK_df = pd.DataFrame(columns=["DATE", "ASK", "ASK_SIZE", "BID", "BID_SIZE"])
    EURNOK_df = pd.DataFrame(columns=["DATE", "ASK", "ASK_SIZE", "BID", "BID_SIZE"])
    CHFNOK_df = pd.DataFrame(columns=["DATE", "ASK", "ASK_SIZE", "BID", "BID_SIZE"])
    CHFSEK_df = pd.DataFrame(columns=["DATE", "ASK", "ASK_SIZE", "BID", "BID_SIZE"])
    NZDCHF_df = pd.DataFrame(columns=["DATE", "ASK", "ASK_SIZE", "BID", "BID_SIZE"])
    AUDNZD_df = pd.DataFrame(columns=["DATE", "ASK", "ASK_SIZE", "BID", "BID_SIZE"])
    EURNZD_df = pd.DataFrame(columns=["DATE", "ASK", "ASK_SIZE", "BID", "BID_SIZE"])
    GBPNOK_df = pd.DataFrame(columns=["DATE", "ASK", "ASK_SIZE", "BID", "BID_SIZE"])

    # Thread(target=streaming_data, args=("AUDNZD", 1,100, AUDNZD_df)).start()
    Thread(target=streaming_data, args=("NOKSEK", 101,200, NOKSEK_df)).start()
    #
    Thread(target=streaming_data, args=("EURSEK", 201,300, EURSEK_df)).start()
    #
    Thread(target=streaming_data, args=("EURNOK", 301,400, EURNOK_df)).start()

    Thread(target=streaming_data, args=("CHFNOK", 601, 700, CHFNOK_df)).start()


    # Thread(target=streaming_data, args=("EURNZD", 401,500, EURNZD_df)).start()
    #
    # # time.sleep(5)

    # Thread(target=streaming_data, args=("CHFSEK", 501,600, CHFSEK_df)).start()
    #



    # Thread(target=streaming_data, args=("NZDCHF", 701,800, NZDCHF_df)).start()
    #

    # Thread(target=streaming_data, args=("GBPSEK", 801,900, GBPSEK_df)).start()
    #

    # Thread(target=streaming_data, args=("GBPNOK", 901,1000, GBPNOK_df)).start()

    return


conn()

run_data()
