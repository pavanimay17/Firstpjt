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
# from docutils.nodes import entry
import random
from IBWrapper import IBWrapper, contract
from ib.ext.EClientSocket import EClientSocket
from ib.ext.ScannerSubscription import ScannerSubscription
from ib.opt import ibConnection, message
import warnings

warnings.filterwarnings('ignore', category=DeprecationWarning)
import warnings

from functools import lru_cache as cache
# @cache(maxsize=None)

# from numba import jit, autojit


# with warnings.catch_warnings():
#     warnings.filterwarnings("ignore", category=DeprecationWarning)
#     import imp
# import pip

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
port = 7498
clientId = 100
create = contract()
callback.initiate_variables()

acc = "DU228380"

commonwealth_curr = ['GBP', 'AUD', 'NZD', 'EUR']


def conn():
    status = tws.isConnected()
    if status == False:
        print("######### $$$$$$$$$$$$$$$$$$ RECONNECTING TWS SESSION   $$$$$$$$$$$$$$$$$$############")
        tws.eConnect(host, port, clientId)
        # print("######### $$$$$$$$$$$$$$$$$$$ TWS CONNECTED   $$$$$$$$$$$$$$$$$$############")
    # else:

    # print("######### $$$$$$$$$$$$$$$$$$$ TWS CONNECTION INTACT  $$$$$$$$$$$$$$$$$$############")
    return


# conn()
# tws.setServerLogLevel(5)

def disconn():
    tws.eDisconnect()
    tws.eDisconnect()
    # print("######### $$$$$$$$$$$$$$$$$$$ TWS DISCONNECTED   $$$$$$$$$$$$$$$$$$############")
    return


def error_handler(msg):
    """Handles the capturing of error messages"""
    print("Server Error: %s" % msg)


def reply_handler(msg):
    """Handles of server replies"""
    print("Server Response: %s, %s" % (msg.typeName, msg))


def contract1(sym, sec, exc, curr, blank, blank2, expiry, mul):
    contract_Details = create.create_contract(sym, sec, exc, curr, blank, blank2, expiry, mul)
    return contract_Details


def realtime(contract_Details):
    data = pd.DataFrame()
    while data.empty:
        conn()
        a = random.sample(range(60001, 60300), 40)
        b = random.sample(range(60301, 60600), 40)
        random.shuffle(a)
        random.shuffle(b)
        tickerId = a[0] + b[0]
        # #print(datetime.now())
        tws.reqRealTimeBars(tickerId,
                            contract_Details,
                            5,
                            "MIDPOINT",
                            0)
        time.sleep(2)
        data = list(callback.real_timeBar)

        for i in range(0, len(data)):
            data[i] = tuple(data[i])

        data = pd.DataFrame.from_records(data,
                                         columns=["reqId", "time", "open", "high", "low", "close", "volume", "wap",
                                                  "count"])
        data = data[data.reqId == tickerId]
    data = data.tail(1)
    data = data.rename(index=str, columns={"time": "date"})
    return data


def tickerId():
    a = random.sample(range(60001, 90000), 2000)
    b = random.sample(range(1, 10000), 40)
    random.shuffle(a)
    random.shuffle(b)
    tickerId = a[0] + b[0]
    return tickerId


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
    return data_df, Id1


def ask_bid(contract, tickerId):
    tickedId = int(tickerId)

    # tws.cancelMktData(tickedId)
    tick = contract
    ask = 1
    bid = 1
    attempts = [0]
    while ask == 1 and bid == 1:
        sleeptime = 0.4

        conn()
        tickedId = int(tickerId + 1)
        tick_data1 = pd.DataFrame()
        while tick_data1.empty:
            instance = attempts[-1]
            if instance > 4:
                conn()
                print("ATTEMPTING HARDER:", instance)
                tickedId = int(tickerId + 1)
                tws.reqMktData(tickedId, tick, "", 0)
                # print("ask/bid unavailable at the moment")
                sleeptime = 1
                time.sleep(sleeptime)
            else:
                conn()
                tickedId = int(tickerId + 1)
                tws.reqMktData(tickedId, tick, "", 0)
                sleeptime = 0.4
                time.sleep(sleeptime)

            attempts.append(attempts[-1] + 1)

            tick_data1 = list(callback.tick_Price)
            for k in range(0, len(tick_data1)):
                tick_data1[k] = tuple(tick_data1[k])
            tick_data1 = pd.DataFrame(tick_data1,
                                      columns=['tickerId', 'field', 'price', 'canAutoExecute'])
            tick_data1 = tick_data1[tick_data1.tickerId == tickedId]
            tws.cancelMktData(tickedId)
        tick_type = {
            1: "BID PRICE",
            2: "ASK PRICE",
        }
        time.sleep(sleeptime / 2)

        tick_data1["Type"] = tick_data1["field"].map(tick_type)
        # #print(tick_data1)
        a = tick_data1
        # #print(a)
        status1 = 'ASK PRICE' in tick_data1.Type.values
        status2 = 'BID PRICE' in tick_data1.Type.values
        # time.sleep(1)
        # #print("ASK", status1, "BID", status2)
        if status1 == False or status2 == False:
            # print("ASK AND BID UNAVAILABLE")
            ask = 1
            bid = 1
        else:
            # print("ASK AND BID AVAILABLE")
            ask = a.loc[a["Type"] == 'ASK PRICE', 'price'].iloc[-1]
            bid = a.loc[a["Type"] == 'BID PRICE', 'price'].iloc[-1]
            ask = float(ask)
            bid = float(bid)
            # ask = a.loc[a["Type"] == 'ASK PRICE', 'price'].iloc[-1]
            # bid = a.loc[a["Type"] == 'BID PRICE', 'price'].iloc[-1]
            # ask=float(ask)
            # bid=float(bid)
        # tws.cancelMktData(tickedId)

    # print(ask,bid)
    # print(attempts)
    conn()
    tws.cancelMktData(tickedId)
    mid = float((ask + bid) / 2)
    # #print("OBTAINED CORRECT DETAILS FOR")

    # print(ask,bid)
    # q1.put([ask, bid, mid])
    return ask, bid, mid


def ask_bid_size(contract, tickerId):
    tickedId = int(tickerId)

    # tws.cancelMktData(tickedId)
    tick = contract
    ask = 1
    bid = 1
    attempts = [0]
    while ask == 1 and bid == 1:
        sleeptime = 0.4

        conn()
        tickedId = int(tickerId + 1)
        tick_data1 = pd.DataFrame()
        while tick_data1.empty:
            instance = attempts[-1]
            if instance > 4:
                conn()
                print("ATTEMPTING HARDER:", instance)
                tickedId = int(tickerId + 1)
                tws.reqMktData(tickedId, tick, "", False)
                # print("ask/bid unavailable at the moment")
                sleeptime = 1
                time.sleep(sleeptime)
            else:
                conn()
                tickedId = int(tickerId + 1)
                tws.reqMktData(tickedId, tick, "", False)
                sleeptime = 0.4
                time.sleep(sleeptime)

            attempts.append(attempts[-1] + 1)

            tick_data1 = list(callback.tick_Size)
            for k in range(0, len(tick_data1)):
                tick_data1[k] = tuple(tick_data1[k])
            tick_data1 = pd.DataFrame(tick_data1,
                                      columns=['tickerId', 'field', 'size'])
            tick_data1 = tick_data1[tick_data1.tickerId == tickedId]
            tws.cancelMktData(tickedId)
        tick_type = {0: "BID SIZE",
                     1: "BID PRICE",
                     2: "ASK PRICE",
                     3: "ASK SIZE",
                     4: "LAST PRICE",
                     5: "LAST SIZE",
                     6: "HIGH",
                     7: "LOW",
                     8: "VOLUME",
                     9: "CLOSE PRICE",
                     10: "BID OPTION COMPUTATION",
                     11: "ASK OPTION COMPUTATION",
                     12: "LAST OPTION COMPUTATION",
                     13: "MODEL OPTION COMPUTATION",
                     14: "OPEN_TICK",
                     15: "LOW 13 WEEK",
                     16: "HIGH 13 WEEK",
                     17: "LOW 26 WEEK",
                     18: "HIGH 26 WEEK",
                     19: "LOW 52 WEEK",
                     20: "HIGH 52 WEEK",
                     21: "AVG VOLUME",
                     22: "OPEN INTEREST",
                     23: "OPTION HISTORICAL VOL",
                     24: "OPTION IMPLIED VOL",
                     27: "OPTION CALL OPEN INTEREST",
                     28: "OPTION PUT OPEN INTEREST",
                     29: "OPTION CALL VOLUME"}
        time.sleep(sleeptime / 2)

        tick_data1["Type"] = tick_data1["field"].map(tick_type)
        # #print(tick_data1)
        a = tick_data1
        print(a.tail(10))

        status1 = 'ASK SIZE' in tick_data1.Type.values
        status2 = 'BID SIZE' in tick_data1.Type.values

        if status1 == False or status2 == False:
            # print("ASK AND BID UNAVAILABLE")
            ask = 1
            bid = 1
        else:
            # print("ASK AND BID AVAILABLE")
            ask = a.loc[a["Type"] == 'ASK SIZE', 'size'].iloc[-1]
            bid = a.loc[a["Type"] == 'BID SIZE', 'size'].iloc[-1]
            ask = int(ask)
            bid = int(bid)
            # ask = a.loc[a["Type"] == 'ASK PRICE', 'price'].iloc[-1]
            # bid = a.loc[a["Type"] == 'BID PRICE', 'price'].iloc[-1]
            # ask=float(ask)
            # bid=float(bid)
        # tws.cancelMktData(tickedId)
        # print(ask,bid)
        # print(attempts)
    conn()
    tws.cancelMktData(tickedId)

    # #print("OBTAINED CORRECT DETAILS FOR")
    # print(ask,bid)
    # q2.put([ask, bid])
    return ask, bid


def mkt_depth(contract, tickerId):
    ask, bid = 1, 1

    attempts = [0]

    while ask == 1 or bid == 1:
        data_mktdepth = pd.DataFrame()
        tickerId = tickerId + 1

        while data_mktdepth.empty:
            tws.reqMktDepth(tickerId, contract, 5)
            attempts.append(attempts[-1] + 1)
            if attempts[-1] > 10:
                print("ATTEMPTING HARDER", attempts[-1])
                sleeptime = 1
            else:
                sleeptime = 0.2
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
                                                  "price", "size"])

        data_mktdepth["operation_type"] = data_mktdepth["operation"].map(operation_type)
        data_mktdepth["side_type"] = data_mktdepth["side"].map(side_type)
        # print(data_mktdepth.tail(8))
        # print(data_mktdepth[-10:])
        # ask = data_mktdepth.loc[data_mktdepth["side"] == '1', 'price'].iloc[-1]
        tws.cancelMktDepth(tickerId)
        # tws.cancelMktData(tickerId)
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

    ask, ask_size = data_mktdepth.loc[data_mktdepth["side_type"] == 'Ask', ['price', 'size']].iloc[-1]
    bid, bid_size = data_mktdepth.loc[data_mktdepth["side_type"] == 'Bid', ['price', 'size']].iloc[-1]

    return ask, ask_size, bid, bid_size


def open_pos():
    for i in range(0, 5):
        tws.reqPositions()
        dat = list(callback.update_Position)
        if len(dat) == 0:
            # print("empty dat")
            dat = pd.DataFrame()
            # time.sleep(30-15*i)
            time.sleep(0.5)

        else:
            for k in range(0, len(dat)):
                dat[k] = tuple(dat[k])
            dat = pd.DataFrame.from_records(dat,
                                            columns=['Account', 'Contract ID', 'Currency', 'Exchange', 'Expiry',
                                                     'Include Expired', 'Local Symbol', 'Multiplier', 'Right',
                                                     'Security Type', 'Strike', 'Symbol', 'Trading Class',
                                                     'Position', 'Average Cost'])

            dat[dat["Account"] == 'DU536394']
            break

    return dat


conn()


######capital and weight#######################################

def generateNumber(num):
    order_id = []
    tws.reqIds(1)
    while not order_id:
        time.sleep(0.1)
        order_id = callback.next_ValidId
        break

    mylist = []
    for i in range(num):
        mylist.append(order_id)
        order_id = order_id + 1
    return mylist


def orderidfun(acc, contract, type, exitidlist, loops):
    exit_signal = "null"

    # loops = loops
    contract_info = contract
    # asset_type = type
    # if asset_type == 'LONG':
    #     exit_signal = 'SELL'
    # elif asset_type == 'SHORT':
    #     exit_signal = 'BUY'

    sellidlist = generateNumber(loops)
    while sellidlist[0] == exitidlist[-1][0]:
        sellidlist = generateNumber(loops)
    for i in sellidlist:
        order_info = create.create_order(acc, "LMT", 10000, "SELL", 10.000, False, False, None)
        tws.placeOrder(int(i), contract_info, order_info)
        time.sleep(0.2)

    buyidlist = generateNumber(loops)

    while buyidlist[0] == sellidlist[0]:
        buyidlist = generateNumber(loops)

    for i in buyidlist:
        order_info = create.create_order(acc, "LMT", 10000, "BUY", 10.000, False, False, None)
        tws.placeOrder(int(i), contract_info, order_info)
        time.sleep(0.2)

    loops = 1
    # exitidlist = generateNumber(loops)
    #
    # while exitidlist[0] == buyidlist[0]:
    #     exitidlist = generateNumber(loops)
    #
    # for i in exitidlist:
    #     order_info = create.create_order(acc, "LMT", 10000, exit_signal, 10.000, False, False, None)
    #     tws.placeOrder(int(i), contract_info, order_info)

    return sellidlist, buyidlist


def transmit_entry(dat, curr_name, qty, contract, ids, round1, signal, tot_time, fav_time, mid_time, tickerId,
                   closetime):
    qty = abs(qty)
    cycledate = datetime.now().strftime("%Y%m%d")
    db_cycledate = datetime.now().strftime("%Y%m%d %H:%M:%S")
    # dat = open_pos()
    price = 0
    status = "NULL"
    exeprice = 0
    filled = 0
    remain = 0
    exetime = "NULL"
    executed_type = "NULL"
    ask = 0
    bid = 0
    tickerId = int(tickerId)
    if (dat.empty) or (dat[(dat["Local Symbol"] == curr_name)].empty) or (
            int(dat[(dat["Local Symbol"] == curr_name)].iloc[-1]['Position']) == 0):
        print("no open pos")
        openpos = 0
    else:
        openpos = int(dat[(dat["Local Symbol"] == curr_name)].iloc[-1]['Position'])
        opensignal = "NONE"
        if openpos < 0:
            opensignal = 'SELL'
        elif openpos > 0:
            opensignal = 'BUY'
        ###for short
        if signal == "SELL":
            if opensignal == 'SELL':
                if qty == abs(openpos):
                    signal = 'null'
                    qty = 0
                    exeprice = 0
                    filled = 0
                    # print("##################open order exists#########################")
                elif qty < abs(openpos):
                    signal = 'BUY'
                    qty = abs(openpos) - qty
                elif qty > abs(openpos):
                    signal = 'SELL'
                    qty = qty - abs(openpos)
            elif opensignal == 'BUY':
                signal = 'SELL'
                qty = abs(openpos) + qty

        ### for long
        elif signal == "BUY":
            if opensignal == 'BUY':
                if qty == abs(openpos):
                    signal = 'null'
                    qty = 0
                    exeprice = 0
                    filled = 0
                    # print("##################open order exists#########################")
                elif qty < abs(openpos):
                    signal = 'SELL'
                    qty = abs(openpos) - qty
                elif qty > abs(openpos):
                    signal = 'BUY'
                    qty = qty - abs(openpos)
            elif opensignal == 'SELL':
                signal = 'BUY'
                qty = abs(openpos) + qty

        # if qty == 0:
        #     if opensignal == "BUY":
        #         signal="SELL"
        #         qty=abs(openpos)
        #     elif opensignal=="SELL":
        #         signal="BUY"
        #         qty=abs(openpos)

    ####################################################

    if signal == "null":
        # print("MOVE TO TP CALCULATION")
        signal = "null"
        exeprice = 0
        filled = 0
        qty = 0
        executed_type = "NULL"
        exetime = "NULL"
    else:
        #########################FUNCTION #################################################
        # exp_date = datetime.now() + timedelta(seconds=+tot_time*2)
        # exp_date = exp_date.strftime('%Y%m%d %H:%M:%S')
        # #print("GTD EXPIRES AT", exp_date)
        # validity=0
        # validity = datetime.now().strftime('%Y%m%d %H:%M:%S')
        if signal == "SELL":
            id = ids[0][0]
            ids[0].remove(id)
        elif signal == "BUY":
            id = ids[1][0]
            ids[1].remove(id)
        # print(id)
        conn()
        for i in range(tot_time):

            currtime = datetime.now().strftime('%Y%m%d %H:%M:%S')
            week_day = datetime.now().weekday()

            if week_day != 4 and "NZD" in curr_name:
                if "14:59:50" <= currtime[-8:] <= "15:15:05":
                    sleeptime = (datetime.strptime("15:15:10", '%H:%M:%S') - datetime.strptime(currtime[-8:],
                                                                                               '%H:%M:%S')).total_seconds()
                    print(curr_name + " sleeping till mkt opens at 15:15")
                    time.sleep(sleeptime)  # sleeptime

            else:
                if "16:59:50" <= currtime[-8:] <= "17:15:05":
                    sleeptime = (datetime.strptime("17:15:10", '%H:%M:%S') - datetime.strptime(currtime[-8:],
                                                                                               '%H:%M:%S')).total_seconds()
                    print(curr_name + " sleeping till mkt opens at 17:15")
                    time.sleep(sleeptime)  # sleeptime

            tickerId = tickerId
            rt_data = ask_bid(contract, tickerId)
            ####pass ask and bid info to df

            ask = rt_data[0]
            bid = rt_data[1]

            if fav_time != 0 and i < fav_time:
                if signal == "BUY":
                    price = rt_data[1]
                    executed_type = "BID"
                    # print(signal, "at BID")
                else:
                    price = rt_data[0]
                    executed_type = "ASK"
                    # print(signal, "at ASK")
            elif (mid_time != 0 and (mid_time + fav_time) > i >= fav_time):
                # print(signal, "at MIDPOINT")
                price = (rt_data[0] + rt_data[1]) / 2
                executed_type = "MID"
            else:
                if i >= (mid_time + fav_time):
                    if signal == "BUY":
                        price = rt_data[0]
                        executed_type = "ASK"
                        # print(signal, "at ASK")
                    else:
                        price = rt_data[1]
                        executed_type = "BID"
                        # print(signal, "at BID")

            price = float(round(price, round1))
            # #print("NEW PRICE", price)
            if price > 0:
                order_info = create.create_order(acc, "LMT", qty, signal, price, True, False,
                                                 None)
                tws.placeOrder(int(id), contract, order_info)

                time.sleep(1)
                confirm = pd.DataFrame()
                while confirm.empty:
                    confirm_data = list(callback.order_Status)
                    for j in range(0, len(confirm_data)):
                        confirm_data[j] = tuple(confirm_data[j])
                    confirm = pd.DataFrame(confirm_data,
                                           columns=['orderId', 'status', 'filled', 'remaining', 'avgFillPrice',
                                                    'permId', 'parentId', 'lastFillPrice', 'clientId', 'whyHeld'])

                validity = datetime.now().strftime('%Y%m%d %H:%M:%S')
                if confirm[confirm.orderId == id].empty == False:
                    status = confirm[confirm.orderId == id].tail(1).iloc[0]['status']
                    remain = confirm[confirm.orderId == id].tail(1).iloc[0]['remaining']
                    filled = confirm[confirm.orderId == id].tail(1).iloc[0]['filled']
                    if remain > 0:
                        partial_fill = int(filled)
                        remain_qty = int(remain)
                        exeprice = 0
                        round1 = 5
                        exetime = datetime.now().strftime("%Y:%m:%d %H:%M:%S")
                    elif remain == 0:
                        exeprice = confirm[confirm.orderId == id].tail(1).iloc[0]['avgFillPrice']
                        exeprice = float(round((exeprice), 5))
                        exetime = datetime.now().strftime("%Y:%m:%d %H:%M:%S")
                        # print("CONDITION MET,BREAKING ")
                        break
            else:
                print("wrong price received from IB")

        tws.cancelOrder(id)
    return exeprice, status, filled, remain, exetime, executed_type, signal, ask, bid, openpos, db_cycledate, cycledate


london_list = ["AUD.NZD", "CHF.NOK", "CHF.SEK", "NZD.CHF", "GBP.SEK", "EUR.NZD", "EUR.NOK", "EUR.SEK", "GBP.NOK",
               "NOK.SEK"]

ny_list = ["AUD.SGD", "CHF.PLN", "GBP.CZK", "GBP.PLN", "AUD.ZAR", "DKK.NOK", "EUR.HUF",
           'EUR.CZK', 'GBP.MXN']

london_df = pd.DataFrame(
    {"CURRENCY": london_list, "OPEN_POSITION": np.nan, "ENTRY_QTY": np.nan, "ENTRY_SIGNAL": np.nan, "ASK": np.nan,
     "BID": np.nan, "ENTRY_TYPE": np.nan,
     "ENTRY_EXE": np.nan, "ENTRY_TIME": np.nan,
     "BUY_EXIT_QTY": np.nan, "BUY_TP1": np.nan, "BUY_TP2": np.nan, "BUY_EXIT_EXE": np.nan, "BUY_EXIT_TIME": np.nan,
     "SELL_EXIT_QTY": np.nan, "SELL_TP1": np.nan, "SELL_TP2": np.nan, "SELL_EXIT_EXE": np.nan, "SELL_EXIT_TIME": np.nan,
     "CLOSE": np.nan, "CYCLE_DATE": np.nan})


london_exit_df = pd.DataFrame(
        {"CURRENCY": ["AUD.NZD", "CHF.NOK", "CHF.SEK", "NZD.CHF", "GBP.SEK", "EUR.NZD", "EUR.NOK", "EUR.NOK",
                      "EUR.SEK", "EUR.SEK", "GBP.NOK", "GBP.NOK", "NOK.SEK", "NOK.SEK"],
         "SIGNAL": ["BUY", "BUY", "BUY", "BUY", "BUY", "SELL", "BUY", "SELL", "BUY", "SELL", "BUY", "SELL", "BUY",
                    "SELL"],
         "MID": ['NO'] * 14,
         "BID": ['NO'] * 14,
         "ASK": ['NO'] * 14,
         "TP HIT": ['NO'] * 14,
         "FILLED": ['NO'] * 14,
         "FILLED@":[""]*14})


ny_df = pd.DataFrame(
    {"CURRENCY": ny_list, "OPEN_POSITION": np.nan, "ENTRY_QTY": np.nan, "ENTRY_SIGNAL": np.nan, "ASK": np.nan,
     "BID": np.nan, "ENTRY_TYPE": np.nan,
     "ENTRY_EXE": np.nan, "ENTRY_TIME": np.nan,
     "BUY_EXIT_QTY": np.nan, "BUY_TP1": np.nan, "BUY_TP2": np.nan, "BUY_EXIT_EXE": np.nan, "BUY_EXIT_TIME": np.nan,
     "SELL_EXIT_QTY": np.nan, "SELL_TP1": np.nan, "SELL_TP2": np.nan, "SELL_EXIT_EXE": np.nan, "SELL_EXIT_TIME": np.nan,
     "CLOSE": np.nan, "CYCLE_DATE": np.nan})


def ldn_report(london_df):
    time.sleep(500)

    # report_df=london_df[['CURRENCY',"CLOSE","OPEN_POSITION","ENTRY_SIGNAL","ENTRY_TYPE","ENTRY_QTY","ENTRY_EXE",
    #                      "ENTRY_TIME","BUY_EXIT_QTY","BUY_TP1","BUY_TP2","SELL_EXIT_QTY","SELL_TP1","SELL_TP2"]]

    # report_df[["ENTRY_QTY","OPEN_POSITION","BUY_EXIT_QTY","SELL_EXIT_QTY"]]=(report_df[["ENTRY_QTY","OPEN_POSITION","BUY_EXIT_QTY","SELL_EXIT_QTY"]]).astype(int)
    report_df = london_df[['CURRENCY', "BUY_EXIT_QTY", "BUY_TP1", "BUY_TP2", "SELL_EXIT_QTY", "SELL_TP1", "SELL_TP2"]]
    print("REPORT")
    print(report_df)
    report_df.to_csv("C:/LOG/LDN_LOG.csv", )
    # report_df.to_html("C:/LOG/LDN_LOG.html")

    return


def ny_report(ny_df):
    time.sleep(500)
    # report_df=ny_df[['CURRENCY',"CLOSE","OPEN_POSITION","ENTRY_SIGNAL","ENTRY_TYPE","ENTRY_QTY","ENTRY_EXE",
    #                      "ENTRY_TIME","BUY_EXIT_QTY","BUY_TP1","BUY_TP2","SELL_EXIT_QTY","SELL_TP1","SELL_TP2"]]
    report_df = ny_df[['CURRENCY', "BUY_EXIT_QTY", "BUY_TP1", "BUY_TP2", "SELL_EXIT_QTY", "SELL_TP1", "SELL_TP2"]]
    # report_df[["ENTRY_QTY", "OPEN_POSITION", "BUY_EXIT_QTY", "SELL_EXIT_QTY"]] = (report_df[["ENTRY_QTY", "OPEN_POSITION", "BUY_EXIT_QTY", "SELL_EXIT_QTY"]]).astype(int)
    print("REPORT")
    print(report_df)
    # report_df.to_html("C:/LOG/NY_LOG.html")
    report_df.to_csv("C:/LOG/NY_LOG.csv")
    return


def LONDON(dat, params, ids, tickerId, histId, london_df,london_exit_df):
    tp_buy = 0
    tp_buy2 = 0
    tp_sell = 0
    tp_sell2 = 0
    round1 = 4
    round_tp = 5
    exit_exeprice = 0
    condition1 = "NOT MET"
    filled = 0
    remain = 0
    list1 = []
    sleeptime = 0
    entry_result = ["null"] * 12
    qty = 0
    price = 0
    pos_diff = 0
    askflag = 'NO'
    bidflag = 'NO'

    df_switch = pd.read_excel('C:/database/FX_DAILY/FOREX_settings.xlsx', 'LONDON_SWITCH')
    df_gtd = pd.read_excel('C:/database/FX_DAILY/FOREX_settings.xlsx', 'EXIT_GTD')
    df_common_inputs = pd.read_excel('C:/database/FX_DAILY/FOREX_settings.xlsx', 'common_inputs')
    closetime = df_common_inputs["CLOSE TIME"].iloc[-1].strftime("%H:%M:%S")
    time_ldn = " " + closetime
    closetime = datetime.strptime(closetime, "%H:%M:%S")
    lookback1 = int(df_common_inputs["LOOKBACK"].iloc[-1])
    lookback = lookback1

    round1 = 5
    contract = create.create_contract(params[0][0:3], "CASH", "IDEALPRO", params[0][4:7], '', '', '', '')
    tp1 = float(df_switch.loc[df_switch.CURRENCY == params[0], 'TP1'].iloc[-1])
    tp2 = float(df_switch.loc[df_switch.CURRENCY == params[0], 'TP2'].iloc[-1])

    # tp1 = 0.004
    # tp2 = 0.002
    # ### temporary inputs###

    entry_result = transmit_entry(dat, params[0], params[2], contract, ids, round1, params[1], params[3], params[4],
                                  params[5], tickerId, closetime)  ###get the execution price
    print(params[0], entry_result)

    # time.sleep(1)

    # ###### END OF ENTRY #################################################################################################################
    #
    #
    # currtime = datetime.now().strftime('%Y%m%d %H:%M:%S')
    # if not "NZD" in params[0]:
    #     if currtime < (entry_result[-1] + ' ' + (closetime + timedelta(seconds=60)).strftime("%H:%M:%S")):
    #         sleeptime = ((closetime + timedelta(seconds=62)) - datetime.strptime(currtime[-8:],
    #                                                                              '%H:%M:%S')).total_seconds()
    #         print(params[0], "sleeping till" + " " + closetime.strftime("%H:%M:%S"))
    #         time.sleep(sleeptime)
    #         # time.sleep(10)
    #
    # ##########################regular mkt close#####################################
    # currtime = datetime.now().strftime('%Y%m%d %H:%M:%S')
    # week_day = datetime.now().weekday()
    # if week_day != 4 and "NZD" in params[0]:
    #     if "14:59:50" <= currtime[-8:] <= "15:15:05":
    #         sleeptime = (
    #                 datetime.strptime("15:15:10", '%H:%M:%S') - datetime.strptime(currtime[-8:],
    #                                                                               '%H:%M:%S')).total_seconds()
    #         print(params[0], "sleeping till mkt opens at 15:15")
    #         time.sleep(sleeptime)  # sleeptime
    #
    # else:
    #     if "16:59:50" <= currtime[-8:] <= "17:15:05":
    #         sleeptime = (
    #                 datetime.strptime("17:15:10", '%H:%M:%S') - datetime.strptime(currtime[-8:],
    #                                                                               '%H:%M:%S')).total_seconds()
    #         print(params[0], "sleeping till mkt opens at 17:15")
    #         time.sleep(sleeptime)  # sleeptime

    ########################################################################################################################
    HC_MA = params[-2]
    LC_MA = params[-1]

    if london_list.index(params[0]) <= 4:

        if entry_result[0] > 0:
            tp_buy = round(float((entry_result[0] - (tp1 * LC_MA * entry_result[0]))), round_tp)
            tp_buy2 = round(float((entry_result[0] - (tp2 * LC_MA * entry_result[0]))), round_tp)


    elif london_list.index(params[0]) == 5:

        if entry_result[0] > 0:
            tp_sell = round(float((entry_result[0] + (tp1 * HC_MA * entry_result[0]))), round_tp)
            tp_sell2 = round(float((entry_result[0] + (tp2 * HC_MA * entry_result[0]))), round_tp)

    elif london_list.index(params[0]) > 5:
        if entry_result[0] > 0:
            tp_buy = round(float((entry_result[0] - (tp1 * LC_MA * entry_result[0]))), round_tp)
            tp_buy2 = round(float((entry_result[0] - (tp2 * LC_MA * entry_result[0]))), round_tp)
            tp_sell = round(float((entry_result[0] + (tp1 * HC_MA * entry_result[0]))), round_tp)
            tp_sell2 = round(float((entry_result[0] + (tp2 * HC_MA * entry_result[0]))), round_tp)

    ####################################################################################################################
    currtime = datetime.now().strftime("%Y%m%d %H:%M:%S")
    if entry_result[0] == 0:
        if currtime[-8:] <= (closetime + timedelta(seconds=10)).strftime("%H:%M:%S"):
            sleeptime = ((closetime + timedelta(seconds=11)) - datetime.strptime(currtime[-8:],
                                                                                 '%H:%M:%S')).total_seconds()
            print(params[0], "sleeping till" + " " + closetime.strftime("%H:%M:%S"))
            time.sleep(sleeptime)

    ########################################GTD EXPDATE#################################################################
    week_day = datetime.now().weekday()
    exp_time = str(df_common_inputs["GTD"].iloc[-1])
    if (week_day == 4):

        num = int(df_gtd.loc[df_gtd.MKT == 'LONDON', "FRIDAY"].iloc[-1])
        exp_date1 = datetime.now() + timedelta(days=num)  ##changed for christmas&new year holiday
        exp_date1 = exp_date1.strftime('%Y%m%d') + " " + exp_time


    elif week_day == 5:
        num = int(df_gtd.loc[df_gtd.MKT == 'LONDON', "FRIDAY"].iloc[-1]) - 1
        exp_date1 = datetime.now() + timedelta(days=num)  ##changed for christmas&new year holiday
        exp_date1 = exp_date1.strftime('%Y%m%d') + " " + exp_time

    else:
        ###### consider all possible days from excel ###
        if week_day == 0:
            num = int(df_gtd.loc[df_gtd.MKT == 'LONDON', "MONDAY"].iloc[-1])
        elif week_day == 1:
            num = int(df_gtd.loc[df_gtd.MKT == 'LONDON', "TUESDAY"].iloc[-1])
        elif week_day == 2:
            num = int(df_gtd.loc[df_gtd.MKT == 'LONDON', "WEDNESDAY"].iloc[-1])
        elif week_day == 3:
            num = int(df_gtd.loc[df_gtd.MKT == 'LONDON', "THURSDAY"].iloc[-1])

        exp_date1 = datetime.now() + timedelta(days=num)
        exp_date1 = exp_date1.strftime('%Y%m%d') + " " + exp_time

    ###################################################################################################################
    dat = open_pos()
    if (dat.empty) or (dat[(dat["Local Symbol"] == params[0])].empty) or (
            int(dat[(dat["Local Symbol"] == params[0])].iloc[-1]['Position']) == 0):
        pos = 0
    else:
        pos = int(dat[(dat["Local Symbol"] == params[0])].iloc[-1]['Position'])
        if london_list.index(params[0]) > 5:
            pos_diff = int(dat[(dat["Local Symbol"] == params[0])].iloc[-1]['Position'])

    ###################################################################################################################

    currtime = datetime.now().strftime("%Y%m%d %H:%M:%S")
    exitid = []
    signallist = []

    if currtime[-8:] < (closetime + timedelta(seconds=11)).strftime("%H:%M:%S"):
        if pos != 0:
            exitid = []
            signallist = []

            if london_list.index(params[0]) <= 4:  ##########short model
                signal, id, price, qty = "BUY", int(ids[1][0]), tp_buy, abs(pos)
                order_info = create.create_order(acc, "LMT", int(qty), signal, float(price), True, 'GTD',
                                                 exp_date1)
                tws.placeOrder(id, contract, order_info)
                exitid.append(id)
                signallist.append(signal)
            elif london_list.index(params[0]) == 5:  ###########long model
                signal, id, price, qty = "SELL", int(ids[0][0]), tp_sell, abs(pos)
                order_info = create.create_order(acc, "LMT", int(qty), signal, float(price), True, 'GTD',
                                                 exp_date1)
                tws.placeOrder(id, contract, order_info)
                exitid.append(id)
                signallist.append(signal)
            elif london_list.index(params[0]) > 5:  ###############LS model

                pos_diff = int(dat[(dat["Local Symbol"] == params[0])].iloc[-1]['Position'])

                signal, id, price = "SELL", int(ids[0][0]), tp_sell

                if pos_diff < 0:
                    qty = abs(entry_result[3]) + abs(params[6])
                else:
                    qty = abs(params[6])

                order_info = create.create_order(acc, "LMT", int(qty), signal, float(price), True, 'GTD',
                                                 exp_date1)
                tws.placeOrder(id, contract, order_info)

                exitid.append(id)
                signallist.append(signal)

                signal, id, price = "BUY", int(ids[1][0]), tp_buy

                if pos_diff > 0:
                    qty = abs(entry_result[3]) + abs(params[7])
                else:
                    qty = abs(params[7])

                order_info = create.create_order(acc, "LMT", int(qty), signal, float(price), True, 'GTD',
                                                 exp_date1)
                tws.placeOrder(id, contract, order_info)

                exitid.append(id)
                signallist.append(signal)
            print("Placed Standing Order for", params[0])
    #############################sending out standby orders#####################################

    exit_sell_list = [tp_sell, "null", "null", "null", "null"]
    exit_buy_list = [tp_buy, "null", "null", "null", "null"]

    if (dat.empty) or (dat[(dat["Local Symbol"] == params[0])].empty) or (
            int(dat[(dat["Local Symbol"] == params[0])].iloc[-1]['Position']) == 0):
        pos = 0
        condition_buy = "NOT FULFILLED"
        condition_sell = "NOT FULFILLED"
    else:

        pos = int(dat[(dat["Local Symbol"] == params[0])].iloc[-1]['Position'])
        round1 = 4
        condition_buy = "NOT FULFILLED"
        condition_sell = "NOT FULFILLED"

        actual_tp_buy = tp_buy
        actual_tp_sell = tp_sell

        flag_buy = False
        flag_sell = False
        close_data = 0
        while pos != 0:
            conn()
            #askflag, bidflag = "NO", "NO"
            ###############sleep for regular mkt close############
            currtime = datetime.now().strftime("%Y%m%d %H:%M:%S")
            week_day = datetime.now().weekday()

            if close_data == 0 and currtime[-8:] >= (closetime + timedelta(seconds=10)).strftime("%H:%M:%S"):
                data_size = 0
                while data_size < 2:
                    asset_hist_data = pd.DataFrame()
                    asset_hist_data, tid = historical(time_ldn, contract, "6 D", histId)
                    data_size = asset_hist_data.shape[0]
                    print("HISTORICAL DATA NOT RECEIVED YET FOR ", params[0])

                print("")
                print("HISTORICAL DATA  RECEIVED FOR ", params[0])
                close_data = round(float(asset_hist_data['close'].iloc[-1]), 5)

                if london_list.index(params[0]) <= 4:

                    tp_buy = round(float((close_data - (tp1 * LC_MA * close_data))), round_tp)
                    tp_buy2 = round(float((close_data - (tp2 * LC_MA * close_data))), round_tp)


                elif london_list.index(params[0]) == 5:

                    tp_sell = round(float((close_data + (tp1 * HC_MA * close_data))), round_tp)
                    tp_sell2 = round(float((close_data + (tp2 * HC_MA * close_data))), round_tp)



                elif london_list.index(params[0]) > 5:

                    tp_buy = round(float((close_data - (tp1 * LC_MA * close_data))), round_tp)
                    tp_buy2 = round(float((close_data - (tp2 * LC_MA * close_data))), round_tp)
                    tp_sell = round(float((close_data + (tp1 * HC_MA * close_data))), round_tp)
                    tp_sell2 = round(float((close_data + (tp2 * HC_MA * close_data))), round_tp)

                ######################################changing the tps of standby orders#####################

                exitid = []
                signallist = []

                if london_list.index(params[0]) <= 4:  ##########short model
                    signal, id, price, qty = "BUY", int(ids[1][0]), tp_buy, abs(pos)
                    order_info = create.create_order(acc, "LMT", int(qty), signal, float(price), True, 'GTD',
                                                     exp_date1)
                    tws.placeOrder(id, contract, order_info)
                    exitid.append(id)
                    signallist.append(signal)
                elif london_list.index(params[0]) == 5:  ###########long model
                    signal, id, price, qty = "SELL", int(ids[0][0]), tp_sell, abs(pos)
                    order_info = create.create_order(acc, "LMT", int(qty), signal, float(price), True, 'GTD',
                                                     exp_date1)
                    tws.placeOrder(id, contract, order_info)
                    exitid.append(id)
                    signallist.append(signal)
                elif london_list.index(params[0]) > 5:  ###############LS model
                    ###################placing SELL 0rder#########################
                    signal, id, price = "SELL", int(ids[0][0]), tp_sell

                    if pos_diff < 0:
                        qty = abs(entry_result[3]) + abs(params[6])
                    else:
                        qty = abs(params[6])

                    order_info = create.create_order(acc, "LMT", int(qty), signal, float(price), True, 'GTD',
                                                     exp_date1)
                    tws.placeOrder(id, contract, order_info)

                    exitid.append(id)
                    signallist.append(signal)
                    ##############placing BUY order#############################
                    signal, id, price = "BUY", int(ids[1][0]), tp_buy

                    if pos_diff > 0:
                        qty = abs(entry_result[3]) + abs(params[7])
                    else:
                        qty = abs(params[7])

                    order_info = create.create_order(acc, "LMT", int(qty), signal, float(price), True, 'GTD',
                                                     exp_date1)
                    tws.placeOrder(id, contract, order_info)

                    exitid.append(id)
                    signallist.append(signal)

                print("Updated Standing Order for", params[0])
                log_data = [entry_result[-1], params, tp_sell, tp_buy]

                print("")
                print("%%%%%%%   LOG FOR ", params[0], " %%%%%%%")
                print("ENTRY AND TP1 PARAMS: ", log_data, "TP2 PARAMS:", tp_sell2, tp_buy2)
                print("%%%%%%%%%%%%%%%%%%%%%%%%")
                print("")


                london_df.loc[london_df.CURRENCY == params[0], "BUY_TP1"] = tp_buy
                london_df.loc[london_df.CURRENCY == params[0], "SELL_TP1"] = tp_sell
                london_df.loc[london_df.CURRENCY == params[0], "BUY_TP2"] = tp_buy2
                london_df.loc[london_df.CURRENCY == params[0], "SELL_TP2"] = tp_sell2

                if london_list.index(params[0]) > 5:
                    london_df.loc[london_df.CURRENCY == params[0], "BUY_EXIT_QTY"] = int(params[7])
                    london_df.loc[london_df.CURRENCY == params[0], "SELL_EXIT_QTY"] = int(params[6])

                elif london_list.index(params[0]) == 5:
                    london_df.loc[london_df.CURRENCY == params[0], "SELL_EXIT_QTY"] = int(params[2])

                elif london_list.index(params[0]) <= 4:
                    london_df.loc[london_df.CURRENCY == params[0], "BUY_EXIT_QTY"] = int(params[2])

                london_df.loc[london_df.CURRENCY == params[0], "CLOSE"] = close_data
                london_df.loc[london_df.CURRENCY == params[0], "OPEN_POSITION"] = entry_result[9]
                london_df.loc[london_df.CURRENCY == params[0], "ENTRY_EXE"] = entry_result[0]
                london_df.loc[london_df.CURRENCY == params[0], "CYCLE_DATE"] = entry_result[-1]
                time.sleep(0.05)
                london_df.loc[london_df.CURRENCY == params[0], "ENTRY_QTY"] = entry_result[2]
                london_df.loc[london_df.CURRENCY == params[0], "ENTRY_SIGNAL"] = entry_result[6]
                london_df.loc[london_df.CURRENCY == params[0], "ASK"] = entry_result[7]
                london_df.loc[london_df.CURRENCY == params[0], "BID"] = entry_result[8]
                london_df.loc[london_df.CURRENCY == params[0], "ENTRY_TYPE"] = entry_result[5]
                london_df.loc[london_df.CURRENCY == params[0], "ENTRY_TIME"] = entry_result[4]

            #########################################Regular Market Sleeps##############################################
            if week_day == 4 and currtime[-8:] >= "16:59:50":
                wakeuptime = ((datetime.now() + timedelta(+2)).strftime("%Y%m%d")) + ' 17:15:10'
                sleeptime = (datetime.strptime(wakeuptime, "%Y%m%d %H:%M:%S") - datetime.strptime(currtime,
                                                                                                  "%Y%m%d %H:%M:%S")).total_seconds()
                print(params[0], 'sleeping' + " and wakesup at " + wakeuptime)

                time.sleep(sleeptime)
                # fetchtime = datetime.now().strftime("%Y%m%d %H:%M:%S")
                print(params[0], " activated at ", wakeuptime)

            else:
                if week_day != 4 and "NZD" in params[0]:
                    if "14:59:50" <= currtime[-8:] <= "15:15:05":
                        sleeptime = (
                                datetime.strptime("15:15:10", '%H:%M:%S') - datetime.strptime(currtime[-8:],
                                                                                              '%H:%M:%S')).total_seconds()
                        print(params[0], "sleeping till mkt opens at 15:15")

                        time.sleep(sleeptime)  # sleeptime

                        fetchtime = datetime.now().strftime("%Y%m%d %H:%M:%S")
                        print(params[0], " activated at", fetchtime)

                else:
                    if "16:59:50" <= currtime[-8:] <= "17:15:05":
                        sleeptime = (
                                datetime.strptime("17:15:10", '%H:%M:%S') - datetime.strptime(currtime[-8:],
                                                                                              '%H:%M:%S')).total_seconds()
                        print(params[0], "sleeping till mkt opens at 17:15")

                        time.sleep(sleeptime)
                        fetchtime = datetime.now().strftime("%Y%m%d %H:%M:%S")
                        print(params[0], " activated at", fetchtime)
            ##############################################################################################################
            rt_price = ask_bid(contract, tickerId)
            data_fetch_time = datetime.now().strftime("%Y%m%d %H:%M:%S")

            # print("Pinged IB at",data_fetch_time," ",params[0]," received data")

            while rt_price[0] == -1 or rt_price[1] == -1:
                negative_time = datetime.now().strftime("%H:%M:%S")
                print("!!!!!! VALID DATA NOT OBTAINED FROM IB FOR " + params[0] + negative_time + " !!!!")
                print(datetime.now().strftime("%Y%m%d %H:%M:%S"))
                tickerId = random.randint(5000, 9000)
                rt_price = ask_bid(contract, tickerId)

            if london_list.index(params[0]) <= 4:  #################SHORT Model

                if condition_buy == "NOT FULFILLED":

                    if flag_buy == False:
                        if rt_price[2] <= tp_buy:
                            print(params[0], " 24% BUY MET", "RT PRICE:", rt_price[2], "TP 24:", tp_buy)
                            london_exit_df.loc[london_exit_df.CURRENCY == params[0], "MID"] = 'YES'
                            flag_buy = True
                    # print("WAITING TO CROSS", tp_buy)
                    if flag_buy == True:
                        if rt_price[0] <= tp_buy2:
                            # print(rt_price[0], tp_buy)
                            signal = "BUY"
                            fetchtime = datetime.now().strftime("%Y%m%d %H:%M:%S")
                            print(signal, condition_buy)
                            print("CONDITION MET FOR", params[0], signal, "at", fetchtime, "MID PRICE:", rt_price[2],
                                  "TRYING TO PLACE AT ASK:", rt_price[0])
                            london_exit_df.loc[london_exit_df.CURRENCY == params[0], "ASK"] = 'YES'
                            askflag, bidflag = "YES", "NO"
                            id = int(ids[1][0])
                            price = float(round(rt_price[0], round1))
                            qty = abs(pos)
                            # print("BUY CONDITION MET")
                            condition1 = "MET"
                            pos = qty
                        elif rt_price[0] > tp_buy2 and rt_price[1] >= tp_buy:
                            signal = "BUY"
                            fetchtime = datetime.now().strftime("%Y%m%d %H:%M:%S")
                            print(signal, condition_buy)
                            print("CONDITION MET FOR", params[0], signal, "at", fetchtime, "MID PRICE:", rt_price[2],
                                  "TRYING TO PLACE AT BID:", rt_price[1])
                            london_exit_df.loc[london_exit_df.CURRENCY == params[0], "BID"] = 'YES'
                            askflag, bidflag = "NO", "YES"
                            id = int(ids[1][0])
                            price = float(round(rt_price[1], round1))
                            qty = abs(pos)
                            # print("BUY CONDITION MET")
                            condition1 = "MET"
                            pos = qty

                else:
                    # print("EXIT CRITERIA MET FOR", params[0])
                    condition1 = "MET ALREADY"


            elif london_list.index(params[0]) == 5:
                # print("LONG")
                if condition_sell == "NOT FULFILLED":

                    if flag_sell == False:
                        if rt_price[2] >= tp_sell:
                            print(params[0], " 24% SELL MET", "RT PRICE:", rt_price[2], "TP 24:", tp_sell)
                            london_exit_df.loc[london_exit_df.CURRENCY == params[0], "MID"] = 'YES'
                            flag_sell = True

                    # print("WAITING TO CROSS", tp_sell)
                    if flag_sell == True:
                        if rt_price[1] >= tp_sell2:
                            # print(rt_price[1], tp_sell)
                            signal = "SELL"
                            fetchtime = datetime.now().strftime("%Y%m%d %H:%M:%S")
                            print(signal, condition_sell)
                            print("CONDITION MET FOR", params[0], signal, "at", fetchtime, "MID PRICE:", rt_price[2],
                                  "TRYING TO PLACE AT BID:", rt_price[1])
                            london_exit_df.loc[london_exit_df.CURRENCY == params[0], "BID"] = 'YES'
                            askflag, bidflag = "NO", "YES"
                            id = int(ids[0][0])
                            price = float(round(rt_price[1], round1))
                            qty = abs(pos)
                            # print("SELL CONDITION MET")
                            condition1 = "MET"
                            pos = qty
                        elif rt_price[1] < tp_sell2 and rt_price[0] <= tp_sell:
                            signal = "SELL"
                            fetchtime = datetime.now().strftime("%Y%m%d %H:%M:%S")
                            print(signal, condition_sell)
                            print("CONDITION MET FOR", params[0], signal, "at", fetchtime, "MID PRICE:", rt_price[2],
                                  "TRYING TO PLACE AT ASK:", rt_price[0])
                            london_exit_df.loc[london_exit_df.CURRENCY == params[0], "ASK"] = 'YES'
                            askflag, bidflag = "YES", "NO"
                            id = int(ids[0][0])
                            price = float(round(rt_price[0], round1))
                            qty = abs(pos)
                            # print("SELL CONDITION MET")
                            condition1 = "MET"
                            pos = qty
                else:
                    print("EXIT CRITERIA MET FOR", params[0])
                    condition1 = "MET ALREADY"


            elif london_list.index(params[0]) > 5:
                # print("LONG_SHORT")
                # print("WAITING TO CROSS", tp_buy)
                # print("WAITING TO CROSS", tp_sell)

                ######################BUY Side######################################
                if condition_buy == "NOT FULFILLED":

                    if flag_buy == False:
                        if rt_price[2] <= tp_buy:
                            print(params[0], " 24% BUY MET", "RT PRICE:", rt_price[2], "TP 24:", tp_buy)
                            london_exit_df.loc[(london_exit_df.CURRENCY == params[0]) & (london_exit_df.SIGNAL == "BUY"), "MID"] = 'YES'
                            flag_buy = True

                    if flag_buy == True:
                        if rt_price[0] <= tp_buy2:
                            # print(rt_price[0], tp_buy)
                            signal = "BUY"
                            fetchtime = datetime.now().strftime("%Y%m%d %H:%M:%S")
                            print(signal, condition_buy)
                            print("CONDITION MET FOR", params[0], signal, "at", fetchtime, "MID PRICE:", rt_price[2],
                                  "TRYING TO PLACE AT ASK:", rt_price[0])

                            london_exit_df.loc[(london_exit_df.CURRENCY == params[0]) & (london_exit_df.SIGNAL == "BUY"), "ASK"] = 'YES'
                            askflag, bidflag = "YES", "NO"
                            id = int(ids[1][0])
                            # print("ID IS,", id)
                            price = float(round(rt_price[0], round1))

                            if pos_diff > 0:
                                qty = abs(entry_result[3]) + abs(params[7])
                            else:
                                qty = abs(params[7])

                            print("SENDING", signal, qty)

                            condition1 = "MET"
                            pos = qty

                        elif rt_price[0] > tp_buy2 and rt_price[1] >= tp_buy:

                            signal = "BUY"
                            fetchtime = datetime.now().strftime("%Y%m%d %H:%M:%S")
                            print(signal, condition_buy)
                            print("CONDITION MET FOR", params[0], signal, "at", fetchtime, "MID PRICE:", rt_price[2],
                                  "TRYING TO PLACE AT BID:", rt_price[1])

                            london_exit_df.loc[(london_exit_df.CURRENCY == params[0]) & (london_exit_df.SIGNAL == "BUY"), "BID"] = 'YES'
                            askflag, bidflag = "NO", "YES"
                            print(signal, condition_buy)
                            id = int(ids[1][0])
                            # print("ID IS,", id)
                            price = float(round(rt_price[1], round1))

                            if pos_diff > 0:
                                qty = abs(entry_result[3]) + abs(params[7])
                            else:
                                qty = abs(params[7])

                            print("SENDING", signal, qty)

                            condition1 = "MET"
                            pos = qty

                # else:
                #     # print("EXIT CRITERIA MET FOR", params[0], signal)
                #     condition1 = "MET ALREADY"

                ############################SELL Side
                if condition_sell == "NOT FULFILLED":

                    if flag_sell == False:
                        if rt_price[2] >= tp_sell:
                            print(params[0], " 24% SELL MET", "RT PRICE:", rt_price[2], "TP 24:", tp_sell)
                            london_exit_df.loc[(london_exit_df.CURRENCY == params[0]) & (
                                        london_exit_df.SIGNAL == "SELL"), "MID"] = 'YES'
                            flag_sell = True

                    if flag_sell == True:
                        if rt_price[1] >= tp_sell2:
                            signal = "SELL"
                            fetchtime = datetime.now().strftime("%Y%m%d %H:%M:%S")
                            print(signal, condition_sell)
                            print("CONDITION MET FOR", params[0], signal, "at", fetchtime, "MID PRICE:", rt_price[2],
                                  "TRYING TO PLACE AT BID:", rt_price[1])

                            london_exit_df.loc[(london_exit_df.CURRENCY == params[0]) & (
                                        london_exit_df.SIGNAL == "SELL"), "BID"] = 'YES'
                            askflag, bidflag = "NO", "YES"

                            id = int(ids[0][0])
                            # print("ID IS,", id)
                            # print(rt_price[1], tp_sell)
                            price = float(round(rt_price[1],
                                                round1))  #### CHANGED FROM REAL TIME PRICE TO CALCULATED TP PRICE FOR EXECUTION #### @@ 20% INITIALLY ###
                            if pos_diff < 0:
                                qty = abs(entry_result[3]) + abs(params[6])
                            else:
                                qty = abs(params[6])
                            print("SENDING", signal, qty)
                            condition1 = "MET"
                            pos = qty

                        elif rt_price[1] < tp_sell2 and rt_price[0] <= tp_sell:
                            signal = "SELL"
                            fetchtime = datetime.now().strftime("%Y%m%d %H:%M:%S")
                            print(signal, condition_sell)
                            print("CONDITION MET FOR", params[0], signal, "at", fetchtime, "MID PRICE:", rt_price[2],
                                  "TRYING TO PLACE AT ASK:", rt_price[0])
                            london_exit_df.loc[(london_exit_df.CURRENCY == params[0]) & (
                                        london_exit_df.SIGNAL == "SELL"), "ASK"] = 'YES'
                            askflag, bidflag = "YES", "NO"

                            id = int(ids[0][0])
                            # print("ID IS,", id)
                            # print(rt_price[1], tp_sell)
                            price = float(round(rt_price[0],
                                                round1))  #### CHANGED FROM REAL TIME PRICE TO CALCULATED TP PRICE FOR EXECUTION #### @@ 20% INITIALLY ###
                            if pos_diff < 0:
                                qty = abs(entry_result[3]) + abs(params[6])
                            else:
                                qty = abs(params[6])
                            print("SENDING", signal, qty)
                            condition1 = "MET"
                            pos = qty

                # else:
                #     # print("EXIT CRITERIA MET FOR", params[0], signal)
                #     condition1 = "MET ALREADY"
                #########MAIN CONDITION FOR EXIT EXECUTION###################################################################
            if condition1 == "MET":
                # id = transmit_exit(contract, qty, signal, price)  ### to be removed
                # print("ORDER EXPIRY:", exp_date1)
                print("Sending Order", signal, params[0])
                order_info = create.create_order(acc, "LMT", int(qty), signal, float(price), True, 'GTD', exp_date1)
                tws.placeOrder(id, contract, order_info)
                time.sleep(1)

            for k in range(len(exitid)):
                Id = exitid[k]
                Signal = signallist[k]

                confirm = pd.DataFrame()
                while confirm.empty:
                    confirm_data = list(callback.order_Status)
                    for j in range(0, len(confirm_data)):
                        confirm_data[j] = tuple(confirm_data[j])
                    confirm = pd.DataFrame(confirm_data,
                                           columns=['orderId', 'status', 'filled', 'remaining', 'avgFillPrice'
                                               , 'permId', 'parentId', 'lastFillPrice', 'clientId', 'whyHeld'])
                if confirm[confirm.orderId == Id].empty == False:
                    status = confirm[confirm.orderId == Id].tail(1).iloc[0]['status']
                    remain = confirm[confirm.orderId == Id].tail(1).iloc[0]['remaining']
                    filled = confirm[confirm.orderId == Id].tail(1).iloc[0]['filled']

                    if remain > 0:
                        # print(params[0], "FILLED:", filled, "REMAINING:", remain)

                        partial_fill = int(filled)
                        remain_qty = int(remain)
                        exit_exeprice = 0
                        round1 = 5
                        if Signal == "BUY":  ####  ONLY IF THE 20% CONDITION IS NOT MET, REPLACE IT WITH 18% VALUE ####
                            # tp_buy = tp_buy2   #### tp_buy / tp_sell is 20 and tp_buy2 / tp_sell2 is 18%  ####
                            condition_buy = "NOT FULFILLED"
                            condition1 = "NOT MET"
                        elif Signal == "SELL":
                            # tp_sell = tp_sell2
                            condition_sell = "NOT FULFILLED"
                            condition1 = "NOT MET"
                    elif remain == 0:
                        exit_exeprice = confirm[confirm.orderId == Id].tail(1).iloc[0]['avgFillPrice']
                        exit_exeprice = float(round((exit_exeprice), 5))
                        # print("CONDITION MET,BREAKING ")
                        if Signal == "BUY":
                            condition_buy = "FULFILLED"
                            condition1 = "MET ALREADY"
                            # print(Signal, condition_buy)
                            exetime = datetime.now().strftime("%Y:%m:%d %H:%M:%S")

                            exit_buy_list = [actual_tp_buy, exit_exeprice, filled, Signal, exetime]


                        elif Signal == "SELL":
                            condition_sell = "FULFILLED"
                            condition1 = "MET ALREADY"
                            # print(Signal, condition_sell)
                            exetime = datetime.now().strftime("%Y:%m:%d %H:%M:%S")
                            exit_sell_list = [actual_tp_sell, exit_exeprice, filled, Signal, exetime]

                        list1 = [params[0], entry_result, exit_buy_list, exit_sell_list, close_data]

                        if london_list.index(params[0]) <= 4:

                            london_df.loc[london_df.CURRENCY == params[0], "BUY_EXIT_EXE"] = exit_buy_list[1]
                            london_df.loc[london_df.CURRENCY == params[0], "BUY_EXIT_TIME"] = exit_buy_list[4]

                            london_exit_df.loc[london_exit_df.CURRENCY == params[0], "FILLED"] = 'YES'

                            if (london_exit_df.loc[london_exit_df.CURRENCY == params[0], "BID"].values[0] != 'YES') and \
                                    (london_exit_df.loc[london_exit_df.CURRENCY == params[0], "ASK"].values[0] != 'YES'):

                                london_exit_df.loc[london_exit_df.CURRENCY == params[0], "TP HIT"] = 'YES'
                                london_exit_df.loc[london_exit_df.CURRENCY == params[0], "FILLED@"] = 'TP'

                            else:
                                if askflag == 'YES':
                                    london_exit_df.loc[london_exit_df.CURRENCY == params[0], "FILLED@"] = 'ASK'
                                elif bidflag == 'YES':
                                    london_exit_df.loc[london_exit_df.CURRENCY == params[0], "FILLED@"] = 'BID'

                        elif london_list.index(params[0]) == 5:

                            london_df.loc[london_df.CURRENCY == params[0], "SELL_EXIT_EXE"] = exit_sell_list[1]
                            london_df.loc[london_df.CURRENCY == params[0], "SELL_EXIT_TIME"] = exit_sell_list[4]

                            london_exit_df.loc[london_exit_df.CURRENCY == params[0], "FILLED"] = 'YES'

                            if (london_exit_df.loc[london_exit_df.CURRENCY == params[0], "BID"].values[0] != 'YES') and \
                                    (london_exit_df.loc[london_exit_df.CURRENCY == params[0], "ASK"].values[0] != 'YES'):

                                london_exit_df.loc[london_exit_df.CURRENCY == params[0], "TP HIT"] = 'YES'
                                london_exit_df.loc[london_exit_df.CURRENCY == params[0], "FILLED@"] = 'TP'

                            else:
                                if askflag == 'YES':
                                    london_exit_df.loc[london_exit_df.CURRENCY == params[0], "FILLED@"] = 'ASK'
                                elif bidflag == 'YES':
                                    london_exit_df.loc[london_exit_df.CURRENCY == params[0], "FILLED@"] = 'BID'


                        elif london_list.index(params[0]) > 5:

                            london_df.loc[london_df.CURRENCY == params[0], "BUY_EXIT_EXE"] = exit_buy_list[1]
                            london_df.loc[london_df.CURRENCY == params[0], "BUY_EXIT_TIME"] = exit_buy_list[4]

                            london_df.loc[london_df.CURRENCY == params[0], "SELL_EXIT_EXE"] = exit_sell_list[1]
                            london_df.loc[london_df.CURRENCY == params[0], "SELL_EXIT_TIME"] = exit_sell_list[4]

                            if london_exit_df.loc[(london_exit_df.CURRENCY == params[0]) & (
                                    london_exit_df.SIGNAL == Signal), "FILLED"].values[0] != 'YES':

                                london_exit_df.loc[(london_exit_df.CURRENCY == params[0]) & (
                                        london_exit_df.SIGNAL == Signal), "FILLED"] = 'YES'

                                if (london_exit_df.loc[(london_exit_df.CURRENCY == params[0]) & (
                                        london_exit_df.SIGNAL == Signal), "BID"].values[0] != 'YES') and \
                                        (london_exit_df.loc[(london_exit_df.CURRENCY == params[0]) & (
                                                london_exit_df.SIGNAL == Signal), "ASK"].values[0] != 'YES'):
                                    london_exit_df.loc[(london_exit_df.CURRENCY == params[0]) & (
                                            london_exit_df.SIGNAL == Signal), "TP HIT"] = 'YES'
                                    london_exit_df.loc[(london_exit_df.CURRENCY == params[0]) & (
                                            london_exit_df.SIGNAL == Signal), "FILLED@"] = 'TP'
                                else:
                                    if askflag == 'YES':
                                        london_exit_df.loc[(london_exit_df.CURRENCY == params[0]) & (
                                                london_exit_df.SIGNAL == Signal), "FILLED@"] = 'ASK'
                                    elif bidflag == 'YES':
                                        london_exit_df.loc[(london_exit_df.CURRENCY == params[0]) & (
                                                london_exit_df.SIGNAL == Signal), "FILLED@"] = 'BID'



                        dat = open_pos()
                        if (dat.empty) or (dat[(dat["Local Symbol"] == params[0])].empty) or (
                                int(dat[(dat["Local Symbol"] == params[0])].iloc[-1]['Position']) == 0):
                            # print(pos)
                            pos = 0

            if pos == 0:
                if london_list.index(params[0]) > 5:

                    if london_exit_df.loc[(london_exit_df.CURRENCY == params[0]) & (
                            london_exit_df.SIGNAL == "SELL"), "FILLED"].values[0] != 'YES':

                        london_exit_df.loc[(london_exit_df.CURRENCY == params[0]) & (
                                london_exit_df.SIGNAL == "SELL"), "FILLED"] = 'YES'

                        if (london_exit_df.loc[(london_exit_df.CURRENCY == params[0]) & (
                                london_exit_df.SIGNAL == "SELL"), "BID"].values[0] != 'YES') and \
                                (london_exit_df.loc[(london_exit_df.CURRENCY == params[0]) & (
                                        london_exit_df.SIGNAL == "SELL"), "ASK"].values[0] != 'YES'):
                            london_exit_df.loc[(london_exit_df.CURRENCY == params[0]) & (
                                    london_exit_df.SIGNAL == "SELL"), "TP HIT"] = 'YES'
                    ###########################################################################

                    if london_exit_df.loc[(london_exit_df.CURRENCY == params[0]) & (
                            london_exit_df.SIGNAL == "BUY"), "FILLED"].values[0] != 'YES':
                        london_exit_df.loc[(london_exit_df.CURRENCY == params[0]) & (
                                london_exit_df.SIGNAL == "BUY"), "FILLED"] = 'YES'

                        if (london_exit_df.loc[(london_exit_df.CURRENCY == params[0]) & (
                                london_exit_df.SIGNAL == "BUY"), "BID"].values[0] != 'YES') and \
                                (london_exit_df.loc[(london_exit_df.CURRENCY == params[0]) & (
                                        london_exit_df.SIGNAL == "BUY"), "ASK"].values[0] != 'YES'):
                            london_exit_df.loc[(london_exit_df.CURRENCY == params[0]) & (
                                    london_exit_df.SIGNAL == "BUY"), "TP HIT"] = 'YES'

                print(params[0], "HAS BEEN EXECUTED")
                pos = 0
                print(london_exit_df)
                print(" ")
                print(london_df)
                #print(london_df)


            check_time = datetime.now().strftime("%Y%m%d %H:%M:%S")

            if check_time >= exp_date1:
                exit_buy_list = [actual_tp_buy, "null", "null", "null", "null"]
                exit_sell_list = [actual_tp_sell, "null", "null", "null", "null"]

                break

        london_df = london_df[["CYCLE_DATE", "CURRENCY", "OPEN_POSITION", "ASK", "BID", "CLOSE",
                               "ENTRY_SIGNAL", "ENTRY_QTY", "ENTRY_TYPE", "ENTRY_EXE", "ENTRY_TIME",
                               "BUY_TP1", "BUY_TP2", "BUY_EXIT_QTY", "BUY_EXIT_EXE", "BUY_EXIT_TIME"
            , "SELL_TP1", "SELL_TP2", "SELL_EXIT_QTY", "SELL_EXIT_EXE", "SELL_EXIT_TIME"]]

        path = "C:\REPORTS/" + entry_result[-1] + "_LONDON.csv"
        london_df.to_csv(path, index=False)

        london_exit_df = london_exit_df[["CURRENCY","SIGNAL","MID","ASK","BID","TP HIT","FILLED","FILLED@"]]
        path = "C:\REPORTS/" + entry_result[-1] + "_LONDON_EXIT_REPORT.csv"
        london_exit_df.to_csv(path, index=False)

    return


def NY(dat, params, ids, tickerId, histId, ny_df):
    tp_buy = 0
    tp_buy2 = 0
    tp_sell = 0
    tp_sell2 = 0
    round1 = 4
    round_tp = 5
    exit_exeprice = 0
    condition1 = "NOT MET"
    filled = 0
    remain = 0
    list1 = []

    qty = 0
    price = 0
    entry_result = ["null"] * 12

    df_switch_ny = pd.read_excel('C:/database/FX_DAILY/FOREX_settings.xlsx', 'NY_SWITCH')
    df_gtd = pd.read_excel('C:/database/FX_DAILY/FOREX_settings.xlsx', 'EXIT_GTD')
    df_common_inputs = pd.read_excel('C:/database/FX_DAILY/FOREX_settings.xlsx', 'common_inputs')
    closetime = df_common_inputs["CLOSE TIME NY"].iloc[-1].strftime("%H:%M:%S")
    time_ny = " " + closetime
    closetime = datetime.strptime(closetime, "%H:%M:%S")
    lookback1 = int(df_common_inputs["LOOKBACK NY"].iloc[-1])
    lookback = lookback1
    round1 = 4

    contract = create.create_contract(params[0][0:3], "CASH", "IDEALPRO", params[0][4:7], '', '', '', '')
    tp1 = float(df_switch_ny.loc[df_switch_ny.CURRENCY == params[0], 'TP1'].iloc[-1])
    tp2 = float(df_switch_ny.loc[df_switch_ny.CURRENCY == params[0], 'TP2'].iloc[-1])

    ### temporary inputs###

    # tws.reqIds(1)
    # id = callback.next_ValidId + 5
    entry_result = transmit_entry(dat, params[0], params[2], contract, ids, round1, params[1], params[3], params[4],
                                  params[5], tickerId, closetime)  ###get the execution price
    print(params[0], entry_result)

    # df_entry = pd.read_csv("C:\TP_VALUE_LOG/ENTRIES_LOG.csv")
    #
    # df_entry.loc[df_entry.CURR == params[0], ["PRICE", "QTY", "AT", "FILLED", "REMAIN", "TIME"]] = [entry_result[0],
    #                                                                                                 int(entry_result[
    #                                                                                                         2] +
    #                                                                                                     entry_result[3])
    #     , entry_result[5], entry_result[2], entry_result[3], entry_result[10]]
    #
    # df_entry = df_entry.loc[:, ~df_entry.columns.str.contains('^Unnamed')]
    # df_entry.to_csv("C:\TP_VALUE_LOG/ENTRIES_LOG.csv")

    # time.sleep(1)
    #
    # ###### END OF ENTRY #################################################################################################################
    # currtime = datetime.now().strftime('%Y%m%d %H:%M:%S')
    # if not 'NZD' in params[0]:
    #     if currtime < (entry_result[-1] + ' ' + (closetime + timedelta(seconds=60)).strftime("%H:%M:%S")):
    #         sleeptime = ((closetime + timedelta(seconds=62)) - datetime.strptime(currtime[-8:], '%H:%M:%S')).total_seconds()
    #         print(params[0], "sleeping till" + " " + closetime.strftime("%H:%M:%S"))
    #         time.sleep(sleeptime)
    # ###########################regular mkt close#####################################
    # currtime = datetime.now().strftime('%Y%m%d %H:%M:%S')
    # week_day = datetime.now().weekday()
    # if week_day != 4 and "NZD" in params[0]:
    #     if "14:59:50" <= currtime[-8:] <= "15:15:05":
    #         sleeptime = (
    #                 datetime.strptime("15:15:10", '%H:%M:%S') - datetime.strptime(currtime[-8:],
    #                                                                               '%H:%M:%S')).total_seconds()
    #         print(params[0], "sleeping till mkt opens at 15:15")
    #         time.sleep(sleeptime)  # sleeptime
    #
    # else:
    #     if "16:59:50" <= currtime[-8:] <= "17:15:05":
    #         sleeptime = (
    #                 datetime.strptime("17:15:10", '%H:%M:%S') - datetime.strptime(currtime[-8:],
    #                                                                               '%H:%M:%S')).total_seconds()
    #         print(params[0], "sleeping till mkt opens at 17:15")
    #         time.sleep(sleeptime)  # sleeptime
    #
    # ######################################################################################################################################
    data_size = 0
    while data_size < 22:
        asset_hist_data = pd.DataFrame()
        asset_hist_data = historical(time_ny, contract, "26 D", histId)
        data_size = asset_hist_data.shape[0]
        print("HISTORICAL DATA NOT RECEIVED YET FOR ", params[0])

        # asset_hist_data = asset_hist_data[:-1]
    close_data = asset_hist_data["close"].iloc[-1]
    close_data = round(float(close_data), 5)
    print("")
    print("HISTORICAL DATA  RECEIVED FOR ", params[0])

    ################ MOVING AVERAGES ###########################################################
    asset_hist_data["LOW CLOSE"] = (pd.Series.rolling(
        (asset_hist_data['low'].shift(-1) / asset_hist_data['close'] - 1).shift(1),
        window=lookback).mean()).dropna()
    asset_hist_data["HIGH CLOSE"] = ((pd.Series.rolling(
        (asset_hist_data['high'].shift(-1) / asset_hist_data['close'] - 1).shift(1),
        window=lookback).mean()).dropna())

    path = "C:\TP_VALUE_LOG/movingAVG_" + params[0] + ".csv"
    asset_hist_data.tail(23).to_csv(path)

    if ny_list.index(params[0]) <= 3:
        if entry_result[0] > 0:
            tp_buy = round(float(((entry_result[0] - (tp1 * abs((pd.Series.rolling(
                (asset_hist_data['low'].shift(-1) / asset_hist_data['close'] - 1).shift(1),
                window=lookback).mean()).dropna()) * entry_result[0])).dropna()).iloc[-1]), round_tp)

            tp_buy2 = round(float(((entry_result[0] - (tp2 * abs((pd.Series.rolling(
                (asset_hist_data['low'].shift(-1) / asset_hist_data['close'] - 1).shift(1),
                window=lookback).mean()).dropna()) * entry_result[0])).dropna()).iloc[-1]), round_tp)
        else:
            tp_buy = round(float(((asset_hist_data['close'] - (tp1 * abs((pd.Series.rolling(
                (asset_hist_data['low'].shift(-1) / asset_hist_data['close'] - 1).shift(1),
                window=lookback).mean()).dropna()) * asset_hist_data['close'])).dropna()).iloc[-1]),
                           round_tp)

            tp_buy2 = round(float(((asset_hist_data['close'] - (tp2 * abs((pd.Series.rolling(
                (asset_hist_data['low'].shift(-1) / asset_hist_data['close'] - 1).shift(1),
                window=lookback).mean()).dropna()) * asset_hist_data['close'])).dropna()).iloc[-1]),
                            round_tp)
        asset_hist_data["TP_BUY"] = "null"
        asset_hist_data.iloc[-1, asset_hist_data.columns.get_loc('TP_BUY')] = tp_buy


    elif 3 < ny_list.index(params[0]) < 7:
        if entry_result[0] > 0:
            tp_sell = round(float(((entry_result[0] + (tp1 * abs((pd.Series.rolling(
                (asset_hist_data['high'].shift(-1) / asset_hist_data['close'] - 1).shift(1),
                window=lookback).mean()).dropna()) * entry_result[0])).dropna()).iloc[-1]), round_tp)

            tp_sell2 = round(float(((entry_result[0] + (tp2 * abs((pd.Series.rolling(
                (asset_hist_data['high'].shift(-1) / asset_hist_data['close'] - 1).shift(1),
                window=lookback).mean()).dropna()) * entry_result[0])).dropna()).iloc[-1]), round_tp)
        else:
            tp_sell = round(float(((asset_hist_data['close'] + (tp1 * abs((pd.Series.rolling(
                (asset_hist_data['high'].shift(-1) / asset_hist_data['close'] - 1).shift(1),
                window=lookback).mean()).dropna()) * asset_hist_data['close'])).dropna()).iloc[-1]), round_tp)

            tp_sell2 = round(float(((asset_hist_data['close'] + (tp2 * abs((pd.Series.rolling(
                (asset_hist_data['high'].shift(-1) / asset_hist_data['close'] - 1).shift(1),
                window=lookback).mean()).dropna()) * asset_hist_data['close'])).dropna()).iloc[-1]), round_tp)

        asset_hist_data["TP_SELL"] = "null"
        asset_hist_data.iloc[-1, asset_hist_data.columns.get_loc('TP_SELL')] = tp_sell


    elif ny_list.index(params[0]) >= 7:
        if entry_result[0] > 0:
            tp_buy = round(float(((entry_result[0] - (tp1 * abs((pd.Series.rolling(
                (asset_hist_data['low'].shift(-1) / asset_hist_data['close'] - 1).shift(1),
                window=lookback).mean()).dropna()) * entry_result[0])).dropna()).iloc[-1]), round_tp)

            tp_sell = round(float(((entry_result[0] + (tp1 * abs((pd.Series.rolling(
                (asset_hist_data['high'].shift(-1) / asset_hist_data['close'] - 1).shift(1),
                window=lookback).mean()).dropna()) * entry_result[0])).dropna()).iloc[-1]), round_tp)

            tp_buy2 = round(float(((entry_result[0] - (tp2 * abs((pd.Series.rolling(
                (asset_hist_data['low'].shift(-1) / asset_hist_data['close'] - 1).shift(1),
                window=lookback).mean()).dropna()) * entry_result[0])).dropna()).iloc[-1]), round_tp)

            tp_sell2 = round(float(((entry_result[0] + (tp2 * abs((pd.Series.rolling(
                (asset_hist_data['high'].shift(-1) / asset_hist_data['close'] - 1).shift(1),
                window=lookback).mean()).dropna()) * entry_result[0])).dropna()).iloc[-1]), round_tp)
        else:
            tp_buy = round(float(((asset_hist_data['close'] - (tp1 * abs((pd.Series.rolling(
                (asset_hist_data['low'].shift(-1) / asset_hist_data['close'] - 1).shift(1),
                window=lookback).mean()).dropna()) * asset_hist_data['close'])).dropna()).iloc[-1]), round_tp)

            tp_sell = round(float(((asset_hist_data['close'] + (tp1 * abs((pd.Series.rolling(
                (asset_hist_data['high'].shift(-1) / asset_hist_data['close'] - 1).shift(1),
                window=lookback).mean()).dropna()) * asset_hist_data['close'])).dropna()).iloc[-1]), round_tp)

            tp_buy2 = round(float(((asset_hist_data['close'] - (tp2 * abs((pd.Series.rolling(
                (asset_hist_data['low'].shift(-1) / asset_hist_data['close'] - 1).shift(1),
                window=lookback).mean()).dropna()) * asset_hist_data['close'])).dropna()).iloc[-1]), round_tp)

            tp_sell2 = round(float(((asset_hist_data['close'] + (tp2 * abs((pd.Series.rolling(
                (asset_hist_data['high'].shift(-1) / asset_hist_data['close'] - 1).shift(1),
                window=lookback).mean()).dropna()) * asset_hist_data['close'])).dropna()).iloc[-1]), round_tp)

        asset_hist_data["TP_BUY"] = "null"
        asset_hist_data.iloc[-1, asset_hist_data.columns.get_loc('TP_BUY')] = tp_buy
        asset_hist_data["TP_SELL"] = "null"
        asset_hist_data.iloc[-1, asset_hist_data.columns.get_loc('TP_SELL')] = tp_sell
        ### writes historical data to local

    path = "C:\TP_VALUE_LOG/" + params[0] + str(datetime.now().strftime("%Y%m%d%H%M%S")) + ".csv"
    asset_hist_data.tail(23).to_csv(path)
    # print("@@@@@@@@@@  TP FOR", params[0], "IS","SELL TP:",tp_sell,"BUY TP", tp_buy, " @@@@@@@@@@@@@")
    log_data = [entry_result[-1], params, tp_sell, tp_buy]
    print("")
    print("%%%%%%%   LOG FOR ", params[0], " %%%%%%%")
    print("ENTRY AND TP PARAMS: ", log_data, "TP2 PARAMS: ", tp_sell2, tp_buy2)
    print("%%%%%%%%%%%%%%%%%%%%%%%%")
    print("")

    ny_df.loc[ny_df.CURRENCY == params[0], "OPEN_POSITION"] = entry_result[9]
    ny_df.loc[ny_df.CURRENCY == params[0], "ENTRY_EXE"] = entry_result[0]
    ny_df.loc[ny_df.CURRENCY == params[0], "CLOSE"] = close_data
    ny_df.loc[ny_df.CURRENCY == params[0], "CYCLE_DATE"] = entry_result[-1]
    ny_df.loc[ny_df.CURRENCY == params[0], "BUY_TP1"] = tp_buy
    ny_df.loc[ny_df.CURRENCY == params[0], "SELL_TP1"] = tp_sell
    ny_df.loc[ny_df.CURRENCY == params[0], "BUY_TP2"] = tp_buy2
    ny_df.loc[ny_df.CURRENCY == params[0], "SELL_TP2"] = tp_sell2
    ny_df.loc[ny_df.CURRENCY == params[0], "ENTRY_QTY"] = entry_result[2]
    ny_df.loc[ny_df.CURRENCY == params[0], "ENTRY_SIGNAL"] = entry_result[6]
    ny_df.loc[ny_df.CURRENCY == params[0], "ASK"] = entry_result[7]
    ny_df.loc[ny_df.CURRENCY == params[0], "BID"] = entry_result[8]
    ny_df.loc[ny_df.CURRENCY == params[0], "ENTRY_TYPE"] = entry_result[5]
    ny_df.loc[ny_df.CURRENCY == params[0], "ENTRY_TIME"] = entry_result[4]

    if ny_list.index(params[0]) >= 7:
        ny_df.loc[ny_df.CURRENCY == params[0], "BUY_EXIT_QTY"] = int(params[7])
        ny_df.loc[ny_df.CURRENCY == params[0], "SELL_EXIT_QTY"] = int(params[6])

    elif 3 < ny_list.index(params[0]) < 7:
        ny_df.loc[ny_df.CURRENCY == params[0], "SELL_EXIT_QTY"] = int(params[2])

    elif ny_list.index(params[0]) <= 3:
        ny_df.loc[ny_df.CURRENCY == params[0], "BUY_EXIT_QTY"] = int(params[2])
    # with open('C:\TP_VALUE_LOG/ny_log.txt', "a") as f:
    #     f.write("   ||   "+ str(log_data))
    # time.sleep(5)

    #### write into csv the log values , to be used in case of sys/tws shut down
    # ny_log = pd.read_csv("C:/LOG/log_ny.csv")
    # time.sleep(1)
    # if ny_list.index(params[0]) <= 3:
    #     ny_log.loc[
    #         ny_log.CURRENCY == params[0], ["TP_BUY", "TP_BUY2", "TP_SELL", "TP_SELL2", "BUY_QTY", "SELL_QTY",
    #                                        "ENTRY_FILLED", "ENTRY_REMAIN"]] = float(tp_buy), float(tp_buy2), float(
    #         tp_sell), float(tp_sell2), int(params[2]), "0", int(entry_result[2]), int(entry_result[3])
    #
    # elif 3 < ny_list.index(params[0]) < 7:
    #     ny_log.loc[
    #         ny_log.CURRENCY == params[0], ["TP_BUY", "TP_BUY2", "TP_SELL", "TP_SELL2", "BUY_QTY", "SELL_QTY",
    #                                        "ENTRY_FILLED", "ENTRY_REMAIN"]] = float(tp_buy), float(tp_buy2), float(
    #         tp_sell), float(tp_sell2), "0", int(params[2]), int(entry_result[2]), int(entry_result[3])
    #
    # elif ny_list.index(params[0]) >= 7:
    #     ny_log.loc[
    #         ny_log.CURRENCY == params[0], ["TP_BUY", "TP_BUY2", "TP_SELL", "TP_SELL2", "BUY_QTY", "SELL_QTY",
    #                                        "ENTRY_FILLED", "ENTRY_REMAIN"]] = float(tp_buy), float(tp_buy2), float(
    #         tp_sell), float(tp_sell2), int(params[7]), int(params[6]), int(entry_result[2]), int(entry_result[3])
    #
    # ny_log.to_csv("C:/LOG/log_ny.csv")
    ##################

    dat = open_pos()
    #########################################################################################

    ############################################################################################

    exit_buy_list = [tp_buy, "null", "null", "null", "null"]
    exit_sell_list = [tp_sell, "null", "null", "null", "null"]
    if (dat.empty) or (dat[(dat["Local Symbol"] == params[0])].empty) or (
            int(dat[(dat["Local Symbol"] == params[0])].iloc[-1]['Position']) == 0):
        pos = 0
        # print("NO ENTRY FOR", params[0])

        condition_buy = "NOT FULFILLED"
        condition_sell = "NOT FULFILLED"
    else:
        if ny_list.index(params[0]) >= 7:
            pos_diff = int(dat[(dat["Local Symbol"] == params[0])].iloc[-1]['Position'])
        print("CHECKING FOR EXIT")

        exp_time = str(df_common_inputs["GTD NY"].iloc[-1])

        ########################################GTD EXPDATE########################################################################
        week_day = datetime.now().weekday()

        if (week_day == 4):

            num = int(df_gtd.loc[df_gtd.MKT == 'NY', "FRIDAY"].iloc[-1])
            exp_date1 = datetime.now() + timedelta(days=num)  ##changed for christmas&new year holiday
            exp_date1 = exp_date1.strftime('%Y%m%d') + " " + exp_time



        elif week_day == 5:
            num = int(df_gtd.loc[df_gtd.MKT == 'NY', "FRIDAY"].iloc[-1]) - 1
            exp_date1 = datetime.now() + timedelta(days=num)  ##changed for christmas&new year holiday
            exp_date1 = exp_date1.strftime('%Y%m%d') + " " + exp_time

        else:
            ###### consider all possible days from excel ###
            if week_day == 0:
                num = int(df_gtd.loc[df_gtd.MKT == 'NY', "MONDAY"].iloc[-1])
            elif week_day == 1:
                num = int(df_gtd.loc[df_gtd.MKT == 'NY', "TUESDAY"].iloc[-1])
            elif week_day == 2:
                num = int(df_gtd.loc[df_gtd.MKT == 'NY', "WEDNESDAY"].iloc[-1])
            elif week_day == 3:
                num = int(df_gtd.loc[df_gtd.MKT == 'NY', "THURSDAY"].iloc[-1])

            exp_date1 = datetime.now() + timedelta(days=num)
            exp_date1 = exp_date1.strftime('%Y%m%d') + " " + exp_time

        ###############################################################################

        pos = int(dat[(dat["Local Symbol"] == params[0])].iloc[-1]['Position'])
        round1 = 4
        condition_buy = "NOT FULFILLED"
        condition_sell = "NOT FULFILLED"
        # exit_sell_list = []
        # exit_buy_list = []
        actual_tp_buy = tp_buy
        actual_tp_sell = tp_sell

        flag_buy = False
        flag_sell = False

        # ny_df.loc[ny_df.CURRENCY == params[0], "OPEN_POSITION"] = entry_result[9]
        # ny_df.loc[ny_df.CURRENCY == params[0], "ENTRY_EXE"] = entry_result[0]
        # ny_df.loc[ny_df.CURRENCY == params[0], "CLOSE"] = close_data
        # ny_df.loc[ny_df.CURRENCY == params[0], "CYCLE_DATE"] = entry_result[-1]
        # ny_df.loc[ny_df.CURRENCY == params[0], "LONG_TP"] = tp_buy
        # ny_df.loc[ny_df.CURRENCY == params[0], "SHORT_TP"] = tp_sell

        # ny_df = ny_df[["CYCLE_DATE", "CURRENCY", "OPEN_POSITION", "ASK", "BID", "CLOSE",
        #                "ENTRY_SIGNAL", "ENTRY_QTY", "ENTRY_TYPE", "ENTRY_EXE", "ENTRY_TIME",
        #                "LONG_TP", "LONG_EXIT_QTY", "LONG_EXIT_EXE", "LONG_EXIT_TIME",
        #                "SHORT_TP", "SHORT_EXIT_QTY", "SHORT_EXIT_EXE", "SHORT_EXIT_TIME"]]
        #
        # path = "C:\REPORTS/" + entry_result[-1] + "_NY.xlsx"
        # writer = pd.ExcelWriter(path)
        # ny_df.to_excel(writer, 'NY')
        # writer.save()

        while pos != 0:
            conn()

            ###############sleep for regular mkt close############
            currtime = datetime.now().strftime("%Y%m%d %H:%M:%S")
            week_day = datetime.now().weekday()

            if week_day == 4 and currtime[-8:] >= "16:59:50":
                wakeuptime = ((datetime.now() + timedelta(+2)).strftime("%Y%m%d")) + ' 17:15:10'
                sleeptime = (datetime.strptime(wakeuptime, "%Y%m%d %H:%M:%S") - datetime.strptime(currtime,
                                                                                                  "%Y%m%d %H:%M:%S")).total_seconds()
                print(params[0], 'sleeping' + "and wakesup at " + wakeuptime)

                time.sleep(sleeptime)
                # fetchtime = datetime.now().strftime("%Y%m%d %H:%M:%S")
                print(params[0], " activated at ", wakeuptime)
            else:
                if week_day != 4 and "NZD" in params[0]:
                    if "14:59:50" <= currtime[-8:] <= "15:15:05":
                        sleeptime = (
                                datetime.strptime("15:15:10", '%H:%M:%S') - datetime.strptime(currtime[-8:],
                                                                                              '%H:%M:%S')).total_seconds()
                        print(params[0], "sleeping till mkt opens at 15:15")
                        time.sleep(sleeptime)
                        fetchtime = datetime.now().strftime("%Y%m%d %H:%M:%S")
                        print(params[0], " activated at", fetchtime)




                else:
                    if "16:59:50" <= currtime[-8:] <= "17:15:05":
                        sleeptime = (
                                datetime.strptime("17:15:10", '%H:%M:%S') - datetime.strptime(currtime[-8:],
                                                                                              '%H:%M:%S')).total_seconds()
                        print(params[0], "sleeping till mkt opens at 17:15")

                        time.sleep(sleeptime)
                        fetchtime = datetime.now().strftime("%Y%m%d %H:%M:%S")
                        print(params[0], " activated at", fetchtime)

            # df_mkt_fetch = pd.read_excel('C:/database/FX_DAILY/FOREX_settings.xlsx', 'CONTROL_EXIT_TRADES')
            # switch = df_mkt_fetch.loc[df_mkt_fetch.CURRENCY == params[0], "EXIT_TRADE_SWITCH"].iloc[-1]
            # while switch != "yes":
            #     time.sleep(1)
            #     df_mkt_fetch = pd.read_excel('C:/database/FX_DAILY/FOREX_settings.xlsx', 'CONTROL_EXIT_TRADES')
            #     switch = df_mkt_fetch.loc[df_mkt_fetch.CURRENCY == params[0], "EXIT_TRADE_SWITCH"].iloc[-1]
            #     print("YOU HAVE STOPPED THE EXIT SWITCH FOR ", params[0])

            rt_price = ask_bid(contract, tickerId)
            while rt_price[0] == -1 or rt_price[1] == -1:
                negative_time = datetime.now().strftime("%H:%M:%S")
                print("!!!!!! VALID DATA NOT OBTAINED FROM IB FOR " + params[0] + negative_time + " !!!!")
                print(datetime.now().strftime("%Y%m%d %H:%M:%S"))
                tickerId = random.randint(9000, 10000)
                rt_price = ask_bid(contract, tickerId)

            if ny_list.index(params[0]) <= 3:
                # print("SHORT")
                if condition_buy == "NOT FULFILLED":

                    if flag_buy == False:
                        if rt_price[2] <= tp_buy:
                            print(params[0], " 24% BUY MET", "RT PRICE:", rt_price[2], "TP 24:", tp_buy)
                            flag_buy = True

                    # print("WAITING TO CROSS", tp_buy)
                    if flag_buy == True and rt_price[0] <= tp_buy2:
                        # print(rt_price[0], tp_buy)
                        signal = "BUY"
                        fetchtime = datetime.now().strftime("%Y%m%d %H:%M:%S")
                        print("CONDITION MET FOR", params[0], signal, "at", fetchtime, "MID PRICE:", rt_price[2],
                              "TRYING TO PLACE AT:", rt_price[0])
                        id = int(ids[1][0])
                        price = float(round(rt_price[0], round1))
                        qty = abs(pos)
                        # print("BUY CONDITION MET")
                        condition1 = "MET"
                        pos = qty
                else:
                    # print("EXIT CRITERIA MET")
                    condition1 = "MET ALREADY"
            elif 3 < ny_list.index(params[0]) < 7:
                # print("LONG")
                if condition_sell == "NOT FULFILLED":
                    # print("WAITING TO CROSS", tp_sell)

                    if flag_sell == False:
                        if rt_price[2] >= tp_sell:
                            print(params[0], " 24% SELL MET", "RT PRICE:", rt_price[2], "TP 24:", tp_sell)
                            flag_sell = True

                    if flag_sell == True and rt_price[1] >= tp_sell2:
                        # print(rt_price[1], tp_sell)
                        signal = "SELL"
                        fetchtime = datetime.now().strftime("%Y%m%d %H:%M:%S")
                        print("CONDITION MET FOR", params[0], signal, "at", fetchtime, "MID PRICE:", rt_price[2],
                              "TRYING TO PLACE AT:", rt_price[1])
                        id = int(ids[0][0])
                        price = float(round(rt_price[1], round1))
                        qty = abs(pos)
                        # print("SELL CONDITION MET")
                        condition1 = "MET"
                        pos = qty
                else:
                    # print("EXIT CRITERIA MET")
                    condition1 = "MET ALREADY"

            elif ny_list.index(params[0]) >= 7:

                # print("LONG_SHORT")
                # print("WAITING TO CROSS", tp_buy)
                # print("WAITING TO CROSS", tp_sell)

                if flag_buy == False:
                    if rt_price[2] <= tp_buy:
                        print(params[0], " 24% BUY MET", "RT PRICE:", rt_price[2], "TP 24:", tp_buy)
                        flag_buy = True

                if flag_sell == False:
                    if rt_price[2] >= tp_sell:
                        print(params[0], " 24% SELL MET", "RT PRICE:", rt_price[2], "TP 24:", tp_sell)
                        flag_sell = True

                if flag_buy == True and rt_price[0] <= tp_buy2:
                    if condition_buy == "NOT FULFILLED":
                        # print(rt_price[0], tp_buy)
                        signal = "BUY"
                        fetchtime = datetime.now().strftime("%Y%m%d %H:%M:%S")
                        print("CONDITION MET FOR", params[0], signal, "at", fetchtime, "MID PRICE:", rt_price[2],
                              "TRYING TO PLACE AT:", rt_price[0])
                        id = int(ids[1][0])
                        # print("ID IS,", id)
                        price = float(round(rt_price[0], round1))

                        if pos_diff > 0:
                            qty = abs(entry_result[3]) + abs(params[7])
                        else:
                            qty = abs(params[7])
                        condition1 = "MET"
                        pos = qty
                    else:
                        # print("BUY CRITERIA MET")
                        condition1 = "MET ALREADY"


                elif flag_sell == True and rt_price[1] >= tp_sell2:
                    if condition_sell == "NOT FULFILLED":
                        signal = "SELL"
                        fetchtime = datetime.now().strftime("%Y%m%d %H:%M:%S")
                        print("CONDITION MET FOR", params[0], signal, "at", fetchtime, "MID PRICE:", rt_price[2],
                              "TRYING TO PLACE AT:", rt_price[1])
                        id = int(ids[0][0])
                        # print("ID IS,", id)
                        # print(rt_price[1], tp_sell)
                        price = float(round(rt_price[1], round1))

                        if pos_diff < 0:
                            qty = abs(entry_result[3]) + abs(params[6])
                        else:
                            qty = abs(params[6])
                        condition1 = "MET"
                        pos = qty
                    else:
                        # print("SELL CRITERIA MET")
                        condition1 = "MET ALREADY"
                    #########MAIN CONDITION FOR EXIT EXECUTION###################################################################
            if condition1 == "MET":
                # id = transmit_exit(contract, qty, signal, price)  ### to be removed
                # print("ORDER EXPIRY:", exp_date1)
                order_info = create.create_order(acc, "LMT", int(qty), signal, float(price), True, 'GTD', exp_date1)
                tws.placeOrder(id, contract, order_info)
                time.sleep(1)
                confirm = pd.DataFrame()
                while confirm.empty:
                    confirm_data = list(callback.order_Status)
                    for j in range(0, len(confirm_data)):
                        confirm_data[j] = tuple(confirm_data[j])
                    confirm = pd.DataFrame(confirm_data,
                                           columns=['orderId', 'status', 'filled', 'remaining', 'avgFillPrice'
                                               , 'permId', 'parentId', 'lastFillPrice', 'clientId', 'whyHeld'])
                if confirm[confirm.orderId == id].empty == False:
                    status = confirm[confirm.orderId == id].tail(1).iloc[0]['status']
                    remain = confirm[confirm.orderId == id].tail(1).iloc[0]['remaining']
                    filled = confirm[confirm.orderId == id].tail(1).iloc[0]['filled']
                    if remain > 0:

                        # if status == 'Cancelled':
                        #     if signal == "SELL":
                        #         if len(ids[0]) == 1:
                        #             print('do nothing')
                        #         else:
                        #             ids[0].remove(id)
                        #     elif signal == "BUY":
                        #         if len(ids[1]) == 1:
                        #             print('do nothing')
                        #         else:
                        #             ids[1].remove(id)

                        partial_fill = int(filled)
                        remain_qty = int(remain)
                        exit_exeprice = 0
                        round1 = 5
                        if signal == "BUY":
                            # tp_buy = tp_buy2
                            condition_buy = "NOT FULFILLED"
                        elif signal == "SELL":
                            # tp_sell = tp_sell2
                            condition_sell = "NOT FULFILLED"
                    elif remain == 0:
                        exit_exeprice = confirm[confirm.orderId == id].tail(1).iloc[0]['avgFillPrice']
                        exit_exeprice = float(round((exit_exeprice), 4))
                        # print("CONDITION MET,BREAKING ")
                        if signal == "BUY":
                            condition_buy = "FULFILLED"
                            exetime = datetime.now().strftime("%Y:%m:%d %H:%M:%S")

                            exit_buy_list = [actual_tp_buy, exit_exeprice, filled, signal, exetime]
                        elif signal == "SELL":
                            condition_sell = "FULFILLED"
                            exetime = datetime.now().strftime("%Y:%m:%d %H:%M:%S")
                            exit_sell_list = [actual_tp_sell, exit_exeprice, filled, signal, exetime]
                        # print(condition_buy, condition_sell)

                        list1 = [params[0], entry_result, exit_buy_list, exit_sell_list, close_data]
                        # print(list1)
                        # print("END OF TRADE")

                        # # ny_df.loc[ny_df.CURRENCY == params[0], "OPEN_POSITION"] = list1[1][9]
                        # ny_df.loc[ny_df.CURRENCY == params[0], "ENTRY_QTY"] = entry_result[2]
                        # ny_df.loc[ny_df.CURRENCY == params[0], "ENTRY_SIGNAL"] = entry_result[6]
                        # ny_df.loc[ny_df.CURRENCY == params[0], "ASK"] = entry_result[7]
                        # ny_df.loc[ny_df.CURRENCY == params[0], "BID"] = entry_result[8]
                        # # ny_df.loc[ny_df.CURRENCY == params[0], "ENTRY_EXE"] = list1[1][0]
                        # ny_df.loc[ny_df.CURRENCY == params[0], "ENTRY_TYPE"] = entry_result[5]
                        # ny_df.loc[ny_df.CURRENCY == params[0], "ENTRY_TIME"] = entry_result[4]
                        # # ny_df.loc[ny_df.CURRENCY == params[0], "CLOSE"] = list1[-1]
                        # # ny_df.loc[ny_df.CURRENCY == params[0], "CYCLE_DATE"] = list1[1][-1]

                        if ny_list.index(params[0]) <= 3:
                            # ny_df.loc[ny_df.CURRENCY == params[0], "LONG_EXIT_QTY"] = exit_buy_list[2]
                            # ny_df.loc[ny_df.CURRENCY == params[0], "LONG_TP"] = exit_buy_list[0]
                            ny_df.loc[ny_df.CURRENCY == params[0], "BUY_EXIT_EXE"] = exit_buy_list[1]
                            ny_df.loc[ny_df.CURRENCY == params[0], "BUY_EXIT_TIME"] = exit_buy_list[4]
                            # if condition_buy == 'FULFILLED':
                            #     ny_df.loc[ny_df.CURRENCY == params[0], "ACT_RETURN"] = round(
                            #         (exit_buy_list[1] / entry_result[0] - 1) * (-1) * 100, 2)

                        elif 3 < ny_list.index(params[0]) < 7:
                            # ny_df.loc[ny_df.CURRENCY == params[0], "SHORT_EXIT_QTY"] = exit_sell_list[2]
                            # ny_df.loc[ny_df.CURRENCY == params[0], "SHORT_TP"] = exit_sell_list[0]
                            ny_df.loc[ny_df.CURRENCY == params[0], "SELL_EXIT_EXE"] = exit_sell_list[1]
                            ny_df.loc[ny_df.CURRENCY == params[0], "SELL_EXIT_TIME"] = exit_sell_list[4]
                            # if condition_sell == 'FULFILLED':
                            #     ny_df.loc[ny_df.CURRENCY == params[0], "ACT_RETURN"] = round(
                            #         (exit_sell_list[1] / entry_result[0] - 1) * 100, 2)

                        elif ny_list.index(params[0]) >= 7:
                            # ny_df.loc[ny_df.CURRENCY == params[0], "LONG_EXIT_QTY"] = exit_buy_list[2]
                            # ny_df.loc[ny_df.CURRENCY == params[0], "LONG_TP"] = exit_buy_list[0]
                            ny_df.loc[ny_df.CURRENCY == params[0], "BUY_EXIT_EXE"] = exit_buy_list[1]
                            ny_df.loc[ny_df.CURRENCY == params[0], "BUY_EXIT_TIME"] = exit_buy_list[4]
                            # ny_df.loc[ny_df.CURRENCY == params[0], "SHORT_EXIT_QTY"] = exit_sell_list[2]
                            # ny_df.loc[ny_df.CURRENCY == params[0], "SHORT_TP"] = exit_sell_list[0]
                            ny_df.loc[ny_df.CURRENCY == params[0], "SELL_EXIT_EXE"] = exit_sell_list[1]
                            ny_df.loc[ny_df.CURRENCY == params[0], "SELL_EXIT_TIME"] = exit_sell_list[4]
                            # if condition_buy == 'FULFILLED' and condition_sell == 'FULFILLED':
                            #     ny_df.loc[ny_df.CURRENCY == params[0], "ACT_RETURN"] = round((
                            #         (exit_sell_list[1] / entry_result[0] - 1) + ((exit_buy_list[1] / entry_result[0] - 1)*(-1)))*100,2)

                        ny_df = ny_df[["CYCLE_DATE", "CURRENCY", "OPEN_POSITION", "ASK", "BID", "CLOSE",
                                       "ENTRY_SIGNAL", "ENTRY_QTY", "ENTRY_TYPE", "ENTRY_EXE", "ENTRY_TIME",
                                       "BUY_TP1", "BUY_TP2", "BUY_EXIT_QTY", "BUY_EXIT_EXE", "BUY_EXIT_TIME"
                            , "SELL_TP1", "SELL_TP2", "SELL_EXIT_QTY", "SELL_EXIT_EXE", "SELL_EXIT_TIME"]]

                        path = "C:\REPORTS/" + entry_result[-1] + "_NY.xlsx"
                        writer = pd.ExcelWriter(path)
                        ny_df.to_excel(writer, 'NY')
                        writer.save()

                        dat = open_pos()
                        if (dat.empty) or (dat[(dat["Local Symbol"] == params[0])].empty) or (
                                int(dat[(dat["Local Symbol"] == params[0])].iloc[-1]['Position']) == 0):
                            # print(pos)
                            pos = 0

            # if pos != 0:
            #     print(params[0], "STILL WAITING TO HIT TP")
            if pos == 0:
                print(params[0], "HAS BEEN EXECUTED")
                print(ny_df)

            ##here
            check_time = datetime.now().strftime("%Y%m%d %H:%M:%S")
            if check_time >= exp_date1:
                exit_buy_list.extend([actual_tp_buy, "null", "null", "null", "null"])
                exit_sell_list.extend([actual_tp_sell, "null", "null", "null", "null"])
                break

    # list1 = [params[0], entry_result, exit_buy_list, exit_sell_list, close_data]
    # print(list1)
    #
    # ny_df.loc[ny_df.CURRENCY == params[0], "OPEN_POSITION"] = list1[1][9]
    # ny_df.loc[ny_df.CURRENCY == params[0], "ENTRY_QTY"] = list1[1][2]
    # ny_df.loc[ny_df.CURRENCY == params[0], "ENTRY_SIGNAL"] = list1[1][6]
    # ny_df.loc[ny_df.CURRENCY == params[0], "ASK"] = list1[1][7]
    # ny_df.loc[ny_df.CURRENCY == params[0], "BID"] = list1[1][8]
    # ny_df.loc[ny_df.CURRENCY == params[0], "ENTRY_EXE"] = list1[1][0]
    # ny_df.loc[ny_df.CURRENCY == params[0], "ENTRY_TYPE"] = list1[1][5]
    # ny_df.loc[ny_df.CURRENCY == params[0], "ENTRY_TIME"] = list1[1][4]
    # ny_df.loc[ny_df.CURRENCY == params[0], "CLOSE"] = list1[-1]
    # ny_df.loc[ny_df.CURRENCY == params[0], "CYCLE_DATE"] = list1[1][-1]
    #
    # if ny_list.index(params[0]) <= 3:
    #     ny_df.loc[ny_df.CURRENCY == params[0], "LONG_EXIT_QTY"] = list1[2][2]
    #     ny_df.loc[ny_df.CURRENCY == params[0], "LONG_TP"] = list1[2][0]
    #     ny_df.loc[ny_df.CURRENCY == params[0], "LONG_EXIT_EXE"] = list1[2][1]
    #     ny_df.loc[ny_df.CURRENCY == params[0], "LONG_EXIT_TIME"] = list1[2][4]
    #     # if condition_buy == 'FULFILLED':
    #     #     ny_df.loc[ny_df.CURRENCY == params[0], "ACT_RETURN"] = round(
    #     #         (exit_buy_list[1] / entry_result[0] - 1) * (-1) * 100, 2)
    #
    # elif 3 < ny_list.index(params[0]) < 7:
    #     ny_df.loc[ny_df.CURRENCY == params[0], "SHORT_EXIT_QTY"] = list1[3][2]
    #     ny_df.loc[ny_df.CURRENCY == params[0], "SHORT_TP"] = list1[3][0]
    #     ny_df.loc[ny_df.CURRENCY == params[0], "SHORT_EXIT_EXE"] = list1[3][1]
    #     ny_df.loc[ny_df.CURRENCY == params[0], "SHORT_EXIT_TIME"] = list1[3][4]
    #     # if condition_sell == 'FULFILLED':
    #     #     ny_df.loc[ny_df.CURRENCY == params[0], "ACT_RETURN"] = round(
    #     #         (exit_sell_list[1] / entry_result[0] - 1) * 100, 2)
    #
    # elif ny_list.index(params[0]) >= 7:
    #     ny_df.loc[ny_df.CURRENCY == params[0], "LONG_EXIT_QTY"] = list1[2][2]
    #     ny_df.loc[ny_df.CURRENCY == params[0], "LONG_TP"] = list1[2][0]
    #     ny_df.loc[ny_df.CURRENCY == params[0], "LONG_EXIT_EXE"] = list1[2][1]
    #     ny_df.loc[ny_df.CURRENCY == params[0], "LONG_EXIT_TIME"] = list1[2][4]
    #     ny_df.loc[ny_df.CURRENCY == params[0], "SHORT_EXIT_QTY"] = list1[3][2]
    #     ny_df.loc[ny_df.CURRENCY == params[0], "SHORT_TP"] = list1[3][0]
    #     ny_df.loc[ny_df.CURRENCY == params[0], "SHORT_EXIT_EXE"] = list1[3][1]
    #     ny_df.loc[ny_df.CURRENCY == params[0], "SHORT_EXIT_TIME"] = list1[3][4]
    #     # if condition_buy == 'FULFILLED' and condition_sell == 'FULFILLED':
    #     #     london_df.loc[london_df.CURRENCY == params[0], "ACT_RETURN"] = round(((
    #     #     exit_sell_list[1] / entry_result[0] - 1) + ((exit_buy_list[1] /entry_result[0] - 1) * (-1))) * 100, 2)
    #
    # ny_df = ny_df[["CYCLE_DATE", "CURRENCY", "OPEN_POSITION", "ASK", "BID", "CLOSE",
    #                "ENTRY_SIGNAL", "ENTRY_QTY","ENTRY_TYPE", "ENTRY_EXE", "ENTRY_TIME",
    #                "LONG_TP", "LONG_EXIT_QTY", "LONG_EXIT_EXE", "LONG_EXIT_TIME"
    #     , "SHORT_TP", "SHORT_EXIT_QTY", "SHORT_EXIT_EXE", "SHORT_EXIT_TIME"]]
    # path = "C:\REPORTS/" + entry_result[-1] + "_NY.xlsx"
    # writer = pd.ExcelWriter(path)
    # ny_df.to_excel(writer, 'NY')
    # writer.save()

    #### DATABASE
    # for i, row in ny_df.iterrows():
    #     if params[0] == 'AUD.SGD':
    #        table = db.Aud_Sgd
    #     elif params[0] == "CHF.PLN":
    #         table = db.Chf_Pln
    #     elif params[0] == "GBP.CZK":
    #         table = db.Gbp_Czk
    #     elif params[0] == "GBP.PLN":
    #         table = db.Gbp_Pln
    #     elif params[0] == "AUD.ZAR":
    #         table = db.Aud_Zar
    #     elif params[0] == "DKK.NOK":
    #         table = db.Dkk_Nok
    #     elif params[0] == "EUR.HUF":
    #         table = db.Eur_Huf
    #     elif params[0] == "EUR.CZK":
    #         table = db.Eur_Czk
    #     elif params[0] == "GBP.MXN":
    #         table = db.Gbp_Mxn
    #     else:
    #         table = ""
    #
    #     if table != "" and params[0] == row['CURRENCY']:
    #         table_data = {
    #             'cycle_date'    :row["CYCLE_DATE"],
    #             'currency'      : row['CURRENCY'],
    #             'act_return'    : row['ACT_RETURN'],
    #             'ask'           : row['ASK'],
    #             'bid'           : row['BID'],
    #             'close'         : row['CLOSE'],
    #             'entry_exe'     : row['ENTRY_EXE'],
    #             'entry_qty'     : row['ENTRY_QTY'],
    #             'entry_signal'  : row['ENTRY_SIGNAL'],
    #             'entry_time'    : row['ENTRY_TIME'],
    #             'long_exit_exe' : row['LONG_EXIT_EXE'],
    #             'long_exit_qty' : row['LONG_EXIT_QTY'],
    #             'long_exit_time': row['LONG_EXIT_TIME'],
    #             'long_TP'       : row['LONG_TP'],
    #             'open_position' : row['OPEN_POSITION'],
    #             'short_exit_exe': row['SHORT_EXIT_EXE'],
    #             'short_exit_qty': row['SHORT_EXIT_QTY'],
    #             'short_exit_time': row['SHORT_EXIT_TIME'],
    #             'short_TP'       : row['SHORT_TP'],
    #             'entry_type'     : row['ENTRY_TYPE']
    #         }
    #
    #         result = table.insert(table_data)

    return  ###EXAMPLE####################


################################ MAIN FUNCTIONS ########################################################
def run_london():
    currtime = datetime.now().strftime('%H:%M:%S')
    if currtime < "12:56:00":
        sleeptime = (
                datetime.strptime("12:56:01", '%H:%M:%S') - datetime.strptime(currtime, '%H:%M:%S')).total_seconds()
        print("LONDON cycle is sleeping till scheduled time 12:56", sleeptime)
        time.sleep(sleeptime)

    # disconn()
    # conn()
    london_list = ["AUD.NZD", "CHF.NOK", "CHF.SEK", "NZD.CHF", "GBP.SEK", "EUR.NZD", "EUR.NOK", "EUR.SEK", "GBP.NOK",
                   "NOK.SEK"]

    london_df = pd.DataFrame(
        {"CURRENCY": london_list, "OPEN_POSITION": np.nan, "ENTRY_QTY": np.nan, "ENTRY_SIGNAL": np.nan, "ASK": np.nan,
         "BID": np.nan, "ENTRY_TYPE": np.nan,
         "ENTRY_EXE": np.nan, "ENTRY_TIME": np.nan,
         "BUY_EXIT_QTY": np.nan, "BUY_TP1": np.nan, "BUY_TP2": np.nan, "BUY_EXIT_EXE": np.nan, "BUY_EXIT_TIME": np.nan,
         "SELL_EXIT_QTY": np.nan, "SELL_TP1": np.nan, "SELL_TP2": np.nan, "SELL_EXIT_EXE": np.nan,
         "SELL_EXIT_TIME": np.nan,
         "CLOSE": np.nan, "CYCLE_DATE": np.nan})

    london_exit_df = pd.DataFrame(
        {"CURRENCY": ["AUD.NZD", "CHF.NOK", "CHF.SEK", "NZD.CHF", "GBP.SEK", "EUR.NZD", "EUR.NOK", "EUR.NOK",
                      "EUR.SEK", "EUR.SEK", "GBP.NOK", "GBP.NOK", "NOK.SEK", "NOK.SEK"],
         "SIGNAL": ["BUY", "BUY", "BUY", "BUY", "BUY", "SELL", "BUY", "SELL", "BUY", "SELL", "BUY", "SELL", "BUY",
                    "SELL"],
         "MID": ['NO'] * 14,
         "BID": ['NO'] * 14,
         "ASK": ['NO'] * 14,
         "TP HIT": ['NO'] * 14,
         "FILLED": ['NO'] * 14,
         "FILLED@": [""] * 14})

    ############ UPDATE DAILY ####################################################
    print("&&&&&&&  CALCULATING LONDON QTY &&&&&&&&&&&&&&&&")
    ####
    df_common_inputs = pd.read_excel('C:/database/FX_DAILY/FOREX_settings.xlsx', 'common_inputs')
    df_london = pd.read_excel('C:/database/FX_DAILY/FOREX_settings.xlsx', 'LONDON_TRADES')
    df_o_val = pd.read_excel('C:/database/FX_DAILY/FOREX_settings.xlsx', 'LONDON_ENTRY')
    df_switch = pd.read_excel('C:/database/FX_DAILY/FOREX_settings.xlsx', 'LONDON_SWITCH')

    ################################HC & LC SPREAD ###############################################
    closetime = df_common_inputs["CLOSE TIME"].iloc[-1].strftime("%H:%M:%S")
    time_ldn = " " + closetime
    closetime = datetime.strptime(closetime, "%H:%M:%S")
    lookback = int(df_common_inputs["LOOKBACK"].iloc[-1])
    ###################################################################################################
    ldn_cap = df_common_inputs["cap_london"].iloc[-1]
    ldn_long_cap = float(df_common_inputs['lon_long_perc'].iloc[0]) * ldn_cap
    ldn_short_cap = float(df_common_inputs['lon_short_perc'].iloc[0]) * ldn_cap

    london_list_wgts = [float(df_london['audnzd_short'].iloc[0]), float(df_london['chfnok_short'].iloc[0]),
                        float(df_london['chfsek_short'].iloc[0]), float(df_london['nzdchf_short'].iloc[0]),
                        float(df_london['gbpsek_short'].iloc[0]), float(df_london['eurnzd_long'].iloc[0])]

    london_ls_weights_L = [float(df_london['eurnok_long'].iloc[0]), float(df_london['eursek_long'].iloc[0]),
                           float(df_london['gbpnok_long'].iloc[0]), float(df_london['noksek_long'].iloc[0])]
    london_ls_weights_S = [float(df_london['eurnok_short'].iloc[0]), float(df_london['eursek_short'].iloc[0]),
                           float(df_london['gbpnok_short'].iloc[0]), float(df_london['noksek_short'].iloc[0])]

    london_list_qty_short = []  # individual short correncies qty stored in this variable
    london_list_qty_long = []  # individual long correncies qty stored in this variable
    london_list_qty_L = []  # long qty from LS model
    london_list_qty_S = []  # short qty from LS model
    ####################################################################

    ##QTY CALCULATION
    conn()
    for i in range(len(london_list)):

        ########################if cycle get delayed#####################################################
        currtime = datetime.now().strftime('%Y%m%d %H:%M:%S')
        week_day = datetime.now().weekday()

        if week_day != 4 and "NZD" in london_list[i]:
            if "14:59:55" <= currtime[-8:] <= "15:15:05":
                sleeptime = (datetime.strptime("15:15:10", '%H:%M:%S') - datetime.strptime(currtime[-8:],
                                                                                           '%H:%M:%S')).total_seconds()
                print(london_list[i] + " sleeping till mkt opens at 15:15")
                time.sleep(sleeptime)  # sleeptime

        else:
            if "16:59:50" <= currtime[-8:] <= "17:15:05":
                sleeptime = (datetime.strptime("17:15:10", '%H:%M:%S') - datetime.strptime(currtime[-8:],
                                                                                           '%H:%M:%S')).total_seconds()
                print(london_list[i] + " sleeping till mkt opens at 17:15")
                time.sleep(sleeptime)  # sleeptime

        ####################################################################################################

        id = tickerId() + tickerId()
        if (i <= 4):
            id = tickerId() + tickerId()
            if (london_list[i][0:3] in commonwealth_curr):
                contract_Details1 = create.create_contract(london_list[i][0:3], "CASH", "IDEALPRO", "USD", '', '', '',
                                                           '')
                data1 = ask_bid(contract_Details1, id)

                convprice1 = float(data1[0])
                london_list_qty_short.append(
                    int(math.ceil((ldn_short_cap * london_list_wgts[i] * (1 / convprice1)) / 10000) * 10000))
                # print(london_list[i][0:3],".USD")
                conn()
            else:
                contract_Details1 = create.create_contract("USD", "CASH", "IDEALPRO", london_list[i][0:3], '', '', '',
                                                           '')
                data1 = ask_bid(contract_Details1, id)

                convprice1 = float(data1[0])
                london_list_qty_short.append(
                    int(math.ceil((ldn_short_cap * london_list_wgts[i] * convprice1) / 10000) * 10000))
                # print("USD.",london_list[i][0:3])
                conn()
        elif (i == 5):
            id = tickerId() + tickerId()

            if (london_list[i][0:3] in commonwealth_curr):
                contract_Details1 = create.create_contract(london_list[i][0:3], "CASH", "IDEALPRO", "USD", '', '', '',
                                                           '')
                data1 = ask_bid(contract_Details1, id)

                convprice1 = float(data1[0])
                london_list_qty_long.append(
                    int(math.ceil((ldn_long_cap * london_list_wgts[i] * (1 / convprice1)) / 10000) * 10000))
                # print(london_list[i][0:3], ".USD")
                conn()
            else:
                contract_Details1 = create.create_contract("USD", "CASH", "IDEALPRO", london_list[i][0:3], '', '', '',
                                                           '')
                data1 = ask_bid(contract_Details1, id)

                convprice1 = float(data1[0])
                london_list_qty_long.append(
                    int(math.ceil((ldn_long_cap * london_list_wgts[i] * convprice1) / 10000) * 10000))
                # print("USD.", london_list[i][0:3])
                conn()
        elif i > 5:
            id = tickerId() + tickerId()

            if (london_list[i][0:3] in commonwealth_curr):
                contract_Details1 = create.create_contract(london_list[i][0:3], "CASH", "IDEALPRO", "USD", '', '', '',
                                                           '')
                data1 = ask_bid(contract_Details1, id)

                convprice1 = float(data1[0])
                london_list_qty_L.append(
                    int(math.ceil((ldn_long_cap * london_ls_weights_L[i - 6] * (1 / convprice1)) / 10000) * 10000))
                london_list_qty_S.append(
                    int(math.ceil((ldn_short_cap * london_ls_weights_S[i - 6] * (1 / convprice1)) / 10000) * 10000))
                # print(london_list[i][0:3], ".USD")
                conn()
            else:
                contract_Details1 = create.create_contract("USD", "CASH", "IDEALPRO", london_list[i][0:3], '', '', '',
                                                           '')
                data1 = ask_bid(contract_Details1, id)

                convprice1 = float(data1[0])
                london_list_qty_L.append(
                    int(math.ceil((ldn_long_cap * london_ls_weights_L[i - 6] * convprice1) / 10000) * 10000))
                london_list_qty_S.append(
                    int(math.ceil((ldn_short_cap * london_ls_weights_S[i - 6] * convprice1) / 10000) * 10000))
                # print("USD.", london_list[i][0:3])
            conn()
        time.sleep(0.9)
    print("OVER")
    conn()
    #### END OF QTY CALCULATION

    ##### ENTRY ORDER VALIDITY AND DISTRIBUTION
    london_entry_excel = ['audnzd_short', 'chfnok_short', 'chfsek_short', 'nzdchf_short', 'gbpsek_short',
                          'eurnzd_long',
                          'eurnok_long', 'eursek_long', 'gbpnok_long', 'noksek_long',
                          'eurnok_short', 'eursek_short', 'gbpnok_short', 'noksek_short']
    LONDON_ENTRY_short = []
    LONDON_ENTRY_long = []
    LONDON_ENTRY_L = []
    LONDON_ENTRY_S = []
    for i in london_entry_excel:
        pos = london_entry_excel.index(i)
        fav = int(df_o_val.loc[df_o_val.TYPE == 'FAVOURABLE', i].iloc[-1])
        mid = int(df_o_val.loc[df_o_val.TYPE == 'MIDPOINT', i].iloc[-1])
        unfav = int(df_o_val.loc[df_o_val.TYPE == 'UNFAVOURABLE', i].iloc[-1])
        total = int(fav + mid + unfav)
        entry_list = [fav, mid, unfav, total]
        if pos <= 4:
            LONDON_ENTRY_short.append(entry_list)
        elif pos == 5:
            LONDON_ENTRY_long.append(entry_list)
        elif 9 >= pos > 5:
            LONDON_ENTRY_L.append(entry_list)
        elif pos > 9:
            LONDON_ENTRY_S.append(entry_list)

    ### GENERATE ENTRY ORDER PARAMS

    ########################Calculating HC_MA,LC_MA#####################################
    histId = 2000
    length = len(london_list)
    HC_LC_Spreadlist = []
    lag_val = 1
    ##############################################################################################
    for i in range(length):
        contract = create.create_contract(london_list[i][0:3], "CASH", "IDEALPRO", london_list[i][4:7], '', '', '', '')
        data_size = 0
        while data_size < 22:
            asset_hist_data = pd.DataFrame()
            asset_hist_data, histId = historical(time_ldn, contract, "26 D", histId + 1)
            data_size = asset_hist_data.shape[0]
            print("HISTORICAL DATA NOT RECEIVED YET FOR ", london_list[i])

        print("HISTORICAL DATA  RECEIVED FOR ", london_list[i])
        asset_hist_data["LOW CLOSE"] = (pd.Series.rolling(
            (asset_hist_data['low'] / asset_hist_data['close'].shift(lag_val) - 1),
            window=lookback).mean())
        asset_hist_data["HIGH CLOSE"] = ((pd.Series.rolling(
            (asset_hist_data['high'] / asset_hist_data['close'].shift(lag_val) - 1),
            window=lookback).mean()).dropna())

        path = "C:\TP_VALUE_LOG/movingAVG_" + london_list[i] + ".csv"
        asset_hist_data.tail(23).to_csv(path)

        HC_MA = float(abs(asset_hist_data["HIGH CLOSE"].iloc[-1]))
        LC_MA = float(abs(asset_hist_data["LOW CLOSE"].iloc[-1]))
        HC_LC_Spreadlist.append([HC_MA, LC_MA])
    #######################################################################################
    #######################################################################################
    short_params = []
    long_params = []
    long_short_params = []

    for i in range(length):

        hc_ma = HC_LC_Spreadlist[i][0]
        lc_ma = HC_LC_Spreadlist[i][1]

        if i <= 4:
            signal = "SELL"
            qty = london_list_qty_short[i]
            total = LONDON_ENTRY_short[i][3]
            fav_time = LONDON_ENTRY_short[i][0]
            mid_time = LONDON_ENTRY_short[i][1]
            curr_name = london_list[i]

            list1 = [curr_name, signal, qty, total, fav_time, mid_time, hc_ma, lc_ma]
            short_params.append(list1)
        elif i == 5:
            signal = "BUY"
            qty = london_list_qty_long[i - 5]
            total = LONDON_ENTRY_long[i - 5][3]
            fav_time = LONDON_ENTRY_long[i - 5][0]
            mid_time = LONDON_ENTRY_long[i - 5][1]
            curr_name = london_list[i]
            list1 = [curr_name, signal, qty, total, fav_time, mid_time, hc_ma, lc_ma]
            long_params.append(list1)
        elif i >= 6:
            long_qty = london_list_qty_L[i - 6]
            short_qty = london_list_qty_S[i - 6]
            qty = long_qty - short_qty
            if qty > 0:
                signal = "BUY"
            else:
                signal = "SELL"
            curr_name = london_list[i]
            list1 = [curr_name, signal, qty, total, fav_time, mid_time, long_qty, short_qty, hc_ma, lc_ma]
            long_short_params.append(list1)
        time.sleep(1.5)

    ##############################################################################
    # time.sleep(35)

    id_list = [[0], [0]]

    shortsum = int(round(df_london[[col for col in df_london.columns if 'short' in col]].sum(axis=1).iloc[-1]))
    longsum = int(round(df_london[[col for col in df_london.columns if 'long' in col]].sum(axis=1).iloc[-1]))
    if shortsum > 1 and longsum > 1:
        print("dont execute any")
    else:
        print(short_params)
        print(long_params)
        print(long_short_params)
        AUDNZD_params = short_params[0]
        contract = create.create_contract(AUDNZD_params[0][0:3], "CASH", "IDEALPRO", AUDNZD_params[0][4:7], '', '',
                                          '', '')
        ids = orderidfun(acc, contract, AUDNZD_params[1], id_list, 2)
        id_list = ids
        time.sleep(1)
        CHFNOK_params = short_params[1]
        contract = create.create_contract(CHFNOK_params[0][0:3], "CASH", "IDEALPRO", CHFNOK_params[0][4:7], '', '',
                                          '', '')
        ids1 = orderidfun(acc, contract, CHFNOK_params[1], id_list, 2)
        id_list = ids1
        time.sleep(1)
        CHFSEK_params = short_params[2]
        contract = create.create_contract(CHFSEK_params[0][0:3], "CASH", "IDEALPRO", CHFSEK_params[0][4:7], '', '',
                                          '', '')
        ids2 = orderidfun(acc, contract, CHFSEK_params[1], id_list, 2)
        id_list = ids2

        time.sleep(1)
        NZDCHF_params = short_params[3]
        contract = create.create_contract(NZDCHF_params[0][0:3], "CASH", "IDEALPRO", NZDCHF_params[0][4:7], '', '',
                                          '', '')
        ids3 = orderidfun(acc, contract, NZDCHF_params[1], id_list, 2)
        id_list = ids3

        time.sleep(1)
        GBPSEK_params = short_params[4]
        contract = create.create_contract(GBPSEK_params[0][0:3], "CASH", "IDEALPRO", GBPSEK_params[0][4:7], '', '',
                                          '', '')
        ids4 = orderidfun(acc, contract, GBPSEK_params[1], id_list, 2)
        id_list = ids4
        time.sleep(1)
        EURNZD_params = long_params[0]
        contract = create.create_contract(EURNZD_params[0][0:3], "CASH", "IDEALPRO", EURNZD_params[0][4:7], '', '',
                                          '', '')
        ids5 = orderidfun(acc, contract, EURNZD_params[1], id_list, 2)
        id_list = ids5

        time.sleep(1)
        EURNOK_params = long_short_params[0]
        contract = create.create_contract(EURNOK_params[0][0:3], "CASH", "IDEALPRO", EURNOK_params[0][4:7], '', '', '',
                                          '')
        ids6 = orderidfun(acc, contract, EURNOK_params[1], id_list, 2)
        id_list = ids6

        time.sleep(1)
        EURSEK_params = long_short_params[1]
        contract = create.create_contract(EURSEK_params[0][0:3], "CASH", "IDEALPRO", EURSEK_params[0][4:7], '', '', '',
                                          '')
        ids7 = orderidfun(acc, contract, EURSEK_params[1], id_list, 2)
        id_list = ids7

        time.sleep(1)
        GBPNOK_params = long_short_params[2]
        contract = create.create_contract(GBPNOK_params[0][0:3], "CASH", "IDEALPRO", GBPNOK_params[0][4:7], '', '', '',
                                          '')
        ids8 = orderidfun(acc, contract, GBPNOK_params[1], id_list, 2)
        id_list = ids8
        time.sleep(1)
        NOKSEK_params = long_short_params[3]
        contract = create.create_contract(NOKSEK_params[0][0:3], "CASH", "IDEALPRO", NOKSEK_params[0][4:7], '', '',
                                          '', '')
        ids9 = orderidfun(acc, contract, NOKSEK_params[1], id_list, 2)
        id_list = ids9
        #############rewuesting for position#####################

        currtime = datetime.now().strftime('%H:%M:%S')
        if currtime < "12:59:25":
            sleeptime = (
                    datetime.strptime("12:59:26", '%H:%M:%S') - datetime.strptime(currtime, '%H:%M:%S')).total_seconds()
            print("LONDON ENTRIES WILL START AT 12:59:26, SECS LEFT: ", sleeptime)
            time.sleep(sleeptime)

        dat = open_pos()
        dat.to_csv("C:/LOG/openpos_LDN.csv")
        ########################################################
        df_switch = pd.read_excel('C:/database/FX_DAILY/FOREX_settings.xlsx', 'LONDON_SWITCH')
        switchflag = df_switch.loc[df_switch.CURRENCY == 'AUD.NZD', 'TRADE'].iloc[-1]
        if (0 <= df_switch.loc[df_switch.CURRENCY == 'AUD.NZD', 'TP1'].iloc[-1] <= 0.25) and \
                (0 <= df_switch.loc[df_switch.CURRENCY == 'AUD.NZD', 'TP2'].iloc[-1] <= 0.25) and (
                switchflag in ('yes', 'YES')):
            Thread(target=LONDON, args=(dat, AUDNZD_params, ids, 100, 200, london_df,london_exit_df)).start()
        ################################################
        switchflag1 = df_switch.loc[df_switch.CURRENCY == 'CHF.NOK', 'TRADE'].iloc[-1]
        if (0 <= df_switch.loc[df_switch.CURRENCY == 'CHF.NOK', 'TP1'].iloc[-1] <= 0.25) and \
                (0 <= df_switch.loc[df_switch.CURRENCY == 'CHF.NOK', 'TP2'].iloc[-1] <= 0.25) and (
                switchflag1 in ('yes', 'YES')):
            Thread(target=LONDON, args=(dat, CHFNOK_params, ids1, 300, 400, london_df,london_exit_df)).start()
        ###############################################
        time.sleep(1)
        switchflag2 = df_switch.loc[df_switch.CURRENCY == 'CHF.SEK', 'TRADE'].iloc[-1]
        if (0 <= df_switch.loc[df_switch.CURRENCY == 'CHF.SEK', 'TP1'].iloc[-1] <= 0.25) and \
                (0 <= df_switch.loc[df_switch.CURRENCY == 'CHF.SEK', 'TP2'].iloc[-1] <= 0.25) and (
                switchflag2 in ('yes', 'YES')):
            Thread(target=LONDON, args=(dat, CHFSEK_params, ids2, 500, 600, london_df,london_exit_df)).start()
        ##########################################################
        switchflag3 = df_switch.loc[df_switch.CURRENCY == 'NZD.CHF', 'TRADE'].iloc[-1]
        if (0 <= df_switch.loc[df_switch.CURRENCY == 'NZD.CHF', 'TP1'].iloc[-1] <= 0.25) and \
                (0 <= df_switch.loc[df_switch.CURRENCY == 'NZD.CHF', 'TP2'].iloc[-1] <= 0.25) and (
                switchflag3 in ('yes', 'YES')):
            Thread(target=LONDON, args=(dat, NZDCHF_params, ids3, 700, 800, london_df,london_exit_df)).start()
        time.sleep(1)
        ############################################################################
        switchflag4 = df_switch.loc[df_switch.CURRENCY == 'GBP.SEK', 'TRADE'].iloc[-1]
        if (0 <= df_switch.loc[df_switch.CURRENCY == 'GBP.SEK', 'TP1'].iloc[-1] <= 0.25) and \
                (0 <= df_switch.loc[df_switch.CURRENCY == 'GBP.SEK', 'TP2'].iloc[-1] <= 0.25) and (
                switchflag4 in ('yes', 'YES')):
            Thread(target=LONDON, args=(dat, GBPSEK_params, ids4, 900, 1000, london_df,london_exit_df)).start()
        ###############################################################

        switchflag5 = df_switch.loc[df_switch.CURRENCY == 'EUR.NZD', 'TRADE'].iloc[-1]
        if (0 <= df_switch.loc[df_switch.CURRENCY == 'EUR.NZD', 'TP1'].iloc[-1] <= 0.25) and \
                (0 <= df_switch.loc[df_switch.CURRENCY == 'EUR.NZD', 'TP2'].iloc[-1] <= 0.25) and (
                switchflag5 in ('yes', 'YES')):
            Thread(target=LONDON, args=(dat, EURNZD_params, ids5, 1100, 1200, london_df,london_exit_df)).start()

        ##########################################################################
        time.sleep(1)
        switchflag6 = df_switch.loc[df_switch.CURRENCY == 'EUR.NOK', 'TRADE'].iloc[-1]
        if (0 <= df_switch.loc[df_switch.CURRENCY == 'EUR.NOK', 'TP1'].iloc[-1] <= 0.25) and \
                (0 <= df_switch.loc[df_switch.CURRENCY == 'EUR.NOK', 'TP2'].iloc[-1] <= 0.25) and (
                switchflag6 in ('yes', 'YES')):
            Thread(target=LONDON, args=(dat, EURNOK_params, ids6, 1300, 1400, london_df,london_exit_df)).start()
        ##############################################

        switchflag7 = df_switch.loc[df_switch.CURRENCY == 'EUR.SEK', 'TRADE'].iloc[-1]
        if (0 <= df_switch.loc[df_switch.CURRENCY == 'EUR.SEK', 'TP1'].iloc[-1] <= 0.25) and \
                (0 <= df_switch.loc[df_switch.CURRENCY == 'EUR.SEK', 'TP2'].iloc[-1] <= 0.25) and (
                switchflag7 in ('yes', 'YES')):
            Thread(target=LONDON, args=(dat, EURSEK_params, ids7, 1500, 1600, london_df,london_exit_df)).start()
        ##################################################################
        time.sleep(1)
        switchflag8 = df_switch.loc[df_switch.CURRENCY == 'GBP.NOK', 'TRADE'].iloc[-1]
        if (0 <= df_switch.loc[df_switch.CURRENCY == 'GBP.NOK', 'TP1'].iloc[-1] <= 0.25) and \
                (0 <= df_switch.loc[df_switch.CURRENCY == 'GBP.NOK', 'TP2'].iloc[-1] <= 0.25) and (
                switchflag8 in ('yes', 'YES')):
            Thread(target=LONDON, args=(dat, GBPNOK_params, ids8, 1700, 1800, london_df,london_exit_df)).start()

        ##########################################################
        switchflag9 = df_switch.loc[df_switch.CURRENCY == 'NOK.SEK', 'TRADE'].iloc[-1]
        if (0 <= df_switch.loc[df_switch.CURRENCY == 'NOK.SEK', 'TP1'].iloc[-1] <= 0.25) and \
                (0 <= df_switch.loc[df_switch.CURRENCY == 'NOK.SEK', 'TP2'].iloc[-1] <= 0.25) and (
                switchflag9 in ('yes', 'YES')):
            Thread(target=LONDON, args=(dat, NOKSEK_params, ids9, 1900, 1999, london_df,london_exit_df)).start()

        Thread(target=ldn_report, args=(london_df,)).start()
        ########################
    return


def run_ny():
    currtime = datetime.now().strftime('%H:%M:%S')
    if currtime < "14:56:00":
        sleeptime = (
                    datetime.strptime("14:56:01", '%H:%M:%S') - datetime.strptime(currtime, '%H:%M:%S')).total_seconds()
        print("NY cycle is sleeping till scheduled time 14:56", sleeptime)
        time.sleep(sleeptime)

    # conn()
    ny_list = ["AUD.SGD", "CHF.PLN", "GBP.CZK", "GBP.PLN", "AUD.ZAR", "DKK.NOK", "EUR.HUF",
               'EUR.CZK', 'GBP.MXN']

    # for i in ny_list:
    #     df_entry = pd.read_csv("C:\TP_VALUE_LOG/ENTRIES_LOG.csv")
    #     df_entry.loc[df_entry.CURR == i, ["PRICE", "QTY", "AT", "FILLED", "REMAIN", "TIME"]] = ["null","null"
    #         ,"null", "null","null", "null"]
    #
    #     df_entry = df_entry.loc[:, ~df_entry.columns.str.contains('^Unnamed')]
    #     df_entry.to_csv("C:\TP_VALUE_LOG/ENTRIES_LOG.csv")

    ny_df = pd.DataFrame(
        {"CURRENCY": ny_list, "OPEN_POSITION": np.nan, "ENTRY_QTY": np.nan, "ENTRY_SIGNAL": np.nan, "ASK": np.nan,
         "BID": np.nan, "ENTRY_TYPE": np.nan,
         "ENTRY_EXE": np.nan, "ENTRY_TIME": np.nan,
         "BUY_EXIT_QTY": np.nan, "BUY_TP1": np.nan, "BUY_TP2": np.nan, "BUY_EXIT_EXE": np.nan, "BUY_EXIT_TIME": np.nan,
         "SELL_EXIT_QTY": np.nan, "SELL_TP1": np.nan, "SELL_TP2": np.nan, "SELL_EXIT_EXE": np.nan,
         "SELL_EXIT_TIME": np.nan,
         "CLOSE": np.nan, "CYCLE_DATE": np.nan})
    # log_df = pd.DataFrame(
    #     {"CURRENCY": ny_list, "ENTRY_FILLED": np.nan, "ENTRY_REMAIN": np.nan, "TP_BUY": np.nan, "TP_BUY2": np.nan,
    #      "BUY_QTY": np.nan, "TP_SELL": np.nan, "TP_SELL2": np.nan, "SELL_QTY": np.nan})
    # log_df = log_df[
    #     ["CURRENCY", "ENTRY_FILLED", "ENTRY_REMAIN", "TP_BUY", "TP_BUY2", "BUY_QTY", "TP_SELL", "TP_SELL2", "SELL_QTY"]]
    #
    # log_df.to_csv("C:/LOG/log_ny.csv")

    ############# UPDATE DAILY ##################################
    print("&&&&&&&  CALCULATING NY QTY &&&&&&&&&&&&&&&&")
    df_ny = pd.read_excel('C:/database/FX_DAILY/FOREX_settings.xlsx', 'NY_TRADES')
    df_o_val_ny = pd.read_excel('C:/database/FX_DAILY/FOREX_settings.xlsx', 'NY_ENTRY')
    df_switch_ny = pd.read_excel('C:/database/FX_DAILY/FOREX_settings.xlsx', 'NY_SWITCH')

    ######capital and weight#######################################
    df_common_inputs = pd.read_excel('C:/database/FX_DAILY/FOREX_settings.xlsx', 'common_inputs')
    ny_cap = df_common_inputs["cap_ny"].iloc[-1]
    ny_long_cap = float(df_common_inputs['ny_long_perc'].iloc[0]) * ny_cap
    ny_short_cap = float(df_common_inputs['ny_short_perc'].iloc[0]) * ny_cap

    ny_list = ["AUD.SGD", "CHF.PLN", "GBP.CZK", "GBP.PLN",
               "AUD.ZAR", "DKK.NOK", "EUR.HUF",
               'EUR.CZK', 'GBP.MXN']
    ny_list_wgts = [float(df_ny['audsgd_short'].iloc[0]), float(df_ny['chfpln_short'].iloc[0]),
                    float(df_ny['gbpczk_short'].iloc[0]), float(df_ny['gbppln_short'].iloc[0]),
                    float(df_ny['audzar_long'].iloc[0]), float(df_ny['dkknok_long'].iloc[0]),
                    float(df_ny['eurhuf_long'].iloc[0])]

    ny_ls_weights_L = [float(df_ny['eurczk_long'].iloc[0]), float(df_ny['gbpmxn_long'].iloc[0])]

    ny_ls_weights_S = [float(df_ny['eurczk_short'].iloc[0]), float(df_ny['gbpmxn_short'].iloc[0])]
    #################################################################
    ny_list_qty_short = []  # individual short correncies qty stored in this variable
    ny_list_qty_long = []  # individual long correncies qty stored in this variable
    ny_list_qty_L = []  # long qty from LS model
    ny_list_qty_S = []  # short qty from LS model
    ####################################################################

    ##QTY CALCULATION
    for i in range(len(ny_list)):

        ########################if cycle get delayed##############################################################
        currtime = datetime.now().strftime('%Y%m%d %H:%M:%S')
        week_day = datetime.now().weekday()

        if week_day != 4 and "NZD" in ny_list[i]:
            if "14:59:55" <= currtime[-8:] <= "15:15:02":
                sleeptime = (datetime.strptime("15:15:05", '%H:%M:%S') - datetime.strptime(currtime[-8:],
                                                                                           '%H:%M:%S')).total_seconds()
                print(ny_list[i] + " sleeping till mkt opens at 15:15")
                time.sleep(sleeptime)  # sleeptime

        else:
            if "16:59:50" <= currtime[-8:] <= "17:15:05":
                sleeptime = (datetime.strptime("17:15:10", '%H:%M:%S') - datetime.strptime(currtime[-8:],
                                                                                           '%H:%M:%S')).total_seconds()
                print(ny_list[i] + " sleeping till mkt opens at 17:15")
                time.sleep(sleeptime)  # sleeptime

        ####################################################################################################

        id = tickerId() + tickerId()
        if (i <= 3):
            conn()
            id = tickerId() + tickerId()
            if (ny_list[i][0:3] in commonwealth_curr):
                contract_Details1 = create.create_contract(ny_list[i][0:3], "CASH", "IDEALPRO", "USD", '', '', '', '')
                data1 = ask_bid(contract_Details1, id)
                convprice1 = float(data1[0])
                ny_list_qty_short.append(
                    int(math.ceil((ny_short_cap * ny_list_wgts[i] * (1 / convprice1)) / 10000) * 10000))
            else:
                contract_Details1 = create.create_contract("USD", "CASH", "IDEALPRO", ny_list[i][0:3], '', '', '', '')
                data1 = ask_bid(contract_Details1, id)
                convprice1 = float(data1[0])
                ny_list_qty_short.append(
                    int(math.ceil((ny_short_cap * ny_list_wgts[i] * convprice1) / 10000) * 10000))
        elif (3 < i < 7):
            conn()
            id = tickerId() + tickerId()
            if (ny_list[i][0:3] in commonwealth_curr):
                contract_Details1 = create.create_contract(ny_list[i][0:3], "CASH", "IDEALPRO", "USD", '', '', '', '')
                data1 = ask_bid(contract_Details1, id)
                convprice1 = float(data1[0])
                ny_list_qty_long.append(
                    int(math.ceil((ny_long_cap * ny_list_wgts[i] * (1 / convprice1)) / 10000) * 10000))
            else:
                contract_Details1 = create.create_contract("USD", "CASH", "IDEALPRO", ny_list[i][0:3], '', '', '', '')
                data1 = ask_bid(contract_Details1, id)
                convprice1 = float(data1[0])
                ny_list_qty_long.append(
                    int(math.ceil((ny_long_cap * ny_list_wgts[i] * convprice1) / 10000) * 10000))
        elif i >= 7:
            conn()
            id = tickerId() + tickerId()
            if (ny_list[i][0:3] in commonwealth_curr):
                contract_Details1 = create.create_contract(ny_list[i][0:3], "CASH", "IDEALPRO", "USD", '', '', '', '')
                data1 = ask_bid(contract_Details1, id)
                convprice1 = float(data1[0])
                ny_list_qty_L.append(
                    int(math.ceil((ny_long_cap * ny_ls_weights_L[i - 7] * (1 / convprice1)) / 10000) * 10000))
                ny_list_qty_S.append(
                    int(math.ceil((ny_short_cap * ny_ls_weights_S[i - 7] * (1 / convprice1)) / 10000) * 10000))

            else:
                contract_Details1 = create.create_contract("USD", "CASH", "IDEALPRO", ny_list[i][0:3], '', '', '', '')
                data1 = ask_bid(contract_Details1, id)
                convprice1 = float(data1[0])
                ny_list_qty_L.append(
                    int(math.ceil((ny_long_cap * ny_ls_weights_L[i - 7] * convprice1) / 10000) * 10000))
                ny_list_qty_S.append(
                    int(math.ceil((ny_short_cap * ny_ls_weights_S[i - 7] * convprice1) / 10000) * 10000))
        conn()
        time.sleep(0.4)
    print("OVER")
    conn()
    #### END OF QTY CALCULATION

    ##### ENTRY ORDER VALIDITY AND DISTRIBUTION
    ny_entry_excel = ['audsgd_short', 'chfpln_short', 'gbpczk_short', 'gbppln_short',
                      'audzar_long', 'dkknok_long', 'eurhuf_long',
                      'eurczk_long', 'gbpmxn_long',
                      'eurczk_short', 'gbpmxn_short']
    NY_ENTRY_short = []
    NY_ENTRY_long = []
    NY_ENTRY_L = []
    NY_ENTRY_S = []
    for i in ny_entry_excel:
        pos = ny_entry_excel.index(i)
        fav = int(df_o_val_ny.loc[df_o_val_ny.TYPE == 'FAVOURABLE', i].iloc[-1])
        mid = int(df_o_val_ny.loc[df_o_val_ny.TYPE == 'MIDPOINT', i].iloc[-1])
        unfav = int(df_o_val_ny.loc[df_o_val_ny.TYPE == 'UNFAVOURABLE', i].iloc[-1])
        total = int(fav + mid + unfav)
        entry_list = [fav, mid, unfav, total]
        if pos <= 3:
            NY_ENTRY_short.append(entry_list)
        elif 3 < pos < 7:
            NY_ENTRY_long.append(entry_list)
        elif 7 <= pos < 9:
            NY_ENTRY_L.append(entry_list)
        elif pos >= 9:
            NY_ENTRY_S.append(entry_list)

    ### GENERATE ENTRY ORDER PARAMS
    short_params_ny = []
    long_params_ny = []
    long_short_params_ny = []
    length = len(ny_list)
    for i in range(length):
        if i <= 3:
            signal = "SELL"
            qty = ny_list_qty_short[i]
            total = NY_ENTRY_short[i][3]
            fav_time = NY_ENTRY_short[i][0]
            mid_time = NY_ENTRY_short[i][1]
            curr_name = ny_list[i]
            list1 = [curr_name, signal, qty, total, fav_time, mid_time]
            short_params_ny.append(list1)
        elif 3 < i < 7:
            signal = "BUY"
            qty = ny_list_qty_long[i - 4]
            total = NY_ENTRY_long[i - 4][3]
            fav_time = NY_ENTRY_long[i - 4][0]
            mid_time = NY_ENTRY_long[i - 4][1]
            curr_name = ny_list[i]
            list1 = [curr_name, signal, qty, total, fav_time, mid_time]
            long_params_ny.append(list1)
        elif i >= 7:
            long_qty = ny_list_qty_L[i - 7]
            short_qty = ny_list_qty_S[i - 7]
            qty = long_qty - short_qty
            if qty > 0:
                signal = "BUY"
            else:
                signal = "SELL"
            curr_name = ny_list[i]
            list1 = [curr_name, signal, qty, total, fav_time, mid_time, long_qty, short_qty]
            long_short_params_ny.append(list1)

    #############################################################

    id_list = [[0], [0]]

    shortsum = int(round(df_ny[[col for col in df_ny.columns if 'short' in col]].sum(axis=1).iloc[-1]))
    longsum = int(round(df_ny[[col for col in df_ny.columns if 'long' in col]].sum(axis=1).iloc[-1]))
    if shortsum > 1 and longsum > 1:
        print("dont execute any")
    else:
        print(long_params_ny)
        print(short_params_ny)
        print(long_short_params_ny)
        #######################short########################
        AUDSGD_params = short_params_ny[0]
        contract = create.create_contract(AUDSGD_params[0][0:3], "CASH", "IDEALPRO", AUDSGD_params[0][4:7], '', '', '',
                                          '')
        audsgdids = orderidfun(acc, contract, AUDSGD_params[1], id_list, 2)
        id_list = audsgdids

        CHFPLN_params = short_params_ny[1]
        contract = create.create_contract(CHFPLN_params[0][0:3], "CASH", "IDEALPRO", CHFPLN_params[0][4:7], '', '', '',
                                          '')
        chfplnids = orderidfun(acc, contract, CHFPLN_params[1], id_list, 2)
        id_list = chfplnids

        GBPCZK_params = short_params_ny[2]
        contract = create.create_contract(GBPCZK_params[0][0:3], "CASH", "IDEALPRO", GBPCZK_params[0][4:7], '', '', '',
                                          '')
        gbpczkids = orderidfun(acc, contract, GBPCZK_params[1], id_list, 2)
        id_list = gbpczkids

        GBPPLN_params = short_params_ny[3]
        contract = create.create_contract(GBPPLN_params[0][0:3], "CASH", "IDEALPRO", GBPPLN_params[0][4:7], '', '', '',
                                          '')
        gbpplnids = orderidfun(acc, contract, GBPPLN_params[1], id_list, 2)
        id_list = gbpplnids
        ########LONG#################

        AUDZAR_params = long_params_ny[0]
        contract = create.create_contract(AUDZAR_params[0][0:3], "CASH", "IDEALPRO", AUDZAR_params[0][4:7], '', '',
                                          '', '')
        audzarids = orderidfun(acc, contract, AUDZAR_params[1], id_list, 2)
        id_list = audzarids

        DKKNOK_params = long_params_ny[1]
        contract = create.create_contract(DKKNOK_params[0][0:3], "CASH", "IDEALPRO", DKKNOK_params[0][4:7], '', '',
                                          '', '')
        dkknokids = orderidfun(acc, contract, DKKNOK_params[1], id_list, 2)
        id_list = dkknokids

        EURHUF_params = long_params_ny[2]
        contract = create.create_contract(EURHUF_params[0][0:3], "CASH", "IDEALPRO", EURHUF_params[0][4:7], '', '',
                                          '', '')
        eurhufids = orderidfun(acc, contract, EURHUF_params[1], id_list, 2)
        id_list = eurhufids
        #######################LS#################################################

        EURCZK_params = long_short_params_ny[0]
        contract = create.create_contract(EURCZK_params[0][0:3], "CASH", "IDEALPRO", EURCZK_params[0][4:7], '', '', '',
                                          '')
        eurczkids = orderidfun(acc, contract, EURCZK_params[1], id_list, 2)
        id_list = eurczkids

        GBPMXN_params = long_short_params_ny[1]
        contract = create.create_contract(GBPMXN_params[0][0:3], "CASH", "IDEALPRO", GBPMXN_params[0][4:7], '', '', '',
                                          '')
        gbpmxnids = orderidfun(acc, contract, GBPMXN_params[1], id_list, 2)
        id_list = gbpmxnids
        ##########################requesting for openpos##############################################

        currtime = datetime.now().strftime('%H:%M:%S')
        if currtime < "14:59:20":
            sleeptime = (datetime.strptime("14:59:21", '%H:%M:%S') - datetime.strptime(currtime,
                                                                                       '%H:%M:%S')).total_seconds()
            print("NY ENTRIES WILL START AT 14:59, SECS LEFT :", sleeptime)
            time.sleep(sleeptime)

        dat = open_pos()
        dat.to_csv("C:/LOG/openpos_NY.csv")
        #####################################
        ###########################################################################
        df_switch = pd.read_excel('C:/database/FX_DAILY/FOREX_settings.xlsx', 'NY_SWITCH')

        switchflag = df_switch.loc[df_switch.CURRENCY == 'AUD.SGD', 'TRADE'].iloc[-1]
        if (0 <= df_switch.loc[df_switch.CURRENCY == 'AUD.SGD', 'TP1'].iloc[-1] <= 0.25) and \
                (0 <= df_switch.loc[df_switch.CURRENCY == 'AUD.SGD', 'TP2'].iloc[-1] <= 0.25) and (
                switchflag in ('yes', 'YES')):
            Thread(target=NY, args=(dat, AUDSGD_params, audsgdids, 2000, 2100, ny_df)).start()
        ##################################################
        switchflag1 = df_switch.loc[df_switch.CURRENCY == 'CHF.PLN', 'TRADE'].iloc[-1]
        if (0 <= df_switch.loc[df_switch.CURRENCY == 'CHF.PLN', 'TP1'].iloc[-1] <= 0.25) and \
                (0 <= df_switch.loc[df_switch.CURRENCY == 'CHF.PLN', 'TP2'].iloc[-1] <= 0.25) and (
                switchflag1 in ('yes', 'YES')):
            Thread(target=NY, args=(dat, CHFPLN_params, chfplnids, 2200, 2300, ny_df)).start()
        ##################################################
        switchflag2 = df_switch.loc[df_switch.CURRENCY == 'GBP.CZK', 'TRADE'].iloc[-1]
        if (0 <= df_switch.loc[df_switch.CURRENCY == 'GBP.CZK', 'TP1'].iloc[-1] <= 0.25) and \
                (0 <= df_switch.loc[df_switch.CURRENCY == 'GBP.CZK', 'TP2'].iloc[-1] <= 0.25) and (
                switchflag2 in ('yes', 'YES')):
            Thread(target=NY, args=(dat, GBPCZK_params, gbpczkids, 2400, 2500, ny_df)).start()

        ##################################################
        switchflag3 = df_switch.loc[df_switch.CURRENCY == 'GBP.PLN', 'TRADE'].iloc[-1]
        if (0 <= df_switch.loc[df_switch.CURRENCY == 'GBP.PLN', 'TP1'].iloc[-1] <= 0.25) and \
                (0 <= df_switch.loc[df_switch.CURRENCY == 'GBP.PLN', 'TP2'].iloc[-1] <= 0.25) and (
                switchflag3 in ('yes', 'YES')):
            Thread(target=NY, args=(dat, GBPPLN_params, gbpplnids, 2600, 2700, ny_df)).start()
        ##################################################################

        switchflag4 = df_switch.loc[df_switch.CURRENCY == 'AUD.ZAR', 'TRADE'].iloc[-1]
        if (0 <= df_switch.loc[df_switch.CURRENCY == 'AUD.ZAR', 'TP1'].iloc[-1] <= 0.25) and \
                (0 <= df_switch.loc[df_switch.CURRENCY == 'AUD.ZAR', 'TP2'].iloc[-1] <= 0.25) and (
                switchflag4 in ('yes', 'YES')):
            Thread(target=NY, args=(dat, AUDZAR_params, audzarids, 2800, 2900, ny_df)).start()
        ##################################################
        switchflag5 = df_switch.loc[df_switch.CURRENCY == 'DKK.NOK', 'TRADE'].iloc[-1]
        if (0 <= df_switch.loc[df_switch.CURRENCY == 'DKK.NOK', 'TP1'].iloc[-1] <= 0.25) and \
                (0 <= df_switch.loc[df_switch.CURRENCY == 'DKK.NOK', 'TP2'].iloc[-1] <= 0.25) and (
                switchflag5 in ('yes', 'YES')):
            Thread(target=NY, args=(dat, DKKNOK_params, dkknokids, 3000, 3100, ny_df)).start()

        ##################################################
        switchflag6 = df_switch.loc[df_switch.CURRENCY == 'EUR.HUF', 'TRADE'].iloc[-1]
        if (0 <= df_switch.loc[df_switch.CURRENCY == 'EUR.HUF', 'TP1'].iloc[-1] <= 0.25) and \
                (0 <= df_switch.loc[df_switch.CURRENCY == 'EUR.HUF', 'TP2'].iloc[-1] <= 0.25) and (
                switchflag6 in ('yes', 'YES')):
            Thread(target=NY, args=(dat, EURHUF_params, eurhufids, 3200, 3300, ny_df)).start()

        ##################################################
        switchflag7 = df_switch.loc[df_switch.CURRENCY == 'EUR.CZK', 'TRADE'].iloc[-1]
        if (0 <= df_switch.loc[df_switch.CURRENCY == 'EUR.CZK', 'TP1'].iloc[-1] <= 0.25) and \
                (0 <= df_switch.loc[df_switch.CURRENCY == 'EUR.CZK', 'TP2'].iloc[-1] <= 0.25) and (
                switchflag7 in ('yes', 'YES')):
            Thread(target=NY, args=(dat, EURCZK_params, eurczkids, 3400, 3500, ny_df)).start()
        ##################################################
        switchflag8 = df_switch.loc[df_switch.CURRENCY == 'GBP.MXN', 'TRADE'].iloc[-1]
        if (0 <= df_switch.loc[df_switch.CURRENCY == 'GBP.MXN', 'TP1'].iloc[-1] <= 0.25) and \
                (0 <= df_switch.loc[df_switch.CURRENCY == 'GBP.MXN', 'TP2'].iloc[-1] <= 0.25) and (
                switchflag8 in ('yes', 'YES')):
            Thread(target=NY, args=(dat, GBPMXN_params, gbpmxnids, 3600, 3700, ny_df)).start()
        ###############################################################
        Thread(target=ny_report, args=(ny_df,)).start()
    return


def ACC_PORTFOLIO():
    df_net = pd.DataFrame()
    while df_net.empty:
        tws.reqAccountUpdates(2, acc)
        tws.reqAccountSummary(2, "All", "NetLiquidation")

        df_net = pd.DataFrame(callback.account_Summary,
                              columns=['Request_ID', 'Account', 'Tag', 'Value', 'Curency'])
        # df_port=pd.DataFrame(callback.update_Portfolio,
        #              columns=['Contract ID','Currency',
        #                       'Expiry','Include Expired',
        #                       'Local Symbol','Multiplier',
        #                       'Primary Exchange','Right',
        #                       'Security Type','Strike',
        #                       'Symbol','Trading Class',
        #                       'Position','Market Price','Market Value',
        #                       'Average Cost', 'Unrealised PnL', 'Realised PnL',
        #                       'Account Name'])
        # df_port["DATETIME"]= datetime.now().strftime("%Y%m%d %H:%M:%S")
        df_net["DATETIME"] = datetime.now().strftime("%Y%m%d %H:%M:%S")
        df_net = df_net.tail(1)

    time.sleep(900)
    # date1=datetime.now().strftime("%Y%m%d %H:%M:%S")
    df = pd.read_excel("C:/database\FX_DAILY/DATA_SWITCH.xlsx", "COND")
    flag = df.loc[df.TYPE == "ACC_DATA", "STREAM"].iloc[-1]
    while flag == "ON":
        conn()
        print("FETCHING DATA")
        df = pd.read_excel("C:/database\FX_DAILY/DATA_SWITCH.xlsx", "COND")
        flag = df.loc[df.TYPE == "ACC_DATA", "STREAM"].iloc[-1]
        tws.reqAccountUpdates(2, acc)
        tws.reqAccountSummary(2, "All", "NetLiquidation")

        df1 = pd.DataFrame(callback.account_Summary,
                           columns=['Request_ID', 'Account', 'Tag', 'Value', 'Curency'])
        # df2 = pd.DataFrame(callback.update_Portfolio,
        #                       columns=['Contract ID', 'Currency',
        #                                'Expiry', 'Include Expired',
        #                                'Local Symbol', 'Multiplier',
        #                                'Primary Exchange', 'Right',
        #                                'Security Type', 'Strike',
        #                                'Symbol', 'Trading Class',
        #                                'Position', 'Market Price', 'Market Value',
        #                                'Average Cost', 'Unrealised PnL', 'Realised PnL',
        #                                'Account Name'])

        df1["DATETIME"] = datetime.now().strftime("%Y%m%d %H:%M:%S")
        # df2["DATETIME"] = datetime.now().strftime("%Y%m%d %H:%M:%S")
        # df_port=pd.concat([df_port,df2])
        df1 = df1.tail(1)
        df_net = pd.concat([df_net, df1])
        # df_port.to_csv("C:/database\FX_DAILY/FOREX_PORTFOLIO.csv")
        df_net.to_csv("C:/database\FX_DAILY/FOREX_NET.csv", index=False)
        print("**************** NET LIQ:  ", datetime.now().strftime("%Y%m%d %H:%M:%S"), "", df_net.tail(1),
              " *********************")
        time.sleep(900)
    print("$$$$$  END OF DATA STREAM $$$$$")
    # print("############### PORTFOLIO ##########################")
    # print(df_port.tail(5))
    # print("############### NET LIQUIDATION ##########################")
    # print(df_net.tail(5))
    return


def backup():
    Thread(target=run_london()).start()
    # Thread(target=run_ny()).start()
    return


import schedule

df_schedule = pd.read_excel('C:/database/FX_DAILY/FOREX_settings.xlsx', 'SCHEDULE')

if df_schedule.loc[df_schedule.DAY == "MONDAY", "PERMISSION_LONDON"].iloc[-1] == "YES" or \
        df_schedule.loc[df_schedule.DAY == "MONDAY", "PERMISSION_LONDON"].iloc[-1] == "yes":
    runtime = (df_schedule.loc[df_schedule.DAY == "MONDAY", "TIME_LONDON"].iloc[-1])
    runtime = runtime.strftime("%H:%M")
    print(runtime, "MONDAY")
    schedule.every().monday.at(runtime).do(run_london)
if df_schedule.loc[df_schedule.DAY == "TUESDAY", "PERMISSION_LONDON"].iloc[-1] == "YES" or \
        df_schedule.loc[df_schedule.DAY == "TUESDAY", "PERMISSION_LONDON"].iloc[-1] == "yes":
    runtime = (df_schedule.loc[df_schedule.DAY == "TUESDAY", "TIME_LONDON"].iloc[-1])
    runtime = runtime.strftime("%H:%M")
    print(runtime, "TUESDAY")
    schedule.every().tuesday.at(runtime).do(run_london)
if df_schedule.loc[df_schedule.DAY == "WEDNESDAY", "PERMISSION_LONDON"].iloc[-1] == "YES" or \
        df_schedule.loc[df_schedule.DAY == "WEDNESDAY", "PERMISSION_LONDON"].iloc[-1] == "yes":
    runtime = (df_schedule.loc[df_schedule.DAY == "WEDNESDAY", "TIME_LONDON"].iloc[-1])
    runtime = runtime.strftime("%H:%M")
    print(runtime, "WEDNESDAY")
    schedule.every().wednesday.at(runtime).do(run_london)

if df_schedule.loc[df_schedule.DAY == "THURSDAY", "PERMISSION_LONDON"].iloc[-1] == "YES" or \
        df_schedule.loc[df_schedule.DAY == "THURSDAY", "PERMISSION_LONDON"].iloc[-1] == "yes":
    runtime = (df_schedule.loc[df_schedule.DAY == "THURSDAY", "TIME_LONDON"].iloc[-1])
    runtime = runtime.strftime("%H:%M")
    print(runtime, "THURSDAY")
    schedule.every().thursday.at(runtime).do(run_london)

if df_schedule.loc[df_schedule.DAY == "FRIDAY", "PERMISSION_LONDON"].iloc[-1] == "YES" or \
        df_schedule.loc[df_schedule.DAY == "FRIDAY", "PERMISSION_LONDON"].iloc[-1] == "yes":
    runtime = (df_schedule.loc[df_schedule.DAY == "FRIDAY", "TIME_LONDON"].iloc[-1])
    runtime = runtime.strftime("%H:%M")
    print(runtime, "FRIDAY")
    schedule.every().friday.at(runtime).do(run_london)

###############NY
if df_schedule.loc[df_schedule.DAY == "MONDAY", "PERMISSION_NY"].iloc[-1] == "YES" or \
        df_schedule.loc[df_schedule.DAY == "MONDAY", "PERMISSION_NY"].iloc[-1] == "yes":
    runtime = (df_schedule.loc[df_schedule.DAY == "MONDAY", "TIME_NY"].iloc[-1])
    runtime = runtime.strftime("%H:%M")
    print(runtime, "MONDAY")
    schedule.every().monday.at(runtime).do(run_ny)
if df_schedule.loc[df_schedule.DAY == "TUESDAY", "PERMISSION_NY"].iloc[-1] == "YES" or \
        df_schedule.loc[df_schedule.DAY == "TUESDAY", "PERMISSION_NY"].iloc[-1] == "yes":
    runtime = (df_schedule.loc[df_schedule.DAY == "TUESDAY", "TIME_NY"].iloc[-1])
    runtime = runtime.strftime("%H:%M")
    print(runtime, "TUESDAY")
    schedule.every().tuesday.at(runtime).do(run_ny)
if df_schedule.loc[df_schedule.DAY == "WEDNESDAY", "PERMISSION_NY"].iloc[-1] == "YES" or \
        df_schedule.loc[df_schedule.DAY == "WEDNESDAY", "PERMISSION_NY"].iloc[-1] == "yes":
    runtime = (df_schedule.loc[df_schedule.DAY == "WEDNESDAY", "TIME_NY"].iloc[-1])
    runtime = runtime.strftime("%H:%M")
    print(runtime, "WEDNESDAY")
    schedule.every().wednesday.at(runtime).do(run_ny)

if df_schedule.loc[df_schedule.DAY == "THURSDAY", "PERMISSION_NY"].iloc[-1] == "YES" or \
        df_schedule.loc[df_schedule.DAY == "THURSDAY", "PERMISSION_NY"].iloc[-1] == "yes":
    runtime = (df_schedule.loc[df_schedule.DAY == "THURSDAY", "TIME_NY"].iloc[-1])
    runtime = runtime.strftime("%H:%M")
    print(runtime, "THURSDAY")
    schedule.every().thursday.at(runtime).do(run_ny)

if df_schedule.loc[df_schedule.DAY == "FRIDAY", "PERMISSION_NY"].iloc[-1] == "YES" or \
        df_schedule.loc[df_schedule.DAY == "FRIDAY", "PERMISSION_NY"].iloc[-1] == "yes":
    runtime = (df_schedule.loc[df_schedule.DAY == "FRIDAY", "TIME_NY"].iloc[-1])
    runtime = runtime.strftime("%H:%M")
    print(runtime, "FRIDAY")
    schedule.every().friday.at(runtime).do(run_ny)

while True:
    schedule.run_pending()
    time.sleep(1)
