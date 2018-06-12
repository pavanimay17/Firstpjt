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
port = 7499
clientId = 100
create = contract()
callback.initiate_variables()

acc = "DU228380"
total_cap = 2000000
Core_port_levrg = 1
LDN_curr_levrg = 16
NY_curr_levrg = 10

# transformation_1_wghts = 0.03816
# transformation_2_wghts = 0.03669
# metaphor_wghts = 0.06162
# # camouflage_wghts =
# fundamentals_wghts = 0.06747
#
# camouflage_intraday_TP = 0.05
# camouflage_EOD_TP = 0.03
#
# transformation_1 = 'YES'
# transformation_2 = 'YES'
# metaphor = 'YES'
# camouflage = 'YES'
# fundamentals = 'YES'
# hl_eqty = 'YES'
# hl_curr_ldn = 'YES'
# hl_curr_ny = 'YES'



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
    return data_df


def ask_bid(contract, tickerId):

    tickedId = int(tickerId)

    # tws.cancelMktData(tickedId)
    tick = contract
    ask = 1
    bid = 1
    attempts=[0]
    while ask == 1 and bid == 1:
        # sleeptime=0.4

        conn()
        tickedId = int(tickerId + 1)
        tick_data1 = pd.DataFrame()
        while tick_data1.empty:
            instance=attempts[-1]
            if instance > 4:
                conn()
                print("ATTEMPTING HARDER:",instance)
                tickedId = int(tickerId+1)
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
        time.sleep(sleeptime/2)

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
    mid = float((ask +bid)/2)
    # #print("OBTAINED CORRECT DETAILS FOR")


    return ask, bid, mid


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


def ask_bid_last(contract, tickerId):
    tickedId = int(tickerId)
    tick = contract
    ask = 1
    bid = 1
    last = 1
    while ask == 1 and bid == 1 and last == 1:
        conn()
        tickedId = tickedId + 1
        tick_data1 = pd.DataFrame()
        while tick_data1.empty:
            tws.reqMktData(tickedId, tick, "", False)
            time.sleep(2)
            tick_data1 = list(callback.tick_Price)
            for k in range(0, len(tick_data1)):
                tick_data1[k] = tuple(tick_data1[k])
            tick_data1 = pd.DataFrame(tick_data1,
                                      columns=['tickerId', 'field', 'price', 'canAutoExecute'])
            tick_data1 = tick_data1[tick_data1.tickerId == tickedId]
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
        tick_data1["Type"] = tick_data1["field"].map(tick_type)
        # #print(tick_data1)
        a = tick_data1
        # #print(a)
        status1 = 'ASK PRICE' in tick_data1.Type.values
        status2 = 'BID PRICE' in tick_data1.Type.values
        status3 = 'LAST PRICE' in tick_data1.Type.values

        time.sleep(1)
        # #print("ASK", status1, "BID", status2)
        if status1 == False or status2 == False or status3 == False:
            # print("ASK AND BID UNAVAILABLE")
            ask = 1
            bid = 1
            last = 1
        else:
            # print("ASK AND BID AVAILABLE")
            ask = a.loc[a["Type"] == 'ASK PRICE', 'price'].iloc[-1]
            bid = a.loc[a["Type"] == 'BID PRICE', 'price'].iloc[-1]
            last = a.loc[a["Type"] == 'LAST PRICE', 'price'].iloc[-1]
            ask = float(ask)
            bid = float(bid)
            last = float(last)
            # ask = a.loc[a["Type"] == 'ASK PRICE', 'price'].iloc[-1]
            # bid = a.loc[a["Type"] == 'BID PRICE', 'price'].iloc[-1]
            # ask=float(ask)
            # bid=float(bid)
    # #print("OBTAINED CORRECT DETAILS FOR")
    return ask, bid, last


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

    buyidlist = generateNumber(loops)

    while buyidlist[0] == sellidlist[0]:
        buyidlist = generateNumber(loops)

    for i in buyidlist:
        order_info = create.create_order(acc, "LMT", 10000, "BUY", 10.000, False, False, None)
        tws.placeOrder(int(i), contract_info, order_info)

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


BO_list = ["EUR.CZK", "SGD.CNH", "EUR.HKD", "AUD.CNH", "EUR.USD", "CHF.PLN", "CHF.CNH", "AUD.CAD","NOK.SEK",
                   "AUD.SGD","EUR.CHF","EUR.CNH","AUD.HKD","NZD.USD","AUD.USD","USD.CNH","USD.ILS",
                   "CL",'CT',]

BO_df = pd.DataFrame(
    {"CURRENCY": BO_list, "OPEN_POSITION": np.nan, "ENTRY_QTY": np.nan, "ENTRY_SIGNAL": np.nan, "ASK": np.nan,
     "BID": np.nan,
     "ENTRY_EXE": np.nan, "ENTRY_TIME": np.nan,
     "LONG_EXIT_QTY": np.nan, "LONG_TP": np.nan, "LONG_EXIT_EXE": np.nan, "LONG_EXIT_TIME": np.nan,
     "SHORT_EXIT_QTY": np.nan, "SHORT_TP": np.nan, "SHORT_EXIT_EXE": np.nan, "SHORT_EXIT_TIME": np.nan,
     "CLOSE": np.nan, "ACT_RETURN": np.nan, "CYCLE_DATE": np.nan})


def LONDON(contract,dat, params, ids, tickerId, histId, BO_df, signal, exit_signal):


    # rolling_window = 8

    # portfolio_size = 15
    # portfolio_allocation = 100

    ################# initialisation #####################
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
    #df_switch = pd.read_excel('C:/database/FX_DAILY/BO_settings.xlsx', 'LONDON_SWITCH')
    df_gtd = pd.read_excel('C:/database/FX_DAILY/BO_settings.xlsx', 'EXIT_GTD')
    df_common_inputs = pd.read_excel('C:/database/FX_DAILY/BO_settings.xlsx', 'BO_COMMON')
    closetime = df_common_inputs["CLOSE TIME"].iloc[-1].strftime("%H:%M:%S")
    time_ldn = " " + closetime
    closetime = datetime.strptime(closetime, "%H:%M:%S")
    lookback1 = int(df_common_inputs["LOOKBACK"].iloc[-1])
    lookback = lookback1

    entry_thresh = float(df_common_inputs["bo_entry_tp"].iloc[-1])
    exit_thresh = float(df_common_inputs["bo_exit_tp"].iloc[-1])

    lag_val = 1
    MA_val = 20

    entry_thresh = 0.01
    exit_thresh = 0.05
    ##########################################################
    # contract = create.create_contract(params[0][0:3], "CASH", "IDEALPRO", params[0][4:7], '', '', '', '')

    asset_hist_data = pd.DataFrame()
    asset_hist_data = historical(time_ldn, contract, "26 D", histId)
    # asset_hist_data = asset_hist_data[:-1]
    close_data = asset_hist_data["close"].iloc[-1]
    close_data = round(float(close_data), 5)

    asset_hist_data = asset_hist_data[["open", "high", "low", "close"]]
    print(asset_hist_data)

    dat = asset_hist_data
    # dat=dat.tail(22)
    dat['temp'] = dat['close'].shift(lag_val)
    # dat['entry_HC_Spread'] = (
    #         abs((dat['high'] / dat['close'].shift(lag_val) - 1).rolling(MA_val).mean()) * entry_thresh).shift(1)
    dat['entry_LC_Spread'] = (
            abs((dat['low'] / dat['close'].shift(lag_val) - 1).rolling(MA_val).mean()) * entry_thresh).shift(1)
    dat['exit_HC_Spread'] = (
            abs((dat['high'] / dat['close'].shift(lag_val) - 1).rolling(MA_val).mean()) * exit_thresh).shift(1)
    # dat['exit_LC_Spread'] = (
    #         abs((dat['low'] / dat['close'].shift(lag_val) - 1).rolling(MA_val).mean()) * exit_thresh).shift(1)
    dat['long_entry'] = 0
    # dat['long_exit'] = 0
    dat = dat.dropna()

    myindex1 = dat.loc[(dat['open'] > dat['temp'])].index
    dat['long_entry'].loc[myindex1] = dat['open'].loc[myindex1] + (
            dat['open'].loc[myindex1] * dat['entry_HC_Spread'].loc[myindex1])
    # dat['long_exit'].loc[myindex1] = dat['open'].loc[myindex1] + (
    #         dat['open'].loc[myindex1] * dat['exit_HC_Spread'].loc[myindex1])

    myindex1 = dat.loc[(dat['long_entry'] == 0)].index
    dat['long_entry'].loc[myindex1] = dat['temp'].loc[myindex1] + (
            dat['temp'].loc[myindex1] * dat['entry_HC_Spread'].loc[myindex1])
    # dat['long_exit'].loc[myindex1] = dat['temp'].loc[myindex1] + (
    #         dat['temp'].loc[myindex1] * dat['exit_HC_Spread'].loc[myindex1])

    long_entry = (round(dat["long_entry"].iloc[-1], 4))
    # long_exit = (round(dat["long_exit"].iloc[-1], 4))

    print("LONG ENTRY:", long_entry) #||   LONG EXIT:", long_exit)

    # dat['short_entry'] = 0
    dat['short_exit'] = 0
    myindex1 = dat.loc[(dat['open'] < dat['temp'])].index
    # dat['short_entry'].loc[myindex1] = dat['open'].loc[myindex1] - (
    #         dat['open'].loc[myindex1] * dat['entry_LC_Spread'].loc[myindex1])
    dat['short_exit'].loc[myindex1] = dat['open'].loc[myindex1] - (
            dat['open'].loc[myindex1] * dat['exit_LC_Spread'].loc[myindex1])

    myindex1 = dat.loc[(dat['short_entry'] == 0)].index
    # dat['short_entry'].loc[myindex1] = dat['temp'].loc[myindex1] - (
    #         dat['temp'].loc[myindex1] * dat['entry_LC_Spread'].loc[myindex1])
    dat['short_exit'].loc[myindex1] = dat['temp'].loc[myindex1] - (
            dat['temp'].loc[myindex1] * dat['exit_LC_Spread'].loc[myindex1])
    dat = dat.dropna()

    # short_entry = (round(dat["short_entry"].iloc[-1], 4))
    short_exit = (round(dat["short_exit"].iloc[-1], 4))


    print("SHORT EXIT:", short_exit)

    ########################################GTD EXPDATE########################################################################
    exp_time = str(df_common_inputs["GTD"].iloc[-1])
    week_day = datetime.now().weekday()

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

    ########################### for entry ##############################################################################
    print(params[0], "WAITING TO PLACE ", signal, params[3])
    pos = 0
    while pos == 0:
        rt_price = ask_bid(contract, tickerId)
        data_fetch_time = datetime.now().strftime("%Y%m%d %H:%M:%S")

        if signal == "BUY":

            while rt_price[0] == -1 or rt_price[1] == -1:
                negative_time = datetime.now().strftime("%H:%M:%S")
                print("!!!!!! VALID DATA NOT OBTAINED FROM IB FOR " + params[0] + negative_time + " !!!!")
                print(datetime.now().strftime("%Y%m%d %H:%M:%S"))
                tickerId = random.randint(5000, 8000)
                rt_price = ask_bid(contract, tickerId)
            if rt_price[0] <= long_entry:
                print("ASK:", rt_price[0], "LONG_ENTRY:", long_entry)
                actual_tp_buy = long_entry
                fetchtime = datetime.now().strftime("%Y%m%d %H:%M:%S")
                print("CONDITION MET FOR", params[0], signal, "at", fetchtime, "PRICE:", rt_price[0])
                id = int(ids[1][0])
                price = float(round(rt_price[0], round1))
                qty = int(params[3])
                condition = "MET"
            else:
                condition = "NOT MET"


        # elif signal == "SELL":
        #
        #     while rt_price[0] == -1 or rt_price[1] == -1:
        #         negative_time = datetime.now().strftime("%H:%M:%S")
        #         print("!!!!!! VALID DATA NOT OBTAINED FROM IB FOR " + params[0] + negative_time + " !!!!")
        #         print(datetime.now().strftime("%Y%m%d %H:%M:%S"))
        #         tickerId = random.randint(5000, 8000)
        #         rt_price = ask_bid(contract, tickerId)
        #     if rt_price[1] >= short_entry:
        #         print("BID:", rt_price[1], "SHORT_ENTRY:", short_entry)
        #         actual_tp_sell = short_entry
        #         fetchtime = datetime.now().strftime("%Y%m%d %H:%M:%S")
        #         print("CONDITION MET FOR", params[0], signal, "at", fetchtime, "PRICE:", rt_price[1])
        #         id = int(ids[0][0])
        #         price = float(round(rt_price[1], round1))
        #         qty = (params[3])
        #         condition = "MET"
        #     else:
        #         condition = "NOT MET"

        if condition == "MET":

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

                    partial_fill = int(filled)
                    remain_qty = int(remain)
                    exit_exeprice = 0
                    round1 = 5
                    if signal == "BUY":
                        # tp_buy = tp_buy2
                        condition_buy = "NOT FULFILLED"
                        pos = 0
                    elif signal == "SELL":
                        # tp_sell = tp_sell2
                        condition_sell = "NOT FULFILLED"
                        pos = 0
                elif remain == 0:
                    entry_exeprice = confirm[confirm.orderId == id].tail(1).iloc[0]['avgFillPrice']
                    entry_exeprice = float(round((entry_exeprice), 5))
                    # print("CONDITION MET,BREAKING ")
                    if signal == "BUY":
                        condition_buy = "FULFILLED"
                        print(signal, condition_buy)
                        exetime = datetime.now().strftime("%Y:%m:%d %H:%M:%S")

                        entry_buy_list = [actual_tp_buy, entry_exeprice, filled, signal, exetime]

                        london_log = pd.read_csv("C:/LOG/log_london.csv")
                        london_log.loc[london_log.CURRENCY == params[0], ["TP_BUY"]] = str("EXECUTED")
                        london_log.to_csv("C:/LOG/log_london.csv")

                    # elif signal == "SELL":
                    #     condition_sell = "FULFILLED"
                    #     exetime = datetime.now().strftime("%Y:%m:%d %H:%M:%S")
                    #     entry_sell_list = [actual_tp_sell, entry_exeprice, filled, signal, exetime]
                    #
                    #     london_log = pd.read_csv("C:/LOG/log_london.csv")
                    #     london_log.loc[london_log.CURRENCY == params[0], ["TP_SELL"]] = str("EXECUTED")
                    #     london_log.to_csv("C:/LOG/log_london.csv")

                    dat = open_pos()
                    if (dat.empty) or (dat[(dat["Local Symbol"] == params[0])].empty) or (
                            int(dat[(dat["Local Symbol"] == params[0])].iloc[-1]['Position']) == 0):
                        # print(pos)
                        pos = 0

                    else:
                        print(params[0], " ENTRY PLACED @", filled, entry_exeprice)
                        break

    ############### end of entry ##############################################################################################
    print(params[0], " TRACKING EXIT CRITERIA ")

    ###############  exit criteria ########################################################################################
    dat = open_pos()
    if (dat.empty) or (dat[(dat["Local Symbol"] == params[0])].empty) or (
            int(dat[(dat["Local Symbol"] == params[0])].iloc[-1]['Position']) == 0):
        # print(pos)
        pos = 0
    else:
        pos = int(dat[(dat["Local Symbol"] == params[0])].iloc[-1]['Position'])
    while pos != 0:
        rt_price = ask_bid(contract, tickerId)
        data_fetch_time = datetime.now().strftime("%Y%m%d %H:%M:%S")

        if exit_signal == "BUY":

            while rt_price[0] == -1 or rt_price[1] == -1:
                negative_time = datetime.now().strftime("%H:%M:%S")
                print("!!!!!! VALID DATA NOT OBTAINED FROM IB FOR " + params[0] + negative_time + " !!!!")
                print(datetime.now().strftime("%Y%m%d %H:%M:%S"))
                tickerId = random.randint(5000, 8000)
                rt_price = ask_bid(contract, tickerId)
            if rt_price[0] <= short_exit:
                print("ASK:", rt_price[0], "SHORT_EXIT:", short_exit)
                actual_tp_buy = short_exit
                fetchtime = datetime.now().strftime("%Y%m%d %H:%M:%S")
                print("CONDITION MET FOR", params[0], exit_signal, "at", fetchtime, "PRICE:", rt_price[0])
                id = int(ids[1][0])
                price = float(round(rt_price[0], round1))
                qty = abs(pos)
                condition = "MET"
            else:
                condition = "NOT MET"


        elif exit_signal == "SELL":

            while rt_price[0] == -1 or rt_price[1] == -1:
                negative_time = datetime.now().strftime("%H:%M:%S")
                print("!!!!!! VALID DATA NOT OBTAINED FROM IB FOR " + params[0] + negative_time + " !!!!")
                print(datetime.now().strftime("%Y%m%d %H:%M:%S"))
                tickerId = random.randint(5000, 8000)
                rt_price = ask_bid(contract, tickerId)
            if rt_price[1] >= long_exit:
                print("BID:", rt_price[1], "LONG_EXIT:", long_exit)
                actual_tp_sell = long_exit
                fetchtime = datetime.now().strftime("%Y%m%d %H:%M:%S")
                print("CONDITION MET FOR", params[0], exit_signal, "at", fetchtime, "PRICE:", rt_price[1])
                id = int(ids[0][0])
                price = float(round(rt_price[1], round1))
                qty = abs(pos)
                condition = "MET"
            else:
                condition = "NOT MET"

        if condition == "MET":

            order_info = create.create_order(acc, "LMT", int(qty), exit_signal, float(price), True, 'GTD', exp_date1)
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

                    partial_fill = int(filled)
                    remain_qty = int(remain)
                    exit_exeprice = 0
                    round1 = 5
                    if exit_signal == "BUY":
                        # tp_buy = tp_buy2
                        condition_buy = "NOT FULFILLED"
                        #pos = 0
                    elif exit_signal == "SELL":
                        # tp_sell = tp_sell2
                        condition_sell = "NOT FULFILLED"
                        #pos = 0
                elif remain == 0:
                    exit_exeprice = confirm[confirm.orderId == id].tail(1).iloc[0]['avgFillPrice']
                    exit_exeprice = float(round((exit_exeprice), 5))
                    # print("CONDITION MET,BREAKING ")
                    if exit_signal == "BUY":
                        condition_buy = "FULFILLED"
                        print(exit_signal, condition_buy)
                        exetime = datetime.now().strftime("%Y:%m:%d %H:%M:%S")

                        exit_buy_list = [actual_tp_buy, exit_exeprice, filled, exit_signal, exetime]

                        london_log = pd.read_csv("C:/LOG/log_london.csv")
                        london_log.loc[london_log.CURRENCY == params[0], ["TP_BUY"]] = str("EXECUTED")
                        london_log.to_csv("C:/LOG/log_london.csv")

                    elif exit_signal == "SELL":
                        condition_sell = "FULFILLED"
                        exetime = datetime.now().strftime("%Y:%m:%d %H:%M:%S")
                        exit_sell_list = [actual_tp_sell, exit_exeprice, filled, exit_signal, exetime]

                        london_log = pd.read_csv("C:/LOG/log_london.csv")
                        london_log.loc[london_log.CURRENCY == params[0], ["TP_SELL"]] = str("EXECUTED")
                        london_log.to_csv("C:/LOG/log_london.csv")

                    dat = open_pos()
                    if (dat.empty) or (dat[(dat["Local Symbol"] == params[0])].empty) or (
                            int(dat[(dat["Local Symbol"] == params[0])].iloc[-1]['Position']) == 0):
                        # print(pos)
                        pos = 0

                    else:
                        pos = int(dat[(dat["Local Symbol"] == params[0])].iloc[-1]['Position'])

    ######## return calculation #############################################################################################
    #
    # dat['HC_Ret'] = 0
    # dat['LC_Ret'] = 0
    # dat.loc[dat['high'] > dat['long_exit'], 'HC_Ret'] = dat['long_exit'] / dat['long_entry'] - 1
    # dat.loc[dat['low'] < dat['short_exit'], 'LC_Ret'] = -1 * (dat['short_exit'] / dat['short_entry'] - 1)
    #
    # myindex = dat.loc[dat['HC_Ret'] == 0].index
    # dat['HC_Ret'].loc[myindex] = dat['close'].loc[myindex] / dat['long_entry'].loc[myindex] - 1
    #
    # myindex = dat.loc[dat['LC_Ret'] == 0].index
    # dat['LC_Ret'].loc[myindex] = -1 * (dat['close'].loc[myindex] / dat['short_entry'].loc[myindex] - 1)
    #
    # dat.loc[dat['high'] < dat['long_entry'], 'HC_Ret'] = 'NaN'
    # dat.loc[dat['low'] > dat['short_entry'], 'LC_Ret'] = 'NaN'
    #
    # dat = dat[['HC_Ret', 'LC_Ret']]
    # print(dat)

    return


def run_london():
    # currtime = datetime.now().strftime('%H:%M:%S')
    # if currtime < "13:56:00":
    #     sleeptime = (
    #     datetime.strptime("13:56:01", '%H:%M:%S') - datetime.strptime(currtime, '%H:%M:%S')).total_seconds()
    #     print("LONDON cycle is sleeping till scheduled time 13:56", sleeptime)
    #     time.sleep(sleeptime)

    # disconn()
    # conn()
    BO_list = ["EUR.CZK", "SGD.CNH", "EUR.HKD", "AUD.CNH", "EUR.USD", "CHF.PLN", "CHF.CNH", "AUD.CAD","NOK.SEK",
                   "AUD.SGD","EUR.CHF","EUR.CNH","AUD.HKD","NZD.USD","AUD.USD","USD.CNH","USD.ILS",
                   "CL",'CT',]

    BO_df = pd.DataFrame(
        {"CURRENCY": BO_list, "OPEN_POSITION": np.nan, "ENTRY_QTY": np.nan, "ENTRY_SIGNAL": np.nan, "ASK": np.nan,
         "BID": np.nan,
         "ENTRY_EXE": np.nan, "ENTRY_TIME": np.nan,
         "LONG_EXIT_QTY": np.nan, "LONG_TP": np.nan, "LONG_EXIT_EXE": np.nan, "LONG_EXIT_TIME": np.nan,
         "SHORT_EXIT_QTY": np.nan, "SHORT_TP": np.nan, "SHORT_EXIT_EXE": np.nan, "SHORT_EXIT_TIME": np.nan,
         "CLOSE": np.nan, "ACT_RETURN": np.nan, "CYCLE_DATE": np.nan})
    ############ UPDATE DAILY ####################################################
    print("&&&&&&&  CALCULATING LONDON QTY &&&&&&&&&&&&&&&&")
    log_df = pd.DataFrame(
        {"CURRENCY": BO_list, "ENTRY_FILLED": np.nan, "ENTRY_REMAIN": np.nan, "TP_BUY": np.nan, "TP_BUY2": np.nan,
         "BUY_QTY": np.nan, "TP_SELL": np.nan, "TP_SELL2": np.nan, "SELL_QTY": np.nan})
    log_df = log_df[
        ["CURRENCY", "ENTRY_FILLED", "ENTRY_REMAIN", "TP_BUY", "TP_BUY2", "BUY_QTY", "TP_SELL", "TP_SELL2", "SELL_QTY"]]
    log_df.to_csv("C:\BREAKOUT/log_BO.csv")
    ####
    df_common_inputs = pd.read_excel('C:\BREAKOUT\SETTINGS/BO_settings.xlsx', 'COMMON')
    df_BO = pd.read_excel('C:\BREAKOUT\SETTINGS/BO_settings.xlsx', 'LONG_ALLOCATION')
    # df_o_val = pd.read_excel('C:/database/FX_DAILY/BO_settings.xlsx', 'LONDON_ENTRY')
    # df_switch = pd.read_excel('C:/database/FX_DAILY/BO_settings.xlsx', 'LONDON_SWITCH')

    BO_cap = df_common_inputs["CAPITAL"].iloc[-1]
    BO_long_cap = float(df_common_inputs['LONG_ALLOC'].iloc[0]) * BO_cap
    BO_short_cap = float(df_common_inputs['SHORT_ALLOC'].iloc[0]) * BO_cap


    BO_list_wgts = [float(df_BO['EUR.CZK'].iloc[0]), float(df_BO['SGD.CNH'].iloc[0]),
                    float(df_BO['EUR.HKD'].iloc[0]), float(df_BO['AUD.CNH'].iloc[0]),
                    float(df_BO['EUR.USD'].iloc[0]), float(df_BO['CHF.PLN'].iloc[0]),
                    float(df_BO['CHF.CNH'].iloc[0]), float(df_BO['AUD.CAD'].iloc[0]),
                    float(df_BO['NOK.SEK'].iloc[0]), float(df_BO['AUD.SGD'].iloc[0]),
                    float(df_BO['EUR.CHF'].iloc[0]), float(df_BO['EUR.CNH'].iloc[0]),
                    float(df_BO['AUD.HKD'].iloc[0]), float(df_BO['NZD.USD'].iloc[0]),
                    float(df_BO['AUD.USD'].iloc[0]), float(df_BO['USD.CNH'].iloc[0]),
                    float(df_BO['USD.ILS'].iloc[0]), float(df_BO['CL'].iloc[0]),]


    # BO_ls_weights_L = []
    # BO_ls_weights_S = []

    BO_list_qty_short = []  # individual short correncies qty stored in this variable
    BO_list_qty_long = []  # individual long correncies qty stored in this variable

    ####################################################################

    ##QTY CALCULATION
    for i in range(len(BO_list)):

        ########################if cycle get delayed#####################################################
        # currtime = datetime.now().strftime('%Y%m%d %H:%M:%S')
        # week_day = datetime.now().weekday()
        #
        # if week_day != 4 and "NZD" in london_list[i]:
        #     if "13:59:55" <= currtime[-8:] <= "14:15:05":
        #         sleeptime = (datetime.strptime("14:15:10", '%H:%M:%S') - datetime.strptime(currtime[-8:],
        #                                                                                    '%H:%M:%S')).total_seconds()
        #         print(london_list[i] + " sleeping till mkt opens at 14:15")
        #         time.sleep(sleeptime)  # sleeptime
        #
        # else:
        #     if "16:59:50" <= currtime[-8:] <= "17:15:05":
        #         sleeptime = (datetime.strptime("17:15:10", '%H:%M:%S') - datetime.strptime(currtime[-8:],
        #                                                                                    '%H:%M:%S')).total_seconds()
        #         print(london_list[i] + " sleeping till mkt opens at 17:15")
        #         time.sleep(sleeptime)  # sleeptime

        ####################################################################################################

        id = tickerId() + tickerId()
        if (i <= 19):
            if (i <= 14):
                if (BO_list[i][0:3] in commonwealth_curr):
                    contract_Details1 = create.create_contract(BO_list[i][0:3], "CASH", "IDEALPRO", "USD", '', '', '','')
                    data1 = ask_bid(contract_Details1, id)
                    convprice1 = float(data1[0])
                    BO_list_qty_long.append(
                        int(math.ceil((BO_short_cap * BO_list_wgts[i] * (1 / convprice1)) / 10000) * 10000))
                else:
                    contract_Details1 = create.create_contract("USD", "CASH", "IDEALPRO", BO_list[i][0:3], '', '', '','')
                    data1 = ask_bid(contract_Details1, id)
                    convprice1 = float(data1[0])
                    BO_list_qty_long.append(
                        int(math.ceil((BO_short_cap * BO_list_wgts[i] * convprice1) / 10000) * 10000))

            elif (i==15 or i==16):
                contract_Details1 = create.create_contract("USD", "CASH", "IDEALPRO", BO_list[i][4:7], '', '', '', '')
                data1 = ask_bid(contract_Details1, id)
                convprice1 = float(data1[0])
                BO_list_qty_long.append(
                    int(math.ceil((BO_short_cap * BO_list_wgts[i] * convprice1) / 10000) * 10000))

            elif (i >16):

                    if BO_list[i]=="CL":
                        cl_mult=1000
                        contract_Details1 = create.create_contract(symbol="CL", secType="FUT",
                                                               exchange="NYMEX", currency="USD",
                                                               right=None, strike=None,
                                                               expiry="201805", multiplier=cl_mult,
                                                               tradingClass=None)
                        data1 = ask_bid(contract_Details1, id)
                        convprice1 = float(data1[1])

                        CL_qty=max(
                                (int(math.ceil(((( (BO_cap*float(df_BO['CL'].iloc[0])) / (data1[1] * int(cl_mult))) / 10000) * 10000))), 1))

                        BO_list_qty_long.append(CL_qty)

                    # if BO_list[i]=="CT":
                    #     cl_mult=1000
                    #     contract_Details1 = create.create_contract(symbol="CL", secType="FUT",
                    #                                            exchange="NYMEX", currency="USD",
                    #                                            right=None, strike=None,
                    #                                            expiry="201805", multiplier=cl_mult,
                    #                                            tradingClass=None)
                    #     data1 = ask_bid(contract_Details1, id)
                    #     convprice1 = float(data1[1])
                    #
                    #     CT_qty=max(
                    #             (int(math.ceil(((( (BO_cap*float(df_BO['CL'].iloc[0])) / (data1[1] * int(cl_mult))) / 10000) * 10000))), 1))
                    #
                    #     BO_list_qty_long.append(CT_qty)




    #### END OF QTY CALCULATION

    ##### ENTRY ORDER VALIDITY AND DISTRIBUTION
    BO_entry_excel =["EUR.CZK", "SGD.CNH", "EUR.HKD", "AUD.CNH", "EUR.USD", "CHF.PLN", "CHF.CNH", "AUD.CAD","NOK.SEK",
                   "AUD.SGD","EUR.CHF","EUR.CNH","AUD.HKD","NZD.USD","AUD.USD","USD.CNH","USD.ILS",
                   "CL"]
    
    # LONDON_ENTRY_short = []
    BO_ENTRY_long = []
    # LONDON_ENTRY_L = []
    # LONDON_ENTRY_S = []
    # for i in BO_entry_excel:
    #     pos = BO_entry_excel.index(i)
    #     fav = int(df_o_val.loc[df_o_val.TYPE == 'FAVOURABLE', i].iloc[-1])
    #     mid = int(df_o_val.loc[df_o_val.TYPE == 'MIDPOINT', i].iloc[-1])
    #     unfav = int(df_o_val.loc[df_o_val.TYPE == 'UNFAVOURABLE', i].iloc[-1])
    #     total = int(fav + mid + unfav)
    #     entry_list = [fav, mid, unfav, total]
    #     if pos <= 4:
    #         BO_ENTRY_short.append(entry_list)
    #     elif pos == 5:
    #         BO_ENTRY_long.append(entry_list)
    #     elif 9 >= pos > 5:
    #         BO_ENTRY_L.append(entry_list)
    #     elif pos > 9:
    #         BO_ENTRY_S.append(entry_list)

    ### GENERATE ENTRY ORDER PARAMS
    # short_params = []
    long_params = []
    long_short_params = []
    length = len(BO_list)
    for i in range(length):
        if i <= 17:
            signal = "BUY"
            exit_signal = "SELL"
            qty = BO_list_qty_long[i]
            # total = BO_ENTRY_short[i][3]
            # fav_time = BO_ENTRY_short[i][0]
            # mid_time = BO_ENTRY_short[i][1]
            curr_name = BO_list[i]
            list1 = [curr_name, signal,exit_signal,qty]
            # , total, fav_time, mid_time]
            long_params.append(list1)
        # elif i == 5:
        #     signal = "BUY"
        #     exit_signal = "SELL"
        #     qty = BO_list_qty_long[i - 5]
        #     total = BO_ENTRY_long[i - 5][3]
        #     fav_time = BO_ENTRY_long[i - 5][0]
        #     mid_time = BO_ENTRY_long[i - 5][1]
        #     curr_name = BO_list[i]
        #     list1 = [curr_name, signal, qty, total, fav_time, mid_time]
        #     long_params.append(list1)

    ##############################################################################
    # time.sleep(35)

    id_list = [[0], [0]]

    # df1 = int(round(df_london[["audnzd_short", "chfnok_short", "chfsek_short", "eurnok_short", "eursek_short",
    #                                "gbpnok_short", "gbpsek_short", "noksek_short"]].sum(axis=1).iloc[-1]))
    # df2 = int(round(
    #     df_london[["eurnok_long", "eurnzd_long", "gbpnok_long", "noksek_long", "eursek_long"]].sum(axis=1).iloc[-1]))
    # shortsum = int(round(df_london[[col for col in df_london.columns if 'short' in col]].sum(axis=1).iloc[-1]))
    # longsum = int(round(df_london[[col for col in df_london.columns if 'long' in col]].sum(axis=1).iloc[-1]))
    # if shortsum > 1 and longsum > 1:
    #     print("dont execute any")
    # else:
    #     # print(short_params)
    print(long_params)
    # print(long_short_params)
    EURCZK_params = long_params[0]
    contract_eurczk = create.create_contract(EURCZK_params[0][0:3], "CASH", "IDEALPRO", EURCZK_params[0][4:7], '', '','', '')
    ids = orderidfun(acc, contract_eurczk, EURCZK_params[1], id_list, 2)
    id_list = ids

    time.sleep(2)
    SGDCNH_params = long_params[1]
    contract = create.create_contract(SGDCNH_params[0][0:3], "CASH", "IDEALPRO", SGDCNH_params[0][4:7], '', '','', '')
    ids1 = orderidfun(acc, contract, SGDCNH_params[1], id_list, 2)
    id_list = ids1

    time.sleep(2)
    EURHKD_params = long_params[1]
    contract = create.create_contract(EURHKD_params[0][0:3], "CASH", "IDEALPRO", EURHKD_params[0][4:7], '', '', '', '')
    ids2 = orderidfun(acc, contract, EURHKD_params[1], id_list, 2)
    id_list = ids2

    time.sleep(2)
    AUDCNH_params = long_params[1]
    contract = create.create_contract(AUDCNH_params[0][0:3], "CASH", "IDEALPRO", AUDCNH_params[0][4:7], '', '', '', '')
    ids3 = orderidfun(acc, contract, AUDCNH_params[1], id_list, 2)
    id_list = ids3

    time.sleep(2)
    EURUSD_params = long_params[1]
    contract = create.create_contract(EURUSD_params[0][0:3], "CASH", "IDEALPRO", EURUSD_params[0][4:7], '', '', '', '')
    ids4 = orderidfun(acc, contract, EURUSD_params[1], id_list, 2)
    id_list = ids4

    time.sleep(2)
    CHFPLN_params = long_params[1]
    contract = create.create_contract(CHFPLN_params[0][0:3], "CASH", "IDEALPRO", CHFPLN_params[0][4:7], '', '', '', '')
    ids5 = orderidfun(acc, contract, CHFPLN_params[1], id_list, 2)
    id_list = ids5

    time.sleep(2)
    CHFCNH_params = long_params[1]
    contract = create.create_contract(CHFCNH_params[0][0:3], "CASH", "IDEALPRO", CHFCNH_params[0][4:7], '', '', '', '')
    ids6 = orderidfun(acc, contract, CHFCNH_params[1], id_list, 2)
    id_list = ids6

    time.sleep(2)
    AUDCAD_params = long_params[1]
    contract = create.create_contract(AUDCAD_params[0][0:3], "CASH", "IDEALPRO", AUDCAD_params[0][4:7], '', '', '', '')
    ids7 = orderidfun(acc, contract, AUDCAD_params[1], id_list, 2)
    id_list = ids7

    time.sleep(2)
    NOKSEK_params = long_params[1]
    contract = create.create_contract(NOKSEK_params[0][0:3], "CASH", "IDEALPRO", NOKSEK_params[0][4:7], '', '', '', '')
    ids8 = orderidfun(acc, contract, NOKSEK_params[1], id_list, 2)
    id_list = ids8

    time.sleep(2)
    AUDSGD_params = long_params[1]
    contract = create.create_contract(AUDSGD_params[0][0:3], "CASH", "IDEALPRO", AUDSGD_params[0][4:7], '', '', '', '')
    ids9 = orderidfun(acc, contract, AUDSGD_params[1], id_list, 2)
    id_list = ids9

    time.sleep(2)
    EURHUF_params = long_params[1]
    contract = create.create_contract(EURHUF_params[0][0:3], "CASH", "IDEALPRO", EURHUF_params[0][4:7], '', '', '', '')
    ids10 = orderidfun(acc, contract, EURHUF_params[1], id_list, 2)
    id_list = ids10

    time.sleep(2)
    EURCNH_params = long_params[1]
    contract = create.create_contract(EURCNH_params[0][0:3], "CASH", "IDEALPRO", EURCNH_params[0][4:7], '', '', '', '')
    ids11 = orderidfun(acc, contract, EURCNH_params[1], id_list, 2)
    id_list = ids11

    time.sleep(2)
    AUDHKD_params = long_params[1]
    contract = create.create_contract(AUDHKD_params[0][0:3], "CASH", "IDEALPRO", AUDHKD_params[0][4:7], '', '', '', '')
    ids12 = orderidfun(acc, contract, AUDHKD_params[1], id_list, 2)
    id_list = ids12

    time.sleep(2)
    NZDUSD_params = long_params[1]
    contract = create.create_contract(NZDUSD_params[0][0:3], "CASH", "IDEALPRO", NZDUSD_params[0][4:7], '', '', '', '')
    ids13 = orderidfun(acc, contract, NZDUSD_params[1], id_list, 2)
    id_list = ids13

    time.sleep(2)
    AUDUSD_params = long_params[1]
    contract = create.create_contract(AUDUSD_params[0][0:3], "CASH", "IDEALPRO", AUDUSD_params[0][4:7], '', '', '', '')
    ids14 = orderidfun(acc, contract, AUDUSD_params[1], id_list, 2)
    id_list = ids14

    USDCNH_params = long_params[1]
    contract = create.create_contract(USDCNH_params[0][0:3], "CASH", "IDEALPRO", USDCNH_params[0][4:7], '', '', '', '')
    ids15 = orderidfun(acc, contract, USDCNH_params[1], id_list, 2)
    id_list = ids15

    USDILS_params = long_params[1]
    contract = create.create_contract(USDILS_params[0][0:3], "CASH", "IDEALPRO", USDILS_params[0][4:7], '', '', '', '')
    ids16 = orderidfun(acc, contract, USDILS_params[1], id_list, 2)
    id_list = ids16

    CL_params = long_params[1]
    contract = create.create_contract('CL', "FUT", "NYMEX", 'USD', '', '', '201805', cl_mult,'')
    ids17 = orderidfun(acc, contract, CL_params[1], id_list, 2)
    id_list = ids17



    #############rewuesting for position#####################
    dat = open_pos()
    dat.to_csv("C:/LOG/openpos_LDN.csv")
    ########################################################

    Thread(target=LONDON,
           args=(contract_eurczk,dat, EURCZK_params, ids, 100, 200, BO_df, long_params[0][1], long_params[0][2])).start()
    Thread(target=LONDON,
           args=(contract_eurczk, dat, SGDCNH_params, ids, 100, 200, BO_df, long_params[0][1], long_params[0][2])).start()
    Thread(target=LONDON,
           args=(contract_eurczk, dat, EURHKD_params, ids, 100, 200, BO_df, long_params[0][1], long_params[0][2])).start()
    Thread(target=LONDON,
           args=(contract_eurczk, dat, AUDCNH_params, ids, 100, 200, BO_df, long_params[0][1], long_params[0][2])).start()
    Thread(target=LONDON,
           args=(contract_eurczk, dat, EURUSD_params, ids, 100, 200, BO_df, long_params[0][1], long_params[0][2])).start()
    Thread(target=LONDON,
           args=(contract_eurczk, dat, CHFPLN_params, ids, 100, 200, BO_df, long_params[0][1], long_params[0][2])).start()
    Thread(target=LONDON,
           args=(contract_eurczk, dat, CHFCNH_params, ids, 100, 200, BO_df, long_params[0][1], long_params[0][2])).start()
    Thread(target=LONDON,
           args=(contract_eurczk, dat, AUDCAD_params, ids, 100, 200, BO_df, long_params[0][1], long_params[0][2])).start()
    Thread(target=LONDON,
           args=(contract_eurczk, dat, NOKSEK_params, ids, 100, 200, BO_df, long_params[0][1], long_params[0][2])).start()
    Thread(target=LONDON,
           args=(contract_eurczk, dat, AUDSGD_params, ids, 100, 200, BO_df, long_params[0][1], long_params[0][2])).start()
    Thread(target=LONDON,
           args=(contract_eurczk, dat, EURHUF_params, ids, 100, 200, BO_df, long_params[0][1], long_params[0][2])).start()
    Thread(target=LONDON,
           args=(contract_eurczk, dat, EURCNH_params, ids, 100, 200, BO_df, long_params[0][1], long_params[0][2])).start()
    Thread(target=LONDON,
           args=(contract_eurczk, dat, AUDHKD_params, ids, 100, 200, BO_df, long_params[0][1], long_params[0][2])).start()
    Thread(target=LONDON,
           args=(contract_eurczk, dat, NZDUSD_params, ids, 100, 200, BO_df, long_params[0][1], long_params[0][2])).start()
    Thread(target=LONDON,
           args=(contract_eurczk, dat, AUDUSD_params, ids, 100, 200, BO_df, long_params[0][1], long_params[0][2])).start()
    Thread(target=LONDON,
           args=(contract_eurczk, dat, USDCNH_params, ids, 100, 200, BO_df, long_params[0][1], long_params[0][2])).start()
    Thread(target=LONDON,
           args=(contract_eurczk, dat, USDILS_params, ids, 100, 200, BO_df, long_params[0][1], long_params[0][2])).start()
    Thread(target=LONDON,
           args=(contract_eurczk, dat, CL_params, ids, 100, 200, BO_df, long_params[0][1], long_params[0][2])).start()

    # ################################################
    # Thread(target=LONDON, args=(contract_Details1,dat, SGDCNH_params, ids1, 300, 400, BO_df,long_params[1][1], long_params[1][2])).start()
    # ###############################################
    # switchflag2 = df_switch.loc[df_switch.CURRENCY == 'CHF.SEK', 'TRADE'].iloc[-1]
    # if (0 <= df_switch.loc[df_switch.CURRENCY == 'CHF.SEK', 'TP1'].iloc[-1] <= 0.25) and \
    #         (0 <= df_switch.loc[df_switch.CURRENCY == 'CHF.SEK', 'TP2'].iloc[-1] <= 0.25) and (
    #         switchflag2 in ('yes', 'YES')):
    #     Thread(target=LONDON, args=(dat,CHFSEK_params, ids2, 500, 600, london_df)).start()
    # ##########################################################
    # switchflag3 = df_switch.loc[df_switch.CURRENCY == 'NZD.CHF', 'TRADE'].iloc[-1]
    # if (0 <= df_switch.loc[df_switch.CURRENCY == 'NZD.CHF', 'TP1'].iloc[-1] <= 0.25) and \
    #         (0 <= df_switch.loc[df_switch.CURRENCY == 'NZD.CHF', 'TP2'].iloc[-1] <= 0.25) and (
    #         switchflag3 in ('yes', 'YES')):
    #     Thread(target=LONDON, args=(dat,NZDCHF_params, ids3, 700, 800, london_df)).start()
    #
    # ############################################################################
    # switchflag4 = df_switch.loc[df_switch.CURRENCY == 'GBP.SEK', 'TRADE'].iloc[-1]
    # if (0 <= df_switch.loc[df_switch.CURRENCY == 'GBP.SEK', 'TP1'].iloc[-1] <= 0.25) and \
    #         (0 <= df_switch.loc[df_switch.CURRENCY == 'GBP.SEK', 'TP2'].iloc[-1] <= 0.25) and (
    #         switchflag4 in ('yes', 'YES')):
    #     Thread(target=LONDON, args=(dat,GBPSEK_params, ids4, 900, 1000, london_df)).start()
    # ###############################################################
    #
    # switchflag5 = df_switch.loc[df_switch.CURRENCY == 'EUR.NZD', 'TRADE'].iloc[-1]
    # if (0 <= df_switch.loc[df_switch.CURRENCY == 'EUR.NZD', 'TP1'].iloc[-1] <= 0.25) and \
    #         (0 <= df_switch.loc[df_switch.CURRENCY == 'EUR.NZD', 'TP2'].iloc[-1] <= 0.25) and (
    #         switchflag5 in ('yes', 'YES')):
    #     Thread(target=LONDON, args=(dat,EURNZD_params, ids5, 1100, 1200, london_df)).start()
    #
    # ##########################################################################
    #
    # switchflag6 = df_switch.loc[df_switch.CURRENCY == 'EUR.NOK', 'TRADE'].iloc[-1]
    # if (0 <= df_switch.loc[df_switch.CURRENCY == 'EUR.NOK', 'TP1'].iloc[-1] <= 0.25) and \
    #         (0 <= df_switch.loc[df_switch.CURRENCY == 'EUR.NOK', 'TP2'].iloc[-1] <= 0.25) and (
    #         switchflag6 in ('yes', 'YES')):
    #     Thread(target=LONDON, args=(dat,EURNOK_params, ids6, 1200, 1300, london_df)).start()
    # ##############################################
    #
    # switchflag7 = df_switch.loc[df_switch.CURRENCY == 'EUR.SEK', 'TRADE'].iloc[-1]
    # if (0 <= df_switch.loc[df_switch.CURRENCY == 'EUR.SEK', 'TP1'].iloc[-1] <= 0.25) and \
    #         (0 <= df_switch.loc[df_switch.CURRENCY == 'EUR.SEK', 'TP2'].iloc[-1] <= 0.25) and (
    #         switchflag7 in ('yes', 'YES')):
    #     Thread(target=LONDON, args=(dat,EURSEK_params, ids7, 1400, 1500, london_df)).start()
    # ##################################################################
    # switchflag8 = df_switch.loc[df_switch.CURRENCY == 'GBP.NOK', 'TRADE'].iloc[-1]
    # if (0 <= df_switch.loc[df_switch.CURRENCY == 'GBP.NOK', 'TP1'].iloc[-1] <= 0.25) and \
    #         (0 <= df_switch.loc[df_switch.CURRENCY == 'GBP.NOK', 'TP2'].iloc[-1] <= 0.25) and (
    #         switchflag8 in ('yes', 'YES')):
    #     Thread(target=LONDON, args=(dat,GBPNOK_params, ids8, 1600, 1700, london_df)).start()
    #
    # ##########################################################
    # switchflag9 = df_switch.loc[df_switch.CURRENCY == 'NOK.SEK', 'TRADE'].iloc[-1]
    # if (0 <= df_switch.loc[df_switch.CURRENCY == 'NOK.SEK', 'TP1'].iloc[-1] <= 0.25) and \
    #         (0 <= df_switch.loc[df_switch.CURRENCY == 'NOK.SEK', 'TP2'].iloc[-1] <= 0.25) and (
    #         switchflag9 in ('yes', 'YES')):
    #     Thread(target=LONDON, args=(dat,NOKSEK_params, ids9, 1800, 1900, london_df)).start()
    ########################
    return


# run_london()