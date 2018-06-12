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
port = 7496
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
    attempts = [0]
    while ask == 1 and bid == 1:
        # sleeptime=0.4

        conn()
        tickedId = int(tickerId + 1)
        tick_data1 = pd.DataFrame()
        while tick_data1.empty:
            instance = attempts[-1]
            if instance > 4:
                conn()
                print("ATTEMPTING HARDER:", instance)
                tickedId = int(tickerId + 1)
                tws.reqMktData(tickedId, tick, "", 1)
                # print("ask/bid unavailable at the moment")
                sleeptime = 1
                time.sleep(sleeptime)
            else:
                conn()
                tickedId = int(tickerId + 1)
                tws.reqMktData(tickedId, tick, "", 1)
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


def orderidfun(acc, contract, type, lastidlist, loops):

    contract_info = contract
    buyidlist = generateNumber(loops)

    while buyidlist[0] == lastidlist[-1][0]:
        buyidlist = generateNumber(loops)

    for i in buyidlist:
        order_info = create.create_order(acc, "LMT", 10000, "BUY", 10.000, False, False, None)
        tws.placeOrder(int(i), contract_info, order_info)

    sellidlist = generateNumber(loops)
    while sellidlist[0] == buyidlist[0]:
        sellidlist = generateNumber(loops)
    for i in sellidlist:
        order_info = create.create_order(acc, "LMT", 10000, "SELL", 10.000, False, False, None)
        tws.placeOrder(int(i), contract_info, order_info)

    return buyidlist, sellidlist


BO_list = ["EUR.CZK", "SGD.CNH", "EUR.HKD", "AUD.CNH", "EUR.USD", "CHF.PLN", "CHF.CNH", "AUD.CAD", "NOK.SEK",
           "AUD.SGD", "EUR.CHF", "EUR.CNH", "AUD.HKD", "NZD.USD", "AUD.USD", "USD.CNH", "USD.ILS"]

BO_df = pd.DataFrame(
    {"CYCLE_DATE": np.nan, "CURRENCY": BO_list, "OPEN_POSITION": np.nan, "CURRENT_OPEN": np.nan,"PREV_DAY_CLOSE":np.nan,
     "CURRENT_CLOSE": np.nan, "ASK": np.nan, "BID": np.nan,
     "ENTRY_QTY": np.nan, "ENTRY_SIGNAL": np.nan, "ENTRY TP": np.nan, "ENTRY_EXE": np.nan, "ENTRY_TYPE": np.nan,
     "ENTRY_TIME": np.nan, "EXIT_QTY": np.nan, "EXIT_SIGNAL": np.nan, "EXIT_TP1": np.nan,"EXIT_TP2": np.nan,"EXIT_EXE": np.nan,
     "EXIT_TIME": np.nan})


def bo_report(BO_df):
    time.sleep(600)
    report_df = BO_df[['CURRENCY', "ENTRY_SIGNAL","ENTRY_QTY", "ENTRY TP", "EXIT_SIGNAL", "EXIT_QTY", "EXIT_TP1", "EXIT_TP2"]]
    print("REPORT")
    print(report_df)
    report_df.to_csv("C:/LOG/BO_LOG.csv", )
    return

def BO_NY(contract, dat, params, ids, tickerId, histId, BO_df, signal, exit_signal):

    cycledate = datetime.now().strftime("%Y%m%d")
    print("SHAPE", BO_df.shape)
    ################# initialisation ####################
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
    exit_sell_list = ['null', "null", "null", "null", "null"]
    ask=0
    bid=0
    qty = 0
    price = 0
    actual_tp_sell=0

    df_switch_bo = pd.read_excel('C:/database/FX_DAILY/BO_settings.xlsx', 'BO_SWITCH')
    df_gtd = pd.read_excel('C:/database/FX_DAILY/BO_settings.xlsx', 'EXIT_GTD')
    df_common_inputs = pd.read_excel('C:/database/FX_DAILY/BO_settings.xlsx', 'BO_COMMON')
    closetime = df_common_inputs["CLOSE TIME"].iloc[-1].strftime("%H:%M:%S")
    time_ldn = " " + closetime
    closetime = datetime.strptime(closetime, "%H:%M:%S")
    lookback1 = int(df_common_inputs["LOOKBACK"].iloc[-1])
    lookback = lookback1

    entry_thresh = float(df_switch_bo.loc[df_switch_bo.CURRENCY == params[0], 'TP_ENTRY'].iloc[-1])
    exit_thresh1 = float(df_switch_bo.loc[df_switch_bo.CURRENCY == params[0], 'TP_EXIT1'].iloc[-1])
    exit_thresh2 = float(df_switch_bo.loc[df_switch_bo.CURRENCY == params[0], 'TP_EXIT2'].iloc[-1])

    lag_val = 1
    MA_val = 20

    # entry_thresh = 0.001
    # exit_thresh1 = 0.005
    # exit_thresh2 = 0.004
    ##########################################################
    # contract = create.create_contract(params[0][0:3], "CASH", "IDEALPRO", params[0][4:7], '', '', '', '')
    size=0
    while size <= 22:
        asset_hist_data = pd.DataFrame()
        asset_hist_data = historical(time_ldn, contract, "26 D", histId)
        size=len(asset_hist_data)
        print("size of hist data",size)

    # asset_hist_data = asset_hist_data[:-1]

    open_data = asset_hist_data["open"].iloc[-1]
    open_data = round(float(open_data), 5)

    prev_close_data = asset_hist_data["close"].iloc[-2]
    prev_close_data = round(float(prev_close_data), 5)

    close_data = asset_hist_data["close"].iloc[-1]
    close_data = round(float(close_data), 5)

    asset_hist_data = asset_hist_data[["open", "high", "low", "close"]]
    # print(asset_hist_data)

    dat = asset_hist_data
    # dat=dat.tail(22)
    dat['temp'] = dat['close'].shift(lag_val)

    dat['entry_HC_Spread'] = (
            abs((dat['high'] / dat['close'].shift(lag_val) - 1).rolling(MA_val).mean()) * entry_thresh).shift(1)
    dat['exit_HC_Spread1'] = (
            abs((dat['high'] / dat['close'].shift(lag_val) - 1).rolling(MA_val).mean()) * exit_thresh1).shift(1)
    dat['exit_HC_Spread2'] = (
            abs((dat['high'] / dat['close'].shift(lag_val) - 1).rolling(MA_val).mean()) * exit_thresh2).shift(1)

    # dat['exit_LC_Spread1'] = (
    #         abs((dat['low'] / dat['close'].shift(lag_val) - 1).rolling(MA_val).mean()) * exit_thresh1).shift(1)
    # dat['exit_LC_Spread2'] = (
    #         abs((dat['low'] / dat['close'].shift(lag_val) - 1).rolling(MA_val).mean()) * exit_thresh2).shift(1)


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


    #print("BUY ENTRY:", buy_entry, "SELL EXIT1:", sell_exit1, "SELL EXIT2:", sell_exit2)


    ################################log data################################################################################
    log_data = [params, buy_entry, sell_exit1,sell_exit2]

    print("")
    print("%%%%%%%   LOG FOR ", params[0], " %%%%%%%")
    print("PARAMS:",log_data[0],"ENTRY:",log_data[1],"EXIT1:",log_data[2],"EXIT2:",log_data[3])
    print("%%%%%%%%%%%%%%%%%%%%%%%%")
    print("")
    ############################################################################################
    BO_df.loc[BO_df.CURRENCY == params[0], "CYCLE_DATE"] = cycledate
    BO_df.loc[BO_df.CURRENCY == params[0], "PREV_DAY_CLOSE"] = prev_close_data
    BO_df.loc[BO_df.CURRENCY == params[0], "CURRENT_OPEN"] = open_data
    BO_df.loc[BO_df.CURRENCY == params[0], "CURRENT_CLOSE"] = close_data
    BO_df.loc[BO_df.CURRENCY == params[0], "ENTRY_QTY"] = params[-1]
    BO_df.loc[BO_df.CURRENCY == params[0], "ENTRY_SIGNAL"] = signal
    BO_df.loc[BO_df.CURRENCY == params[0], "ENTRY_TP"] = buy_entry
    BO_df.loc[BO_df.CURRENCY == params[0], "EXIT_TP1"] = sell_exit1
    BO_df.loc[BO_df.CURRENCY == params[0], "EXIT_TP2"] = sell_exit2

    ########################################GTD EXPDATE########################################################################
    exp_time = str(df_common_inputs["GTD"].iloc[-1])
    week_day = datetime.now().weekday()

    if (week_day == 4):

        num = int(df_gtd.loc[df_gtd.MKT == 'NY', "FRIDAY"].iloc[-1])
        exp_date1 = datetime.now() + timedelta(days=num)  ##changed for christmas&new year holiday
        exp_date1 = exp_date1.strftime('%Y%m%d') + " " + exp_time


    elif week_day == 5:
        num = int(df_gtd.loc[df_gtd.MKT == 'NY', "SATURDAY"].iloc[-1]) - 1
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

    ########################### for entry ##############################################################################
    print(params[0], "WAITING TO PLACE ", signal, params[3])
    pos = 0
    while pos == 0:
        conn()

        ###############################regualr mkt close###########################################################################
        currtime = datetime.now().strftime("%Y%m%d %H:%M:%S")
        week_day = datetime.now().weekday()

        if week_day == 5 and currtime[-8:] >= "02:29:50":
            wakeuptime = ((datetime.now() + timedelta(+2)).strftime("%Y%m%d")) + ' 02:45:10'
            sleeptime = (datetime.strptime(wakeuptime, "%Y%m%d %H:%M:%S") - datetime.strptime(currtime,
                                                                                              "%Y%m%d %H:%M:%S")).total_seconds()
            print(params[0], 'sleeping' + "and wakesup at " + wakeuptime)

            time.sleep(sleeptime)
            # fetchtime = datetime.now().strftime("%Y%m%d %H:%M:%S")
            print(params[0], " activated at ", wakeuptime)
        else:
            if week_day != 5 and "NZD" in params[0]:
                if "00:29:50" <= currtime[-8:] <= "00:45:05":
                    sleeptime = (
                            datetime.strptime("00:45:10", '%H:%M:%S') - datetime.strptime(currtime[-8:],
                                                                                          '%H:%M:%S')).total_seconds()
                    print(params[0], "sleeping till mkt opens at 00:45")
                    time.sleep(sleeptime)
                    fetchtime = datetime.now().strftime("%Y%m%d %H:%M:%S")
                    print(params[0], " activated at", fetchtime)


            else:
                if "02:29:50" <= currtime[-8:] <= "02:45:05":
                    sleeptime = (
                            datetime.strptime("02:45:10", '%H:%M:%S') - datetime.strptime(currtime[-8:],
                                                                                          '%H:%M:%S')).total_seconds()
                    print(params[0], "sleeping till mkt opens at 02:45")

                    time.sleep(sleeptime)
                    fetchtime = datetime.now().strftime("%Y%m%d %H:%M:%S")
                    print(params[0], " activated at", fetchtime)

        #########################################################################


        rt_price = ask_bid(contract, tickerId)
        data_fetch_time = datetime.now().strftime("%Y%m%d %H:%M:%S")

        if signal == "BUY":

            while rt_price[0] == -1 or rt_price[1] == -1:
                negative_time = datetime.now().strftime("%H:%M:%S")
                print("!!!!!! VALID DATA NOT OBTAINED FROM IB FOR " + params[0] + negative_time + " !!!!")
                print(datetime.now().strftime("%Y%m%d %H:%M:%S"))
                tickerId = random.randint(5000, 8000)
                rt_price = ask_bid(contract, tickerId)

            if rt_price[0] <= buy_entry:
                # print("ASK:", rt_price[0], "BUY_ENTRY:", buy_entry)
                actual_tp_buy = buy_entry
                fetchtime = datetime.now().strftime("%Y%m%d %H:%M:%S")
                print("CONDITION MET FOR", params[0], signal, "at", fetchtime, "PRICE:", rt_price[0])
                id = int(ids[0][0])
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
                        ask = rt_price[0]
                        bid = rt_price[1]
                        entry_buy_list = [actual_tp_buy, entry_exeprice, filled, signal, exetime]

                        BO_df.loc[BO_df.CURRENCY == params[0], "ASK"] = ask
                        BO_df.loc[BO_df.CURRENCY == params[0], "BID"] = bid
                        BO_df.loc[BO_df.CURRENCY == params[0], "ENTRY_EXE"] = entry_buy_list[1]
                        # BO_df.loc[BO_df.CURRENCY == params[0], "ENTRY_TYPE"] = entry_buy_list[3]
                        BO_df.loc[BO_df.CURRENCY == params[0], "ENTRY_TIME"] = entry_buy_list[-1]
                        BO_df.loc[BO_df.CURRENCY == params[0], "EXIT_QTY"] = entry_buy_list[2]



                    # elif signal == "SELL":
                    #     condition_sell = "FULFILLED"
                    #     exetime = datetime.now().strftime("%Y:%m:%d %H:%M:%S")
                    #     entry_sell_list = [actual_tp_sell, entry_exeprice, filled, signal, exetime]
                    #
                    #     london_log = pd.read_csv("C:/LOG/log_london.csv")
                    #     london_log.loc[london_log.CURRENCY == params[0], ["TP_SELL"]] = str("EXECUTED")
                    #     london_log.to_csv("C:/LOG/log_london.csv")

                    dat = open_pos()
                    if params[0] == 'CL':
                        if (dat.empty) or (dat[(dat["Symbol"] == params[0])].empty) or (
                                int(dat[(dat["Symbol"] == params[0])].iloc[-1]['Position']) == 0):
                            # print(pos)
                            pos = 0

                        else:
                            pos = int(dat[(dat["Symbol"] == params[0])].iloc[-1]['Position'])
                            print(params[0], " ENTRY PLACED @", filled, entry_exeprice)
                    else:
                        if (dat.empty) or (dat[(dat["Local Symbol"] == params[0])].empty) or (
                                int(dat[(dat["Local Symbol"] == params[0])].iloc[-1]['Position']) == 0):
                            # print(pos)
                            pos = 0

                        else:
                            pos = int(dat[(dat["Local Symbol"] == params[0])].iloc[-1]['Position'])
                            print(params[0], " ENTRY PLACED @", filled, entry_exeprice)



    print(params[0], entry_buy_list)
    ############### end of entry ##############################################################################################
    print(params[0], " TRACKING EXIT CRITERIA ")

    ###############  exit criteria ########################################################################################
    dat = open_pos()
    if params[0] == "CL":
        if (dat.empty) or (dat[(dat["Symbol"] == params[0])].empty) or (
                int(dat[(dat["Symbol"] == params[0])].iloc[-1]['Position']) == 0):
            # print(pos)
            pos = 0
        else:
            # print(pos)
            pos = int(dat[(dat["Symbol"] == params[0])].iloc[-1]['Position'])
    else:
        if (dat.empty) or (dat[(dat["Local Symbol"] == params[0])].empty) or (
                int(dat[(dat["Local Symbol"] == params[0])].iloc[-1]['Position']) == 0):
            # print(pos)
            print(dat)
            pos = 0
        else:
            pos = int(dat[(dat["Local Symbol"] == params[0])].iloc[-1]['Position'])
            print("position after entry for ",params[0],pos)

    condition = "NOT MET"
    while pos != 0:
        conn()

        ##################################################################################
        currtime = datetime.now().strftime("%Y%m%d %H:%M:%S")
        week_day = datetime.now().weekday()

        if week_day == 5 and currtime[-8:] >= "02:29:50":
            wakeuptime = ((datetime.now() + timedelta(+2)).strftime("%Y%m%d")) + ' 02:45:10'
            sleeptime = (datetime.strptime(wakeuptime, "%Y%m%d %H:%M:%S") - datetime.strptime(currtime,
                                                                                              "%Y%m%d %H:%M:%S")).total_seconds()
            print(params[0], 'sleeping' + "and wakesup at " + wakeuptime)

            time.sleep(sleeptime)
            # fetchtime = datetime.now().strftime("%Y%m%d %H:%M:%S")
            print(params[0], " activated at ", wakeuptime)
        else:
            if week_day != 5 and "NZD" in params[0]:
                if "00:29:50" <= currtime[-8:] <= "00:45:05":
                    sleeptime = (
                            datetime.strptime("00:45:10", '%H:%M:%S') - datetime.strptime(currtime[-8:],
                                                                                          '%H:%M:%S')).total_seconds()
                    print(params[0], "sleeping till mkt opens at 00:45")
                    time.sleep(sleeptime)
                    fetchtime = datetime.now().strftime("%Y%m%d %H:%M:%S")
                    print(params[0], " activated at", fetchtime)
            else:
                if "02:29:50" <= currtime[-8:] <= "02:45:05":
                    sleeptime = (
                            datetime.strptime("02:45:10", '%H:%M:%S') - datetime.strptime(currtime[-8:],
                                                                                          '%H:%M:%S')).total_seconds()
                    print(params[0], "sleeping till mkt opens at 02:45")

                    time.sleep(sleeptime)
                    fetchtime = datetime.now().strftime("%Y%m%d %H:%M:%S")
                    print(params[0], " activated at", fetchtime)


        #############################################################################################################
        rt_price = ask_bid(contract, tickerId)
        data_fetch_time = datetime.now().strftime("%Y%m%d %H:%M:%S")

        # if exit_signal == "BUY":
        #
        #     while rt_price[0] == -1 or rt_price[1] == -1:
        #         negative_time = datetime.now().strftime("%H:%M:%S")
        #         print("!!!!!! VALID DATA NOT OBTAINED FROM IB FOR " + params[0] + negative_time + " !!!!")
        #         print(datetime.now().strftime("%Y%m%d %H:%M:%S"))
        #         tickerId = random.randint(5000, 8000)
        #         rt_price = ask_bid(contract, tickerId)
        #     if rt_price[0] <= short_exit:
        #         print("ASK:", rt_price[0], "LONG_EXIT:", long_exit)
        #         actual_tp_buy = short_exit
        #         fetchtime = datetime.now().strftime("%Y%m%d %H:%M:%S")
        #         print("CONDITION MET FOR", params[0], exit_signal, "at", fetchtime, "PRICE:", rt_price[0])
        #         id = int(ids[1][0])
        #         price = float(round(rt_price[0], round1))
        #         qty = abs(pos)
        #         condition = "MET"
        #     else:
        #         condition = "NOT MET"

        if exit_signal == "SELL":

            while rt_price[0] == -1 or rt_price[1] == -1:
                negative_time = datetime.now().strftime("%H:%M:%S")
                print("!!!!!! VALID DATA NOT OBTAINED FROM IB FOR " + params[0] + negative_time + " !!!!")
                print(datetime.now().strftime("%Y%m%d %H:%M:%S"))
                tickerId = random.randint(9000, 10000)
                rt_price = ask_bid(contract, tickerId)
            curr_time = datetime.now().strftime("%H:%M:%S")
            if (rt_price[2] >= sell_exit1 and rt_price[1] >= sell_exit2) or ("00:15:00" <= curr_time <= "00:28:50"):
                # print("BID:", rt_price[1], "SELL_EXIT:", sell_exit)
                actual_tp_sell = sell_exit1
                fetchtime = datetime.now().strftime("%Y%m%d %H:%M:%S")
                print("CONDITION MET FOR", params[0], exit_signal, "at", fetchtime, "PRICE:", rt_price[1])
                id = int(ids[1][0])
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
                        sell_exit1 = sell_exit2
                        condition_buy = "NOT FULFILLED"
                        # pos = 0
                    elif exit_signal == "SELL":
                        # tp_sell = tp_sell2
                        condition_sell = "NOT FULFILLED"
                        # pos = 0
                elif remain == 0:
                    exit_exeprice = confirm[confirm.orderId == id].tail(1).iloc[0]['avgFillPrice']
                    exit_exeprice = float(round((exit_exeprice), 5))
                    # print("CONDITION MET,BREAKING ")
                    if exit_signal == "BUY":
                        condition_buy = "FULFILLED"
                        print(exit_signal, condition_buy)
                        exetime = datetime.now().strftime("%Y:%m:%d %H:%M:%S")

                        exit_buy_list = [actual_tp_buy, exit_exeprice, filled, exit_signal, exetime]



                    elif exit_signal == "SELL":
                        condition_sell = "FULFILLED"
                        exetime = datetime.now().strftime("%Y:%m:%d %H:%M:%S")
                        exit_sell_list = [actual_tp_sell, exit_exeprice, filled, exit_signal, exetime]

                    BO_df.loc[BO_df.CURRENCY == params[0], "EXIT_SIGNAL"] = exit_signal
                    BO_df.loc[BO_df.CURRENCY == params[0], "EXIT_EXE"] = exit_sell_list[1]
                    BO_df.loc[BO_df.CURRENCY == params[0], "EXIT_TIME"] = exit_sell_list[-1]

                    dat = open_pos()
                    if params[0] == 'CL':
                        if (dat.empty) or (dat[(dat["Symbol"] == params[0])].empty) or (
                                int(dat[(dat["Symbol"] == params[0])].iloc[-1]['Position']) == 0):
                            # print(pos)
                            pos = 0
                            print(params[0], "HAS BEEN EXECUTED")
                            print(exit_sell_list)
                        else:
                            pos = int(dat[(dat["Symbol"] == params[0])].iloc[-1]['Position'])
                    else:
                        if (dat.empty) or (dat[(dat["Local Symbol"] == params[0])].empty) or (
                                int(dat[(dat["Local Symbol"] == params[0])].iloc[-1]['Position']) == 0):
                            # print(pos)
                            pos = 0
                            print(params[0], "HAS BEEN EXECUTED")
                            print(exit_sell_list)
                        else:
                            pos = int(dat[(dat["Local Symbol"] == params[0])].iloc[-1]['Position'])




    BO_df = BO_df[["CYCLE_DATE", "CURRENCY", "OPEN_POSITION","PREV_DAY_CLOSE","CURRENT_OPEN","CURRENT_CLOSE",
                   "ENTRY_SIGNAL", "ENTRY_QTY", "ENTRY_TYPE", "ENTRY_TP","ASK", "BID","ENTRY_EXE","ENTRY_TIME",
                   "EXIT_SIGNAL", "EXIT_QTY", "EXIT_TP1","EXIT_TP2", "EXIT_EXE", "EXIT_TIME"]]

    path = "C:\REPORTS/" + cycledate + "_BO.xlsx"
    writer = pd.ExcelWriter(path)
    BO_df.to_excel(writer, 'BO')
    writer.save()



    return


def run_bo():

    currtime = datetime.now().strftime('%H:%M:%S')
    # if currtime < "00:27:00":
    #     sleeptime = (
    #                 datetime.strptime("00:27:01", '%H:%M:%S') - datetime.strptime(currtime, '%H:%M:%S')).total_seconds()
    #     print("BO cycle is sleeping till scheduled time 00:27", sleeptime)
    #     time.sleep(sleeptime)

    BO_list = ["EUR.CZK", "SGD.CNH", "EUR.HKD", "AUD.CNH", "EUR.USD", "CHF.PLN", "CHF.CNH", "AUD.CAD", "NOK.SEK",
               "AUD.SGD", "EUR.CHF", "EUR.CNH", "AUD.HKD", "NZD.USD", "AUD.USD", "USD.CNH", "USD.ILS"]

    BO_df = pd.DataFrame(
        {"CYCLE_DATE": np.nan, "CURRENCY": BO_list, "OPEN_POSITION": np.nan, "CURRENT_OPEN": np.nan,
         "PREV_DAY_CLOSE": np.nan,
         "CURRENT_CLOSE": np.nan, "ASK": np.nan, "BID": np.nan,
         "ENTRY_QTY": np.nan, "ENTRY_SIGNAL": np.nan, "ENTRY TP": np.nan, "ENTRY_EXE": np.nan, "ENTRY_TYPE": np.nan,
         "ENTRY_TIME": np.nan, "EXIT_QTY": np.nan, "EXIT_SIGNAL": np.nan, "EXIT_TP1": np.nan, "EXIT_TP2": np.nan,
         "EXIT_EXE": np.nan,
         "EXIT_TIME": np.nan})

    ############ UPDATE DAILY ####################################################
    print("&&&&&&&  CALCULATING LONDON QTY &&&&&&&&&&&&&&&&")
    # log_df = pd.DataFrame(
    #     {"CURRENCY": BO_list, "ENTRY_FILLED": np.nan, "ENTRY_REMAIN": np.nan, "TP_BUY": np.nan, "TP_BUY2": np.nan,
    #      "BUY_QTY": np.nan, "TP_SELL": np.nan, "TP_SELL2": np.nan, "SELL_QTY": np.nan})
    # log_df = log_df[
    #     ["CURRENCY", "ENTRY_FILLED", "ENTRY_REMAIN", "TP_BUY", "TP_BUY2", "BUY_QTY", "TP_SELL", "TP_SELL2", "SELL_QTY"]]
    # log_df.to_csv("C:/databse/BREAKOUT/log_BO.csv")
    ####
    df_common_inputs = pd.read_excel('C:/database/FX_DAILY/BO_settings.xlsx', 'BO_COMMON')
    df_BO = pd.read_excel('C:/database/FX_DAILY/BO_settings.xlsx', 'BO_LONG_ALLOCATION')
    # df_o_val = pd.read_excel('C:/database/FX_DAILY/BO_settings.xlsx', 'LONDON_ENTRY')
    df_switch = pd.read_excel('C:/database/FX_DAILY/BO_settings.xlsx', 'BO_SWITCH')

    BO_cap = df_common_inputs["cap_bo"].iloc[-1]
    # BO_long_cap = float(df_common_inputs['LONG_ALLOC'].iloc[0]) * BO_cap
    BO_short_cap = float(df_common_inputs['bo_long_perc'].iloc[0]) * BO_cap

    BO_list_wgts = [float(df_BO['EUR.CZK'].iloc[0]), float(df_BO['SGD.CNH'].iloc[0]),
                    float(df_BO['EUR.HKD'].iloc[0]), float(df_BO['AUD.CNH'].iloc[0]),
                    float(df_BO['EUR.USD'].iloc[0]), float(df_BO['CHF.PLN'].iloc[0]),
                    float(df_BO['CHF.CNH'].iloc[0]), float(df_BO['AUD.CAD'].iloc[0]),
                    float(df_BO['NOK.SEK'].iloc[0]), float(df_BO['AUD.SGD'].iloc[0]),
                    float(df_BO['EUR.CHF'].iloc[0]), float(df_BO['EUR.CNH'].iloc[0]),
                    float(df_BO['AUD.HKD'].iloc[0]), float(df_BO['NZD.USD'].iloc[0]),
                    float(df_BO['AUD.USD'].iloc[0]), float(df_BO['USD.CNH'].iloc[0]),
                    float(df_BO['USD.ILS'].iloc[0])]

    # BO_ls_weights_L = []
    # BO_ls_weights_S = []

    BO_list_qty_short = []  # individual short correncies qty stored in this variable
    BO_list_qty_long = []  # individual long correncies qty stored in this variable

    ####################################################################

    ##QTY CALCULATION
    for i in range(len(BO_list)):

        ########################if cycle get delayed#####################################################
        currtime = datetime.now().strftime('%Y%m%d %H:%M:%S')
        week_day = datetime.now().weekday()

        if week_day != 5 and "NZD" in BO_list[i]:
            if "00:29:55" <= currtime[-8:] <= "00:45:02":
                sleeptime = (datetime.strptime("00:45:05", '%H:%M:%S') - datetime.strptime(currtime[-8:],
                                                                                           '%H:%M:%S')).total_seconds()
                print(BO_list[i] + " sleeping till mkt opens at 00:45")
                time.sleep(sleeptime)  # sleeptime

        else:
            if "02:29:50" <= currtime[-8:] <= "02:45:05":
                sleeptime = (datetime.strptime("02:45:10", '%H:%M:%S') - datetime.strptime(currtime[-8:],
                                                                                           '%H:%M:%S')).total_seconds()
                print(BO_list[i] + " sleeping till mkt opens at 02:45")
                time.sleep(sleeptime)  # sleeptime

        ####################################################################################################

        id = tickerId() + tickerId()
        if (i <= 19):
            if (i <= 14):
                if (BO_list[i][0:3] in commonwealth_curr):
                    contract_Details1 = create.create_contract(BO_list[i][0:3], "CASH", "IDEALPRO", "USD", '', '', '',
                                                               '')
                    data1 = ask_bid(contract_Details1, id)
                    convprice1 = float(data1[0])
                    BO_list_qty_long.append(
                        int(math.ceil((BO_short_cap * BO_list_wgts[i] * (1 / convprice1)) / 10000) * 10000))
                else:
                    contract_Details1 = create.create_contract("USD", "CASH", "IDEALPRO", BO_list[i][0:3], '', '', '',
                                                               '')
                    data1 = ask_bid(contract_Details1, id)
                    convprice1 = float(data1[0])
                    BO_list_qty_long.append(
                        int(math.ceil((BO_short_cap * BO_list_wgts[i] * convprice1) / 10000) * 10000))

            elif (i == 15 or i == 16):
                contract_Details1 = create.create_contract("USD", "CASH", "IDEALPRO", BO_list[i][4:7], '', '', '', '')
                data1 = ask_bid(contract_Details1, id)
                convprice1 = float(data1[0])
                BO_list_qty_long.append(
                    int(math.ceil((BO_short_cap * BO_list_wgts[i] * convprice1) / 10000) * 10000))


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
    BO_entry_excel = ["EUR.CZK", "SGD.CNH", "EUR.HKD", "AUD.CNH", "EUR.USD", "CHF.PLN", "CHF.CNH", "AUD.CAD", "NOK.SEK",
                      "AUD.SGD", "EUR.CHF", "EUR.CNH", "AUD.HKD", "NZD.USD", "AUD.USD", "USD.CNH", "USD.ILS"]

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
            list1 = [curr_name, signal, exit_signal, qty]
            # , total, fav_time, mid_time]
            long_params.append(list1)
    print(long_params)
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



    EURCZK_params = long_params[0]
    contract_eurczk = create.create_contract(EURCZK_params[0][0:3], "CASH", "IDEALPRO", EURCZK_params[0][4:7], '', '',
                                             '', '')
    ids = orderidfun(acc, contract_eurczk, EURCZK_params[1], id_list, 2)
    id_list = ids

    time.sleep(2)
    SGDCNH_params = long_params[1]
    contract_sgdcnh = create.create_contract(SGDCNH_params[0][0:3], "CASH", "IDEALPRO", SGDCNH_params[0][4:7], '', '',
                                             '', '')
    ids1 = orderidfun(acc, contract_sgdcnh, SGDCNH_params[1], id_list, 2)
    id_list = ids1

    time.sleep(2)
    EURHKD_params = long_params[2]
    contract_eurhkd = create.create_contract(EURHKD_params[0][0:3], "CASH", "IDEALPRO", EURHKD_params[0][4:7], '', '',
                                             '', '')
    ids2 = orderidfun(acc, contract_eurhkd, EURHKD_params[1], id_list, 2)
    id_list = ids2

    time.sleep(2)
    AUDCNH_params = long_params[3]
    contract_audcnh = create.create_contract(AUDCNH_params[0][0:3], "CASH", "IDEALPRO", AUDCNH_params[0][4:7], '', '',
                                             '', '')
    ids3 = orderidfun(acc, contract_audcnh, AUDCNH_params[1], id_list, 2)
    id_list = ids3

    time.sleep(2)
    EURUSD_params = long_params[4]
    contract_eurusd = create.create_contract(EURUSD_params[0][0:3], "CASH", "IDEALPRO", EURUSD_params[0][4:7], '', '',
                                             '', '')
    ids4 = orderidfun(acc, contract_eurusd, EURUSD_params[1], id_list, 2)
    id_list = ids4

    time.sleep(2)
    CHFPLN_params = long_params[5]
    contract_chfpln = create.create_contract(CHFPLN_params[0][0:3], "CASH", "IDEALPRO", CHFPLN_params[0][4:7], '', '',
                                             '', '')
    ids5 = orderidfun(acc, contract_chfpln, CHFPLN_params[1], id_list, 2)
    id_list = ids5

    time.sleep(2)
    CHFCNH_params = long_params[6]
    contract_chfcnh = create.create_contract(CHFCNH_params[0][0:3], "CASH", "IDEALPRO", CHFCNH_params[0][4:7], '', '',
                                             '', '')
    ids6 = orderidfun(acc, contract_chfcnh, CHFCNH_params[1], id_list, 2)
    id_list = ids6

    time.sleep(2)
    AUDCAD_params = long_params[7]
    contract_audcad = create.create_contract(AUDCAD_params[0][0:3], "CASH", "IDEALPRO", AUDCAD_params[0][4:7], '', '',
                                             '', '')
    ids7 = orderidfun(acc, contract_audcad, AUDCAD_params[1], id_list, 2)
    id_list = ids7

    time.sleep(2)
    NOKSEK_params = long_params[8]
    contract_noksek = create.create_contract(NOKSEK_params[0][0:3], "CASH", "IDEALPRO", NOKSEK_params[0][4:7], '', '',
                                             '', '')
    ids8 = orderidfun(acc, contract_noksek, NOKSEK_params[1], id_list, 2)
    id_list = ids8

    time.sleep(2)
    AUDSGD_params = long_params[9]
    contract_audsgd = create.create_contract(AUDSGD_params[0][0:3], "CASH", "IDEALPRO", AUDSGD_params[0][4:7], '', '',
                                             '', '')
    ids9 = orderidfun(acc, contract_audsgd, AUDSGD_params[1], id_list, 2)
    id_list = ids9

    time.sleep(2)
    EURCHF_params = long_params[10]
    contract_eurchf = create.create_contract(EURCHF_params[0][0:3], "CASH", "IDEALPRO", EURCHF_params[0][4:7], '', '',
                                             '', '')
    ids10 = orderidfun(acc, contract_eurchf, EURCHF_params[1], id_list, 2)
    id_list = ids10

    time.sleep(2)
    EURCNH_params = long_params[11]
    contract_eurcnh = create.create_contract(EURCNH_params[0][0:3], "CASH", "IDEALPRO", EURCNH_params[0][4:7], '', '',
                                             '', '')
    ids11 = orderidfun(acc, contract_eurcnh, EURCNH_params[1], id_list, 2)
    id_list = ids11

    time.sleep(2)
    AUDHKD_params = long_params[12]
    contract_audhkd = create.create_contract(AUDHKD_params[0][0:3], "CASH", "IDEALPRO", AUDHKD_params[0][4:7], '', '',
                                             '', '')
    ids12 = orderidfun(acc, contract_audhkd, AUDHKD_params[1], id_list, 2)
    id_list = ids12

    time.sleep(2)
    NZDUSD_params = long_params[13]
    contract_nzdusd = create.create_contract(NZDUSD_params[0][0:3], "CASH", "IDEALPRO", NZDUSD_params[0][4:7], '', '',
                                             '', '')
    ids13 = orderidfun(acc, contract_nzdusd, NZDUSD_params[1], id_list, 2)
    id_list = ids13

    time.sleep(2)
    AUDUSD_params = long_params[14]
    contract_audusd = create.create_contract(AUDUSD_params[0][0:3], "CASH", "IDEALPRO", AUDUSD_params[0][4:7], '', '',
                                             '', '')
    ids14 = orderidfun(acc, contract_audusd, AUDUSD_params[1], id_list, 2)
    id_list = ids14

    USDCNH_params = long_params[15]
    contract_usdcnh = create.create_contract(USDCNH_params[0][0:3], "CASH", "IDEALPRO", USDCNH_params[0][4:7], '', '',
                                              '', '')
    ids15 = orderidfun(acc, contract_usdcnh, USDCNH_params[1], id_list, 2)
    id_list = ids15

    USDILS_params = long_params[16]
    contract_usdils = create.create_contract(USDILS_params[0][0:3], "CASH", "IDEALPRO", USDILS_params[0][4:7], '', '',
                                             '', '')
    ids16 = orderidfun(acc, contract_usdils, USDILS_params[1], id_list, 2)
    id_list = ids16
    ####################################################################
    currtime = datetime.now().strftime('%H:%M:%S')
    if currtime < "00:31:00":
        sleeptime = (datetime.strptime("00:31:01", '%H:%M:%S') - datetime.strptime(currtime,
                                                                                   '%H:%M:%S')).total_seconds()
        print("BO ENTRIES WILL START AT 00:31, SECS LEFT :", sleeptime)
        time.sleep(sleeptime)
    #############rewuesting for position#####################
    dat = open_pos()
    dat.to_csv("C:/LOG/openpos_BO.csv")
    ########################################################

    switchflag2 = df_switch.loc[df_switch.CURRENCY == 'EUR.CZK', 'TRADE'].iloc[-1]
    if (0 <= df_switch.loc[df_switch.CURRENCY == 'EUR.CZK', 'TP_ENTRY'].iloc[-1] <=0.40) and \
            (0 <= df_switch.loc[df_switch.CURRENCY == 'EUR.CZK', 'TP_EXIT1'].iloc[-1] <=0.40) and \
            (0 <= df_switch.loc[df_switch.CURRENCY == 'EUR.CZK', 'TP_EXIT2'].iloc[-1] <=0.40) and\
            (switchflag2 in ('yes', 'YES')):
        Thread(target=BO_NY,
               args=(contract_eurczk, dat, EURCZK_params, ids, 100, 200, BO_df, long_params[0][1], long_params[0][2])).start()
    # ##########################################################
    switchflag3 = df_switch.loc[df_switch.CURRENCY == 'SGD.CNH', 'TRADE'].iloc[-1]
    if (0 <= df_switch.loc[df_switch.CURRENCY == 'SGD.CNH', 'TP_ENTRY'].iloc[-1] <=0.40) and \
            (0 <= df_switch.loc[df_switch.CURRENCY == 'SGD.CNH', 'TP_EXIT1'].iloc[-1] <=0.40) and \
            (0 <= df_switch.loc[df_switch.CURRENCY == 'SGD.CNH', 'TP_EXIT2'].iloc[-1] <=0.40) and \
            (switchflag3 in ('yes', 'YES')):
        Thread(target=BO_NY,
               args=(
                   contract_sgdcnh, dat, SGDCNH_params, ids1, 300, 400, BO_df, long_params[0][1],long_params[0][2])).start()
    # ############################################################################
    switchflag4 = df_switch.loc[df_switch.CURRENCY == 'EUR.HKD', 'TRADE'].iloc[-1]
    if (0 <= df_switch.loc[df_switch.CURRENCY == 'EUR.HKD', 'TP_ENTRY'].iloc[-1] <=0.40) and \
            (0 <= df_switch.loc[df_switch.CURRENCY == 'EUR.HKD', 'TP_EXIT1'].iloc[-1] <=0.40) and \
            (0 <= df_switch.loc[df_switch.CURRENCY == 'EUR.HKD', 'TP_EXIT2'].iloc[-1] <=0.40) and \
            (switchflag4 in ('yes', 'YES')):
       Thread(target=BO_NY,
               args=(contract_eurhkd, dat, EURHKD_params, ids2, 500, 600, BO_df, long_params[0][1],
                   long_params[0][2])).start()
    # ###############################################################
    switchflag5 = df_switch.loc[df_switch.CURRENCY == 'AUD.CNH', 'TRADE'].iloc[-1]
    if (0 <= df_switch.loc[df_switch.CURRENCY == 'AUD.CNH', 'TP_ENTRY'].iloc[-1] <=0.40) and \
            (0 <= df_switch.loc[df_switch.CURRENCY == 'AUD.CNH', 'TP_EXIT1'].iloc[-1] <=0.40) and \
            (0 <= df_switch.loc[df_switch.CURRENCY == 'AUD.CNH', 'TP_EXIT2'].iloc[-1] <=0.40) and \
            (switchflag5 in ('yes', 'YES')):
        Thread(target=BO_NY,
               args=(contract_audcnh, dat, AUDCNH_params, ids3, 700, 800, BO_df, long_params[0][1],
                   long_params[0][2])).start()
    #
    # ##########################################################################
    switchflag6 = df_switch.loc[df_switch.CURRENCY == 'EUR.USD', 'TRADE'].iloc[-1]
    if (0 <= df_switch.loc[df_switch.CURRENCY == 'EUR.USD', 'TP_ENTRY'].iloc[-1] <=0.40) and \
            (0 <= df_switch.loc[df_switch.CURRENCY == 'EUR.USD', 'TP_EXIT1'].iloc[-1] <=0.40) and \
            (0 <= df_switch.loc[df_switch.CURRENCY == 'EUR.USD', 'TP_EXIT2'].iloc[-1] <=0.40) and \
            (switchflag6 in ('yes', 'YES')):
        Thread(target=BO_NY,
               args=(contract_eurusd, dat, EURUSD_params, ids4, 800, 900, BO_df, long_params[0][1],
                   long_params[0][2])).start()
    # ##############################################
    switchflag7 = df_switch.loc[df_switch.CURRENCY == 'CHF.PLN', 'TRADE'].iloc[-1]
    if (0 <= df_switch.loc[df_switch.CURRENCY == 'CHF.PLN', 'TP_ENTRY'].iloc[-1] <=0.40) and \
            (0 <= df_switch.loc[df_switch.CURRENCY == 'CHF.PLN', 'TP_EXIT1'].iloc[-1] <=0.40) and \
            (0 <= df_switch.loc[df_switch.CURRENCY == 'CHF.PLN', 'TP_EXIT2'].iloc[-1] <=0.40) and \
            (switchflag7 in ('yes', 'YES')):
        Thread(target=BO_NY,
               args=(contract_chfpln, dat, CHFPLN_params, ids5, 1000, 1100, BO_df, long_params[0][1],
                     long_params[0][2])).start()
    # ##################################################################
    switchflag8 = df_switch.loc[df_switch.CURRENCY == 'CHF.CNH', 'TRADE'].iloc[-1]
    if (0 <= df_switch.loc[df_switch.CURRENCY == 'CHF.CNH', 'TP_ENTRY'].iloc[-1] <=0.40) and \
            (0 <= df_switch.loc[df_switch.CURRENCY == 'CHF.CNH', 'TP_EXIT1'].iloc[-1] <=0.40) and \
            (0 <= df_switch.loc[df_switch.CURRENCY == 'CHF.CNH', 'TP_EXIT2'].iloc[-1] <=0.40) and \
            (switchflag8 in ('yes', 'YES')):
        Thread(target=BO_NY,
               args=(contract_chfcnh, dat, CHFCNH_params, ids6, 1200, 1300, BO_df, long_params[0][1],
                     long_params[0][2])).start()
    # ##########################################################
    switchflag9 = df_switch.loc[df_switch.CURRENCY == 'AUD.CAD', 'TRADE'].iloc[-1]
    if (0 <= df_switch.loc[df_switch.CURRENCY == 'AUD.CAD', 'TP_ENTRY'].iloc[-1] <=0.40) and \
            (0 <= df_switch.loc[df_switch.CURRENCY == 'AUD.CAD', 'TP_EXIT1'].iloc[-1] <=0.40) and \
            (0 <= df_switch.loc[df_switch.CURRENCY == 'AUD.CAD', 'TP_EXIT2'].iloc[-1] <=0.40) and \
            (switchflag9 in ('yes', 'YES')):
        Thread(target=BO_NY,
               args=(contract_audcad, dat, AUDCAD_params, ids7, 1400, 1500, BO_df, long_params[0][1],
                     long_params[0][2])).start()
    ########################
    # ##########################################################
    switchflag10 = df_switch.loc[df_switch.CURRENCY == 'NOK.SEK', 'TRADE'].iloc[-1]
    if (0 <= df_switch.loc[df_switch.CURRENCY == 'NOK.SEK', 'TP_ENTRY'].iloc[-1] <=0.40) and \
            (0 <= df_switch.loc[df_switch.CURRENCY == 'NOK.SEK', 'TP_EXIT1'].iloc[-1] <=0.40) and \
            (0 <= df_switch.loc[df_switch.CURRENCY == 'NOK.SEK', 'TP_EXIT2'].iloc[-1] <=0.40) and \
            (switchflag10 in ('yes', 'YES')):
        Thread(target=BO_NY,
               args=(contract_noksek, dat, NOKSEK_params, ids8, 1600, 1700, BO_df, long_params[0][1],
                     long_params[0][2])).start()
    ########################
    # ##########################################################
    switchflag11 = df_switch.loc[df_switch.CURRENCY == 'AUD.SGD', 'TRADE'].iloc[-1]
    if (0 <= df_switch.loc[df_switch.CURRENCY == 'AUD.SGD', 'TP_ENTRY'].iloc[-1] <=0.40) and \
            (0 <= df_switch.loc[df_switch.CURRENCY == 'AUD.SGD', 'TP_EXIT1'].iloc[-1] <=0.40) and \
            (0 <= df_switch.loc[df_switch.CURRENCY == 'AUD.SGD', 'TP_EXIT2'].iloc[-1] <=0.40) and \
            (switchflag11 in ('yes', 'YES')):
        Thread(target=BO_NY,
               args=(contract_audsgd, dat, AUDSGD_params, ids9, 1800, 1900, BO_df, long_params[0][1],
                     long_params[0][2])).start()
    ########################
    # ##########################################################
    switchflag12 = df_switch.loc[df_switch.CURRENCY == 'EUR.CHF', 'TRADE'].iloc[-1]
    if (0 <= df_switch.loc[df_switch.CURRENCY == 'EUR.CHF', 'TP_ENTRY'].iloc[-1] <=0.40) and \
            (0 <= df_switch.loc[df_switch.CURRENCY == 'EUR.CHF', 'TP_EXIT1'].iloc[-1] <=0.40) and \
            (0 <= df_switch.loc[df_switch.CURRENCY == 'EUR.CHF', 'TP_EXIT2'].iloc[-1] <=0.40) and \
            (switchflag12 in ('yes', 'YES')):
        Thread(target=BO_NY,
               args=(contract_eurchf, dat, EURCHF_params, ids10, 2000, 2100, BO_df, long_params[0][1],
                     long_params[0][2])).start()
    ########################
    # ##########################################################
    switchflag13 = df_switch.loc[df_switch.CURRENCY == 'EUR.CNH', 'TRADE'].iloc[-1]
    if (0 <= df_switch.loc[df_switch.CURRENCY == 'EUR.CNH', 'TP_ENTRY'].iloc[-1] <=0.40) and \
            (0 <= df_switch.loc[df_switch.CURRENCY == 'EUR.CNH', 'TP_EXIT1'].iloc[-1] <=0.40) and \
            (0 <= df_switch.loc[df_switch.CURRENCY == 'EUR.CNH', 'TP_EXIT2'].iloc[-1] <=0.40) and \
            (switchflag13 in ('yes', 'YES')):
        Thread(target=BO_NY,
               args=(contract_eurcnh, dat, EURCNH_params, ids11, 2200, 2300, BO_df, long_params[0][1],
                     long_params[0][2])).start()
    ########################
    # ##########################################################
    switchflag14 = df_switch.loc[df_switch.CURRENCY == 'AUD.HKD', 'TRADE'].iloc[-1]
    if (0 <= df_switch.loc[df_switch.CURRENCY == 'AUD.HKD', 'TP_ENTRY'].iloc[-1] <=0.40) and \
            (0 <= df_switch.loc[df_switch.CURRENCY == 'AUD.HKD', 'TP_EXIT1'].iloc[-1] <=0.40) and \
            (0 <= df_switch.loc[df_switch.CURRENCY == 'AUD.HKD', 'TP_EXIT2'].iloc[-1] <=0.40) and \
            (switchflag14 in ('yes', 'YES')):
        Thread(target=BO_NY,
               args=(contract_audhkd, dat, AUDHKD_params, ids12, 2400, 2500, BO_df, long_params[0][1],
                     long_params[0][2])).start()
    ########################
    # ##########################################################
    switchflag15 = df_switch.loc[df_switch.CURRENCY == 'NZD.USD', 'TRADE'].iloc[-1]
    if (0 <= df_switch.loc[df_switch.CURRENCY == 'NZD.USD', 'TP_ENTRY'].iloc[-1] <=0.40) and \
            (0 <= df_switch.loc[df_switch.CURRENCY == 'NZD.USD', 'TP_EXIT1'].iloc[-1] <=0.40) and \
            (0 <= df_switch.loc[df_switch.CURRENCY == 'NZD.USD', 'TP_EXIT2'].iloc[-1] <=0.40) and \
            (switchflag15 in ('yes', 'YES')):
        Thread(target=BO_NY,
               args=(contract_nzdusd, dat, NZDUSD_params, ids13, 2600, 2700, BO_df, long_params[0][1],
                     long_params[0][2])).start()
    ########################
    # ##########################################################
    switchflag16 = df_switch.loc[df_switch.CURRENCY == 'AUD.USD', 'TRADE'].iloc[-1]
    if (0 <= df_switch.loc[df_switch.CURRENCY == 'AUD.USD', 'TP_ENTRY'].iloc[-1] <=0.40) and \
            (0 <= df_switch.loc[df_switch.CURRENCY == 'AUD.USD', 'TP_EXIT1'].iloc[-1] <=0.40) and \
            (0 <= df_switch.loc[df_switch.CURRENCY == 'AUD.USD', 'TP_EXIT2'].iloc[-1] <=0.40) and \
            (switchflag16 in ('yes', 'YES')):
        Thread(target=BO_NY,
               args=(contract_audusd, dat, AUDUSD_params, ids14, 2800, 2900, BO_df, long_params[0][1],
                     long_params[0][2])).start()
    ########################
    # ##########################################################
    switchflag17 = df_switch.loc[df_switch.CURRENCY == 'USD.CNH', 'TRADE'].iloc[-1]
    if (0 <= df_switch.loc[df_switch.CURRENCY == 'USD.CNH', 'TP_ENTRY'].iloc[-1] <=0.40) and \
            (0 <= df_switch.loc[df_switch.CURRENCY == 'USD.CNH', 'TP_EXIT1'].iloc[-1] <=0.40) and \
            (0 <= df_switch.loc[df_switch.CURRENCY == 'USD.CNH', 'TP_EXIT2'].iloc[-1] <=0.40) and \
            (switchflag17 in ('yes', 'YES')):
        Thread(target=BO_NY,
               args=(contract_usdcnh, dat, USDCNH_params, ids15, 3000, 3100, BO_df, long_params[0][1],
                     long_params[0][2])).start()
    ########################
    # ##########################################################
    switchflag18 = df_switch.loc[df_switch.CURRENCY == 'USD.ILS', 'TRADE'].iloc[-1]
    if (0 <= df_switch.loc[df_switch.CURRENCY == 'USD.ILS', 'TP_ENTRY'].iloc[-1] <=0.40) and \
            (0 <= df_switch.loc[df_switch.CURRENCY == 'USD.ILS', 'TP_EXIT1'].iloc[-1] <=0.40) and \
            (0 <= df_switch.loc[df_switch.CURRENCY == 'USD.ILS', 'TP_EXIT2'].iloc[-1] <=0.40) and \
            (switchflag18 in ('yes', 'YES')):
        Thread(target=BO_NY,
               args=(contract_usdils, dat, USDILS_params, ids16, 3200, 3300, BO_df, long_params[0][1],
                     long_params[0][2])).start()
    ########################
    Thread(target=bo_report, args=(BO_df,)).start()
    ######################################################
    return




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
                sleeptime = 0.3
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
    df_rt=df_rt[["DATE","ASK","ASK_SIZE","BID","BID_SIZE"]]


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
    AUDNZD_df = pd.DataFrame(columns=["DATE", "ASK","ASK_SIZE", "BID","BID_SIZE"])
    NOKSEK_df = pd.DataFrame(columns=["DATE", "ASK", "ASK_SIZE", "BID", "BID_SIZE"])
    EURSEK_df = pd.DataFrame(columns=["DATE", "ASK", "ASK_SIZE", "BID", "BID_SIZE"])
    EURNOK_df = pd.DataFrame(columns=["DATE", "ASK", "ASK_SIZE", "BID", "BID_SIZE"])

    Thread(target=streaming_data, args=("AUDNZD", 1,100, AUDNZD_df)).start()


    Thread(target=streaming_data, args=("NOKSEK", 101,200, NOKSEK_df)).start()

    Thread(target=streaming_data, args=("EURSEK", 201,300, EURSEK_df)).start()

    Thread(target=streaming_data, args=("EURNOK", 301,400, EURNOK_df)).start()

    # EURNZD_df = pd.DataFrame(columns=["DATE", "ASK", "ASK_SIZE", "BID", "BID_SIZE"])
    # Thread(target=streaming_data, args=("EURNZD", 100, EURNZD_df)).start()
    #
    # # time.sleep(5)
    # CHFSEK_df = pd.DataFrame(columns=["DATE", "ASK", "ASK_SIZE", "BID", "BID_SIZE"])
    # Thread(target=streaming_data, args=("CHFSEK", 125, CHFSEK_df)).start()
    #
    # CHFNOK_df = pd.DataFrame(columns=["DATE", "ASK", "ASK_SIZE", "BID", "BID_SIZE"])
    # Thread(target=streaming_data, args=("CHFNOK", 150, CHFNOK_df)).start()
    #
    # NZDCHF_df = pd.DataFrame(columns=["DATE", "ASK", "ASK_SIZE", "BID", "BID_SIZE"])
    # Thread(target=streaming_data, args=("NZDCHF", 175, NZDCHF_df)).start()
    #
    # GBPSEK_df = pd.DataFrame(columns=["DATE", "ASK", "ASK_SIZE", "BID", "BID_SIZE"])
    # Thread(target=streaming_data, args=("GBPSEK", 200, GBPSEK_df)).start()
    #
    # GBPNOK_df = pd.DataFrame(columns=["DATE", "ASK", "ASK_SIZE", "BID", "BID_SIZE"])
    # Thread(target=streaming_data, args=("GBPNOK", 225, GBPNOK_df)).start()

    return


import schedule

df_schedule = pd.read_excel('C:/database/FX_DAILY/BO_settings.xlsx', 'SCHEDULE')

if df_schedule.loc[df_schedule.DAY == "TUESDAY", "PERMISSION_NY"].iloc[-1] == "YES" or \
        df_schedule.loc[df_schedule.DAY == "TUESDAY", "PERMISSION_NY"].iloc[-1] == "yes":
    runtime = (df_schedule.loc[df_schedule.DAY == "TUESDAY", "TIME_NY"].iloc[-1])
    runtime = runtime.strftime("%H:%M")
    print(runtime, "TUESDAY")
    schedule.every().tuesday.at(runtime).do(run_bo)
if df_schedule.loc[df_schedule.DAY == "WEDNESDAY", "PERMISSION_NY"].iloc[-1] == "YES" or \
        df_schedule.loc[df_schedule.DAY == "WEDNESDAY", "PERMISSION_NY"].iloc[-1] == "yes":
    runtime = (df_schedule.loc[df_schedule.DAY == "WEDNESDAY", "TIME_NY"].iloc[-1])
    runtime = runtime.strftime("%H:%M")
    print(runtime, "WEDNESDAY")
    schedule.every().wednesday.at(runtime).do(run_bo)

if df_schedule.loc[df_schedule.DAY == "THURSDAY", "PERMISSION_NY"].iloc[-1] == "YES" or \
        df_schedule.loc[df_schedule.DAY == "THURSDAY", "PERMISSION_NY"].iloc[-1] == "yes":
    runtime = (df_schedule.loc[df_schedule.DAY == "THURSDAY", "TIME_NY"].iloc[-1])
    runtime = runtime.strftime("%H:%M")
    print(runtime, "THURSDAY")
    schedule.every().thursday.at(runtime).do(run_bo)

if df_schedule.loc[df_schedule.DAY == "FRIDAY", "PERMISSION_NY"].iloc[-1] == "YES" or \
        df_schedule.loc[df_schedule.DAY == "FRIDAY", "PERMISSION_NY"].iloc[-1] == "yes":
    runtime = (df_schedule.loc[df_schedule.DAY == "FRIDAY", "TIME_NY"].iloc[-1])
    runtime = runtime.strftime("%H:%M")
    print(runtime, "FRIDAY")
    schedule.every().friday.at(runtime).do(run_bo)


if df_schedule.loc[df_schedule.DAY == "SATURDAY", "PERMISSION_NY"].iloc[-1] == "YES" or \
        df_schedule.loc[df_schedule.DAY == "SATURDAY", "PERMISSION_NY"].iloc[-1] == "yes":
    runtime = (df_schedule.loc[df_schedule.DAY == "SATURDAY", "TIME_NY"].iloc[-1])
    runtime = runtime.strftime("%H:%M")
    print(runtime, "SATURDAY")
    schedule.every().saturday.at(runtime).do(run_bo)

schedule.every().wednesday.at("22:30").do(run_data)

while True:
    schedule.run_pending()
    time.sleep(1)

