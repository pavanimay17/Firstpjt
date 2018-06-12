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
port = 7496
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



def tickerId():
    a = random.sample(range(60001, 90000), 2000)
    b = random.sample(range(1, 10000), 40)
    random.shuffle(a)
    random.shuffle(b)
    tickerId = a[0] + b[0]
    return tickerId




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
        order_info = create.create_order(acc, "LMT", 10000, "SELL", 10.000, False, False, None,None,None,None,None,None)
        tws.placeOrder(int(i), contract_info, order_info)
        time.sleep(0.2)

    buyidlist = generateNumber(loops)

    while buyidlist[0] == sellidlist[0]:
        buyidlist = generateNumber(loops)

    for i in buyidlist:
        order_info = create.create_order(acc, "LMT", 10000, "BUY", 10.000, False, False, None,None,None,None,None,None)
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



conn()
id_list = [[0], [0]]
curr='NOK.SEK'
contract=create.create_contract(curr[0:3], "CASH", "IDEALPRO", curr[4:7], '', '', '', '')
idlist=orderidfun(acc,contract,type,id_list,2)
rt_price=ask_bid(contract,2000)

order_info = create.create_order(acc, "LMT", 10000, "BUY", float(rt_price[0]), True, 'GTC', None,None,None,None,None,None)
tws.placeOrder(int(id_list[1][0]), contract, order_info)
