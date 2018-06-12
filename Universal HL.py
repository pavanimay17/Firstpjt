# -*- coding: utf-8 -*-
"""
Created on Sun Apr 15 01:18:29 2018

@author: Ravi
"""
import pandas as pd
import os
#import matplotlib.pyplot as plt
#import numpy as np

Data_Frequency= 'Weekly'           #'Daily'|'weekly'|'Quarterly'|'Monthly
Data_resampling='Thu'              #'Mon' or 'Tue' or 'Wed' or 'Thur' or 'Fri' Works only for weekly data
lag_val=1
MA_val=10                          #how many daily or weekly or quarterly data used to calculate the TP
TP_Cut=0.2
rolling_window=8                   #  Determines insample and out of sample data
portfolio_size=7
mypath='F:/Work\Data\TickDataNOKSEK'
arr = os.listdir(mypath)

#High Low model function. 
def High_Low(dat,lag_val,MA_val,TP_Cut):
    dat['HC_TP']=(dat['Close']+(dat['Close']*abs((dat['High']/dat['Close'].shift(lag_val)-1).rolling(MA_val).mean())*TP_Cut)).shift(lag_val) #calculate the high tp close based TP
    dat['LC_TP']=(dat['Close']-(dat['Close']*abs((dat['Low']/dat['Close'].shift(lag_val)-1).rolling(MA_val).mean())*TP_Cut)).shift(lag_val) #calculate the low tp close based TP
    dat['temp']=dat['Close'].shift(lag_val) # lagging the close price so that everything is one row for comparison
    dat=dat.dropna()
    dat['HC_Ret'] = 0 #initializing empty col
    dat['LC_Ret'] = 0 #initializing empty col
    dat.loc[dat['Open']>dat['HC_TP'],'HC_Ret'] = dat['Open']/dat['temp']-1 # All rows where open >Tp calculate ret based on open price
    dat.loc[dat['Open']<dat['LC_TP'],'LC_Ret'] = -1*(dat['Open']/dat['temp']-1)  # All rows where open >Tp calculate ret based on open price
    myindex=dat.loc[(dat['HC_Ret']== 0)&(dat['High']>dat['HC_TP'])].index # identifying all rows where High > TP
    dat['HC_Ret'].loc[myindex] = dat['HC_TP'].loc[myindex]/dat['temp'].loc[myindex]-1 # calculate the ret for rows identified above usikng the high price
    myindex=dat.loc[(dat['LC_Ret']== 0)&(dat['Low']<dat['LC_TP'])].index # identifying all rows where low < TP
    dat['LC_Ret'].loc[myindex] = -1*(dat['LC_TP'].loc[myindex]/dat['temp'].loc[myindex]-1) # calculate the ret for rows identified above usikng the low price
    # for all other rows claculate the ret using the close price
    myindex=dat.loc[(dat['HC_Ret']== 0)].index
    dat['HC_Ret'].loc[myindex] =dat['Close'].loc[myindex]/dat['temp'].loc[myindex]-1
    myindex=dat.loc[(dat['LC_Ret']== 0)].index
    dat['LC_Ret'].loc[myindex] = -1*(dat['Close'].loc[myindex]/dat['temp'].loc[myindex]-1)  
    dat=dat[['HC_Ret','LC_Ret']] #take only the return columns andstore in dat. This is the output of the function
    return dat

#the below functions are to get the first and the last day of the week/month/quarter
def take_first(array_like):
    return array_like[0]
def take_last(array_like):
    return array_like[-1]


# now we create weekly/monthly/quarterly dataset from the daily data (input data set) based on user input and run the high low function for all assets in arr
# all the returns for each asset is stored in the list final_dat
final_dat=[]
for p in range(len(arr)):
    mydat=pd.read_csv(mypath+'/'+arr[p])
    mydat.columns=['Date','Open','High','Low','Close']
    mydat.index=pd.to_datetime(mydat['Date'])
    mydat=mydat.iloc[:,1: 5]
    if(Data_Frequency=='weekly'):
        mydat=mydat.resample('W-'+Data_resampling).agg({'Open': take_first, 
                         'High': 'max',
                         'Low': 'min',
                         'Close': take_last})
    elif(Data_Frequency=='Monthly'):
        mydat=mydat.resample('BM').agg({'Open': take_first, 
                         'High': 'max',
                         'Low': 'min',
                         'Close': take_last})
    elif(Data_Frequency=='Quarterly'):
        mydat=mydat.resample('BQ').agg({'Open': take_first, 
                         'High': 'max',
                         'Low': 'min',
                         'Close': take_last})  
    dat1=High_Low(mydat,lag_val,MA_val,TP_Cut)
    dat1.columns=[arr[p].split('.csv')[0]+'_HC_Ret',arr[p].split('.csv')[0]+'_LC_Ret']
    final_dat.append(dat1)

# the below two lines we aggregate the individual asset returns by summing up row wise and then calculate the portfolio cumulative ret
final_port1=pd.DataFrame({'Portfolio_Ret':final_dat[4].iloc[:,0:1].dropna().sum(axis=1)})
final_port1['Cum_Ret']=final_port1['Portfolio_Ret'].cumsum()