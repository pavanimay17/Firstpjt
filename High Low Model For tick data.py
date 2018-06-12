# -*- coding: utf-8 -*-
"""
Created on Wed May  9 16:49:49 2018

@author: user
"""

"""
Created on Fri Mar  9 17:57:23 2018

@author:Ravi
"""
import pandas as pd
import os
lag_val=1
MA_val=20
TP_Cut=0.2
rolling_window=8
close_time=' 13:00:00'

mypath='F:/Work/Data/TickData'
arr = os.listdir(mypath)



final_dat=pd.DataFrame()
for p in range(len(arr)):
    dat=pd.read_csv(mypath+'/'+arr[p])
    dat.columns=['Date','Open','High','Low','Close']
    uniq_dates=pd.DataFrame(pd.to_datetime(dat['Date']).map(lambda x: x.strftime('%Y-%m-%d')).unique())
    uniq_time= pd.DataFrame(pd.to_datetime(dat['Date']).map(lambda x: x.strftime('%H:%M:%S')).unique())
    dat.index=pd.to_datetime(dat['Date'])
    dat=dat.iloc[:,1: 5]
    dat.columns=['Open','High','Low','Close']
    entry_price=[]
    high_price=[]
    low_price=[]
    for i in range(uniq_dates.shape[0]-1):
        temp = dat.loc[(dat.index>=pd.to_datetime(uniq_dates[0].loc[i]+close_time))&(dat.index<=pd.to_datetime(uniq_dates[0].loc[i+1]+close_time))]
        entry_price.append(temp.iloc[0:1,:].values.mean())
        high_price.append(temp['High'].max())
        low_price.append(temp['Low'].min())
    entry_HL_dat=pd.DataFrame(data=[entry_price,high_price,low_price]).T
    entry_HL_dat.columns=['entry_price','high_price','low_price']
    entry_HL_dat.index=pd.to_datetime(uniq_dates[0].loc[1:uniq_dates.shape[0]])
    entry_HL_dat['HC_MA']=(abs((entry_HL_dat['high_price']/entry_HL_dat['entry_price']-1).rolling(MA_val).mean())*TP_Cut).shift(lag_val)
    entry_HL_dat['LC_MA']=(abs((entry_HL_dat['low_price']/entry_HL_dat['entry_price']-1).rolling(MA_val).mean())*TP_Cut).shift(lag_val)
    #entry_HL_dat['HC_TP']=entry_HL_dat['entry_price']+(entry_HL_dat['entry_price']*entry_HL_dat['HC_MA'])
    #entry_HL_dat['LC_TP']=entry_HL_dat['entry_price']-(entry_HL_dat['entry_price']*entry_HL_dat['LC_MA'])
    entry_HL_dat=entry_HL_dat.dropna()
    uniq_dates=uniq_dates[0].loc[pd.to_datetime(uniq_dates[0])>=entry_HL_dat.index[0]]
    uniq_dates.index=range(0,uniq_dates.shape[0])
    uniq_dates=pd.DataFrame(uniq_dates)
    
    for i in range((uniq_dates.shape[0]-1)):
        HC_TP=0
        LC_TP=0
        temp = dat.loc[(dat.index>=pd.to_datetime(uniq_dates[0].loc[i]+close_time))&(dat.index<=pd.to_datetime(uniq_dates[0].loc[i+1]+close_time))]
        HC_TP=temp.iloc[0:1,:].values.mean()+(temp.iloc[0:1,:].values.mean()*entry_HL_dat['HC_MA'].loc[uniq_dates[0].loc[i]])
        LC_TP=temp.iloc[0:1,:].values.mean()-(temp.iloc[0:1,:].values.mean()*entry_HL_dat['LC_MA'].loc[uniq_dates[0].loc[i]])
        entry_val=temp.iloc[0:1,:].values.mean()
        temp['HC_Ret'] = 0
        temp['LC_Ret'] = 0
        for m in range(1, temp.shape[0]):
            if (temp.iloc[m:(m+1),0:4].values.any()>HC_TP):
                if ( (~(pd.to_datetime(uniq_dates[0].loc[i]).weekday()==4))&(temp.iloc[m:(m+1),0:1].values>HC_TP)):
                    temp.iloc[m:(m+1),4:5]=float(temp.iloc[m:(m+1),0:1].values/entry_val-1)
                    break
                elif(temp.iloc[m:(m+1),1:2].values>HC_TP):
                    temp.iloc[m:(m+1),4:5]=float(temp.iloc[m:(m+1),1:2].values/entry_val-1)
                    break
                elif(temp.iloc[m:(m+1),3:4].values>HC_TP):
                    temp.iloc[m:(m+1),4:5]=float(temp.iloc[m:(m+1),3:4].values/entry_val-1)
                    break
        if (~temp['HC_Ret'].values.any()):
            temp.iloc[m:(m+1),4:5]=float(temp.iloc[m:(m+1),:].values.mean()/entry_val-1)
                    
        for m in range(1, temp.shape[0]):
            if (temp.iloc[m:(m+1),0:4].values.any()<LC_TP):
                if ((~(pd.to_datetime(uniq_dates[0].loc[i]).weekday()==4))&(temp.iloc[m:(m+1),0:1].values<LC_TP)):
                    temp.iloc[m:(m+1),5:6]=-1*(float(temp.iloc[m:(m+1),0:1].values/entry_val-1))
                    break
                elif(temp.iloc[m:(m+1),2:3].values<LC_TP):
                    temp.iloc[m:(m+1),5:6]=-1*(float(temp.iloc[m:(m+1),1:2].values/entry_val-1))
                    break
                elif (temp.iloc[m:(m+1),3:4].values<LC_TP):
                    temp.iloc[m:(m+1),5:6]=-1*(float(temp.iloc[m:(m+1),3:4].values/entry_val-1))
                    break
        if (~temp['LC_Ret'].values.any()):
            temp.iloc[m:(m+1),5:6]=-1*(float(temp.iloc[m:(m+1),:].values.mean()/entry_val-1))
        
        tt=pd.DataFrame(data=[temp.index[0],entry_val,temp['HC_Ret'].loc[temp['HC_Ret']!=0].index[0],temp['HC_Ret'].loc[temp['HC_Ret']!=0].values,temp['LC_Ret'].loc[temp['LC_Ret']!=0].index[0],temp['LC_Ret'].loc[temp['LC_Ret']!=0].values]).T
        tt.columns=['Entry Date','Entry_Price','Long_Exit','Long_Ret','Short_Exit','Short_Ret']
        final_dat=final_dat.append(tt, ignore_index=True)
       
final_dat.to_csv('F:/final_dat.csv')      
