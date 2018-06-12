#!/usr/bin/python
# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np
from pandas.tseries.offsets import BDay
from itertools import combinations
from dateutil.relativedelta import relativedelta
from itertools import accumulate

dat = pd.read_csv('D:/ARB/data/Asset_1000_0.csv').set_index('Date')  # having price 0
dat11 = pd.read_csv('D:/ARB/data/Asset_1000_1.csv').set_index('Date')
dat.index = pd.to_datetime(dat.index, dayfirst=True)
dat11.index = pd.to_datetime(dat.index, dayfirst=True)

fx_data = pd.read_csv('D:/ARB/data/FX_1000_0.csv').set_index('Date')
fx_data.index = pd.to_datetime(fx_data.index, dayfirst=True)
fx_data = fx_data.loc[dat.index]

multipliers = pd.read_csv('D:/ARB/data/mult.csv')
tick_curr = pd.read_csv('D:/ARB/data/ticker_currency.csv')
tcost = pd.read_csv('D:/ARB/data/TCosts.csv')

dat_ret = dat.pct_change()  # daily returns
dat_ret1 = dat11.pct_change()

user_input = 250000
lag_indicator = 20
mean_sd_lag = 20

# list_tlevel=[0.8, 0.825, 0.85,0.875,0.9]

t_level = 0.8

# list_zscorelevel=[1, 1.25, 1.5, 1.75, 2, 2.25, 2.5, 2.75, 3]

z_score_level = 1
st_yr = '2015'
st_month = '12'

end_yr = '2016'
end_month = '06'
strt_dt = st_yr + '-' + st_month
end_dt = end_yr + '-' + end_month

corr_window = 1  # input number of months
roll_freq = 'monthly'  # 'weeks', 'months', 'quarters', and 'years'


# creating a function for zscore

def zscore(x):
    dif_mean = pd.rolling_mean(x,
                               x.index.get_loc(x.groupby(pd.TimeGrouper('M'
                               )).nth(0).index[1])).shift().dropna()
    dif_sd = pd.rolling_std(x,
                            x.index.get_loc(x.groupby(pd.TimeGrouper('M'
                            )).nth(0).index[1])).shift().dropna()
    return (x - dif_mean) / dif_sd


traded_price = dat11.copy(deep=True)
local_price = fx_data.copy(deep=True)

mult = multipliers.loc[pd.match(traded_price.columns,
                       multipliers['Ticker'])]

for i in range(len(mult)):
    traded_price[traded_price.columns[i]] = \
        traded_price[traded_price.columns[i]] * mult.iat[i, 1]

(a, b) = (local_price[[col for col in local_price if col[3:6] == 'USD'
          in col]].columns, local_price[[col for col in local_price
          if col[3:6] != 'USD' in col]].columns)
local_price[a] = user_input / local_price[a]
local_price[b] = user_input * local_price[b]

new_trd = traded_price.copy(deep=True)
temp = tick_curr.loc[pd.match(new_trd.columns, tick_curr['Ticker'
                     ])]['Currency']

for i in range(len(temp)):
    new_trd = new_trd.rename(columns={new_trd.columns[i]: temp.iat[i]})

# get local_prices for each currency

new_loc = new_trd.copy(deep=True)
for i in range(len(new_loc.columns)):
    for j in range(len(local_price.columns)):
        if new_loc.columns[i] == 'USD':
            new_loc[[i]] = user_input
        elif new_loc.columns[i] in local_price.columns[j]:
            new_loc[new_loc.columns[i]] = \
                local_price[local_price.columns[j]]

# no.of stocks

stocks_units = np.round(new_loc / new_trd.values)
stocks_units.columns = dat.columns
stocks_units = stocks_units.replace('inf', 0)

new_loc = traded_price.values * stocks_units
new_loc.columns = traded_price.columns

temp = dat_ret[strt_dt:end_dt]

# temp = temp[:temp[end_dt].index[0]]

# roll_freq='months'  #'weeks', 'months', 'quarters', and 'years'

if roll_freq == 'monthly':
    grouped_data = temp.resample('MS')
    grouped_data1 = temp.groupby(pd.TimeGrouper('M')).nth(-1)
elif roll_freq == 'quarterly':
    grouped_data = temp.groupby(pd.TimeGrouper('Q')).nth(0)
    grouped_data1 = temp.groupby(pd.TimeGrouper('Q')).nth(-1)
elif roll_freq == 'yearly':
    grouped_data = temp.groupby(pd.TimeGrouper('Y')).nth(0)
    grouped_data1 = temp.groupby(pd.TimeGrouper('Y')).nth(-1)
elif roll_freq == 'quarterly':
    grouped_data = temp.groupby(pd.TimeGrouper('W')).nth(0)
    grouped_data1 = temp.resample('W')

# getting the names of possible combination of pairs

name_comb = []
for c in combinations(temp.columns, 2):
    name_comb.append(c)

names = pd.DataFrame(name_comb)
names = (names[[0]] + '_' + names[[1]].values).T

# calculates correlation for all months

z = grouped_data.index.tolist()
z.append(grouped_data1.index[-1] + BDay(1))
cor_table = pd.DataFrame()
for i in range(len(z)):
    corr = dat_ret1.loc[z[i] - relativedelta(months=+corr_window):z[i]
        - pd.offsets.Day(1)].corr()
    corr.values[[np.arange(corr.shape[1])] * 2] = 0
    temp1 = pd.DataFrame(corr.values[np.triu_indices(corr.shape[1])])
    temp1 = temp1[(temp1.T != 0).all()].T
    temp1.columns = names.stack()
    cor_table = pd.concat([cor_table, temp1])

# get the list of pairs having values>t_level

cor_vec = []
for i in range(len(cor_table)):
    cor_vec.append([pd.DataFrame(cor_table[i:i + 1].loc[:,
                   cor_table[i:i + 1].max() > t_level].columns)])

cor_vector = []
cor_vector = [pd.DataFrame(cor_vec[i][0][0].str.split('_')) for i in
              range(len(cor_vec))]

lengths = [len(cor_vector[i]) for i in range(len(cor_vector))]  # get the length of no. of dataframe in each list

tick_curr.index = tick_curr['Ticker']
tcost.index = tcost['Ticker']

# roll_freq='months'  #'weeks', 'months', 'quarters', and 'years'

if roll_freq == 'monthly':
    grouped_data = temp.groupby(pd.TimeGrouper('M')).nth(0)
    grouped_data1 = temp.groupby(pd.TimeGrouper('M')).nth(-1)
elif roll_freq == 'quarterly':
    grouped_data = temp.groupby(pd.TimeGrouper('Q')).nth(0)
    grouped_data1 = temp.groupby(pd.TimeGrouper('Q')).nth(-1)
elif roll_freq == 'yearly':
    grouped_data = temp.groupby(pd.TimeGrouper('Y')).nth(0)
    grouped_data1 = temp.groupby(pd.TimeGrouper('Y')).nth(-1)
elif roll_freq == 'weekly':
    grouped_data = temp.groupby(pd.TimeGrouper('W')).nth(0)
    grouped_data1 = temp.resample('W')

a = pd.DataFrame(names.T)
a.columns = ['pairs']

st_month1 = str(int(st_month) - 1)

end_yr = '2016'
end_month = '07'
strt_dt1 = st_yr + '-' + st_month1

dat_ret2 = dat_ret1[strt_dt1:]
one = dat_ret2[a['pairs'].str[0:9]]
two = dat_ret2[a['pairs'].str[10:]]

diff = one - two.values
diff.columns = a
diff = diff.loc[diff.index[2]:]

# diff=diff.replace([np.inf,-np.inf], 0)

m = pd.rolling_mean(diff, 20).shift()
s = pd.rolling_std(diff, 20).shift()
diff = (diff - m) / s

# diff=diff.apply(zscore)

signals = pd.DataFrame(np.where(diff < -1 * z_score_level, 1,
                       np.where(diff > z_score_level, -1, 0)),
                       index=diff.index, columns=diff.columns)

# all months end date signals

month_endsignals = signals.groupby(pd.TimeGrouper('M')).nth(-1)

month_endsignals=month_endsignals[:-1]

for i in range(len(cor_vec)):
    for j in range(len(month_endsignals.columns)):
        if (month_endsignals.ix[i].index[j][0] in str(np.array(cor_vec[i-1][0])) and (month_endsignals.ix[i].index[j][0] in str(np.array(cor_vec[i][0])))):
            pass
        else:
            month_endsignals.ix[i][j]=0
        
# getting returns for all pairs of cor_vector

temp1 = []
for i in range(len(grouped_data)):
    for j in range(len(cor_vector[i])):
        if dat_ret1.index.get_loc(grouped_data.index[i]) \
            - lag_indicator < 0:
            temp1.append(dat_ret1[cor_vector[i][0][j]].loc[dat_ret1.index[dat_ret1.index.get_loc(grouped_data.index[i])
                         - dat_ret1.index.get_loc(grouped_data.index[i])]:grouped_data1.index[i]])
        else:
            temp1.append(dat_ret1[cor_vector[i][0][j]].loc[dat_ret1.index[dat_ret1.index.get_loc(grouped_data.index[i])
                         - lag_indicator]:grouped_data1.index[i]])

# last dates for each month

last_dates = [x.groupby(pd.TimeGrouper('M')).nth(-1).index[0] for x in
              temp1]


def fx(x):
    [x.insert(2, 'diff', x[[0]] - x[[1]].values) for x in temp1]
    [x.insert(3, 'zscore', zscore(x[[2]].interpolate())) for x in temp1]


fx(temp1)

zscore1 = pd.DataFrame([x['zscore'] for x in temp1]).T

# for returns of price having 0

temp2 = []
for i in range(len(grouped_data)):
    for j in range(len(cor_vector[i])):
        if dat_ret.index.get_loc(grouped_data.index[i]) - lag_indicator \
            < 0:
            temp2.append(dat_ret[cor_vector[i][0][j]].loc[dat_ret.index[dat_ret.index.get_loc(grouped_data.index[i])
                         - dat_ret.index.get_loc(grouped_data.index[i])]:grouped_data1.index[i]])
        else:
            temp2.append(dat_ret[cor_vector[i][0][j]].loc[dat_ret.index[dat_ret.index.get_loc(grouped_data.index[i])
                         - lag_indicator]:grouped_data1.index[i]])

# def fx1(x):
#    [(x.insert( 2,'diff',x[[0]]-x[[1]].values)) for x in (temp2)]
#    for i in range(len(temp2)):
#        temp2[i]=pd.concat([temp2[i],zscore1[[i]]],axis=1).dropna(how='all')
#    [(x.insert( 4,'Signal1',np.where(x[[3]] < -1 * z_score_level, 1,np.where(x[[3]] > z_score_level, -1,0)))) for x in (temp2)]
#    [(x.insert( 5,'Signal',x['Signal1'].shift())) for x in (temp2)]
#    [(x.drop('Signal1', axis=1, inplace=True)) for x in (temp2)]
#    for i in range(len(temp2)):
#        for j in range(len(signals.columns)):
#            if (temp2[i].columns[0]==month_endsignals.columns[j][-1][0:9] and temp2[i].columns[1]==month_endsignals.columns[j][-1][10:]):
#                temp2[i].loc[temp2[i].groupby(pd.TimeGrouper('M')).nth(0).index[1]]['Signal']=month_endsignals[[j]].loc[last_dates[i]]
#
#    for i in range(len(temp2)):
#        a=np.where(np.any(dat[temp2[i].columns[0:2]].loc[temp2[i].index]==0, axis=1))[0]
#        b=a+1
#        if len(a)==0:
#            continue
#        for j in range(len(a)):
#            if a[j]<temp2[i].shape[0]:
#                temp2[i].iloc[a[j]]['Signal']=0
#            if b[j]<temp2[i].shape[0]:
#                temp2[i].iloc[b[j]]['Signal']=0
#
#    [(x.insert( 5,'cur1',(new_loc[x.columns[0]].loc[x[[0]].index]
#                     * x[x.columns[0]].shift(-1)).shift())) for x in (temp2)]
#    [(x.insert( 6,'cur2',(new_loc[x.columns[1]].loc[x[[1]].index]
#                     * x[x.columns[1]].shift(-1)).shift())) for x in (temp2)]
#
#    for temps in temp2:
#        if tick_curr.loc[temps.columns[0]]['Currency Pair'] != 'USD':
#            temps[tick_curr.loc[temps.columns[0]]['Currency Pair']] = \
#                fx_data.loc[temps.index][tick_curr.loc[temps.columns[0]]['Currency Pair'
#                    ]]
#            if (temps.columns[7])[0:3] == 'USD':
#                temps['cur1'] = temps[[5]] / temps[[7]].values
#            else:
#                temps['cur1'] = temps[[5]] * temps[[7]].values
#
#        if tick_curr.loc[temps.columns[1]]['Currency Pair'] != 'USD':
#
#                temps[tick_curr.loc[temps.columns[1]]['Currency Pair']] = \
#                    fx_data.loc[temps.index][tick_curr.loc[temps.columns[1]]['Currency Pair'
#                        ]]
#                if (tick_curr.loc[temps.columns[1]]['Currency Pair'])[0:3] \
#                    == 'USD':
#                    temps['cur2'] = temps[[6]] / temps[[temps.shape[1]
#                            - 1]].values
#                else:
#                    temps['cur2'] = temps[[6]] * temps[[temps.shape[1]
#                            - 1]].values
#
#
#    [(x.insert( 7,'leg1',(stocks_units.loc[x.index][x.columns[0]].shift(1)* tcost.loc[x.columns[0]][1]))) for x in (temp2)]
#    [(x.insert( 8,'leg2',(stocks_units.loc[x.index][x.columns[1]].shift(1)* tcost.loc[x.columns[1]][1]))) for x in (temp2)]
#
#    for temps in temp2:
#        if tick_curr.loc[temps.columns[0]][1] != 'USD':
#            if (tick_curr.loc[temps.columns[0]][2])[0:3] == 'USD':
#                    temps['leg1'] = temps['leg1'] \
#                        / fx_data[tick_curr.loc[temps.columns[0]][2]].loc[temps.index].values
#            else:
#                    temps['leg1'] = temps['leg1'] \
#                        * fx_data[tick_curr.loc[temps.columns[0]][2]].loc[temps.index].values
#
#        if tick_curr.loc[temps.columns[1]][1] != 'USD':
#            if (tick_curr.loc[temps.columns[1]][2])[0:3] == 'USD':
#                    temps['leg2'] = temps['leg2'] \
#                        / fx_data[tick_curr.loc[temps.columns[1]][2]].loc[temps.index].values
#            else:
#                    temps['leg2'] = temps['leg2'] \
#                        * fx_data[tick_curr.loc[temps.columns[1]][2]].loc[temps.index].values
#
#    [(x.insert( 9,'gross_gain1',(x['Signal'] * x['cur1'] - x['Signal'] * x['cur2']))) for x in (temp2)]
#    [(x.insert( 10,'net_gain1',(x['gross_gain1'] - (x['leg1']+ x['leg2']) * abs(x['Signal'])))) for x in (temp2)]
#    [(x.insert(11,'investleg1',(new_loc[x.columns[0]].loc[x[[0]].index])))for x in (temp2)]
#    [(x.insert(12,'investleg2',(new_loc[x.columns[1]].loc[x[[1]].index])))for x in (temp2)]
#
#
#    for temps in temp2:
#        if tick_curr.loc[temps.columns[0]]['Currency Pair'] != 'USD':
#
#
#                if (temps.columns[7])[0:3] == 'USD':
#                    temps['investleg1'] = temps['investleg1'] \
#                        / temps[tick_curr.loc[temps.columns[0]]['Currency Pair'
#                                ]]
#                else:
#                    temps['investleg1'] = temps['investleg1'] \
#                        * temps[tick_curr.loc[temps.columns[0]]['Currency Pair'
#                                ]]
#
#
#        if tick_curr.loc[temps.columns[1]]['Currency Pair'] != 'USD':
#                if (tick_curr.loc[temps.columns[1]]['Currency Pair'])[0:3] \
#                    == 'USD':
#                    temps['investleg2'] = temps['investleg2'] \
#                        / temps[tick_curr.loc[temps.columns[1]]['Currency Pair'
#                                ]]
#                else:
#                    temps['investleg2'] = temps['investleg2'] \
#                        * temps[tick_curr.loc[temps.columns[1]]['Currency Pair'
#                                ]]
#
#    [(x.insert( 13,'invest_leg1',x['investleg1'].shift())) for x in (temp2)]
#    [(x.insert( 14,'invest_leg2',x['investleg2'].shift())) for x in (temp2)]
#    [(x.insert( 15,'total_invest(USD)',x['invest_leg1']+ x['invest_leg2'])) for x in (temp2)]
#    [(x['total_invest(USD)']*abs(x['Signal'])) for x in (temp2)]
#    [(x.insert(16,'total_tcost',(x['leg1'] + x['leg2'])* abs(x['Signal'])))for x in (temp2)]
#    [(x.insert(17,'total_grossret',(x['cur1'] * x['Signal'] / x['invest_leg1'] + x['cur2'] * x['Signal'] * -1 / x['invest_leg2'])))for x in (temp2)]
#    [(x.insert(18,'leg1_ret',((x['cur1']*x['Signal'])-x['leg1']*abs(x['Signal']))/x['invest_leg1']))for x in (temp2)]
#    [(x.insert(19,'leg2_ret',((x['cur2']*x['Signal']*-1)-x['leg2']*abs(x['Signal']))/x['invest_leg2']))for x in (temp2)]
#    [(x.insert(20,'total_ret',(x['leg1_ret']+x['leg2_ret'].values)))for x in (temp2)]
#    [(x.insert(21,'gross_gain',(x['gross_gain1']*abs(x['Signal']))))for x in (temp2)]
#    [[(x.insert(22,'net_gain',(x['net_gain1']*abs(x['Signal']))))for x in (temp2)]]

fx1(temp2)

result = [temp2[end - length:end] for (length, end) in zip(lengths,
          accumulate(lengths))]

temp3 = []
for i in range(len(result)):
    for j in range(len(result[i])):
        result[i][j] = \
            result[i][j].loc[grouped_data.index[i]:grouped_data1.index[i]]
        temp3.append(result[i][j])


#a list having net gain, gross gain, non zero elements, winning count
df1 = []
df2 = []
df3 = []
for i in range(len(result) - 1):
    sum_ng = pd.DataFrame([x['net_gain'] for x in
                          result[i]]).T.sum(axis=1)
    sum_gg = pd.DataFrame([x['gross_gain'] for x in
                          result[i]]).T.sum(axis=1)
    sum_totret = pd.DataFrame([x['total_ret'] for x in
                              result[i]]).T.sum(axis=1)
    a = pd.DataFrame([x['net_gain'] for x in
                     result[i]]).T.replace(np.nan, 0)
    non_zero = a.astype(bool).sum(axis=1)
    b = pd.DataFrame([x['total_ret'] for x in
                     result[i]]).T.replace(np.nan, 0)
    non_zerototret = b.astype(bool).sum(axis=1)
    if a.shape[1] == 1:
        positive = np.sign(a).replace('-1', 0)
    else:
        positive = pd.DataFrame(a.net_gain.T.apply(lambda x: \
                                pd.Series([(x
                                > 0).sum(axis=0)])).stack()[0])
    sum_totret = sum_totret / non_zerototret
    df = pd.concat([sum_gg, sum_ng, non_zero, positive, sum_totret],
                   axis=1)
    df.columns = ['sum_grossgains', 'sum_netgains', 'nonzero_elements',
                  'positive', 'total_ret']
    df1.append(df)
    df2.append(df1[i]['sum_netgains'].sum())
    df3.append(df1[i]['sum_grossgains'].sum())

# sum of net gains and gross gain for all months

ng_gg = pd.concat([pd.DataFrame(df2), pd.DataFrame(df3)], axis=1)
ng_gg.columns = ['sum_netgains', 'sum_gross_gains']

			
   
