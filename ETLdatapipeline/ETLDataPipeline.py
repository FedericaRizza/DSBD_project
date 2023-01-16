from prometheus_api_client import PrometheusConnect, MetricsList, MetricSnapshotDataFrame, MetricRangeDataFrame
from datetime import timedelta

from prometheus_api_client.utils import parse_datetime

import pandas as pd
import matplotlib.pyplot as plt
#%matplotlib inline

prom = PrometheusConnect(url="http://15.160.61.227:29090", disable_ssl=True)

#prova commit
#prova commit 2

#%%---------------CALCOLO SET DI METADATI-----------------------------------------------------------------------------------------------
#prende a partire da ora, la metrica, nei 10 minuti passati, avaibleMem relativa ai nodi con job = summary. Campiona 

label_config = {'job': 'summary'}
start_time = parse_datetime("2w") 
end_time = parse_datetime("now")
chunk_size = timedelta(days=1) #??discuterne con gli altri campione? oppure insieme di valori presi in un giorno? dagli appunti: Un chunks è una raccolta di campioni in un intervallo di tempo per una particolare serie.

metric_data = prom.get_metric_range_data(
        metric_name = "availableMem",
        label_config = label_config,
        start_time = start_time,
        end_time = end_time,
        chunk_size = chunk_size,
    )

#%%---------------PUNTO 3: PREDIZIONE DI MAX, MIN, AVG NEI SUCCESSIVI 10MIN---------------------------------------------------------------------
metric_object_list = MetricsList(metric_data)
my_metric_object = metric_object_list[0] #ogni elemento della lista è la serie temporale di un'istanza di job
my_metric_object.metric_values.to_csv('./ETLDataPipeline/availableMem.csv',index=False)
        #scriviamo i valori calcolati in un file
ts = pd.read_csv('./ETLDataPipeline/availableMem.csv', header=0, parse_dates=[0], dayfirst=True, index_col=0) #leggo il file
#print("indici: ", ts.index) #stampa i nomi delle righe, che nel caso nostro sono i timestamp
#print(ts)
#ts.plot()

#%%----------------DECOMPOSIZIONE----------------------------------------

from statsmodels.tsa.filters.hp_filter import hpfilter
# Tuple unpacking
ts_cycle, ts_trend = hpfilter(ts, lamb=1600)  #1600 suggested value that works fine with 

#define size
plt.figure(figsize=(20,8))
#add axes labels and a title
plt.ylabel('Values', fontsize=14)
plt.xlabel('Time', fontsize=14)
plt.title('Values over time', fontsize=16)
plt.plot(ts, "-", label = 'ts')
plt.plot(ts_trend,"--", label = 'ts_t')
plt.plot(ts_cycle,".", label = 'ts_c')
plt.legend(title='Series')


from statsmodels.tsa.seasonal import seasonal_decompose
#esplicito la frequenza perchè non riesce a capire il tempo di campionameto?
ricampionamento = 1
tsr = ts.resample(rule=str(ricampionamento)+'T').mean()

result= seasonal_decompose(tsr, model='add', period=6)
result.plot()

#define size
plt.figure(figsize=(24,10))
#add axes labels and a title
plt.ylabel('Values', fontsize=14)
plt.xlabel('Time', fontsize=14)
plt.title('Values over time', fontsize=16)
plt.plot(ts.iloc[:250], "-", label = 'ts')
plt.plot(result.trend.iloc[:250],"*", label = 'ts_t')
plt.plot(result.seasonal.iloc[:250],"--", label = 'ts_s')
plt.plot(result.resid.iloc[:250],".", label = 'ts_err')
plt.legend(title='Series')

plt.show()

#tsr = ts.resample(rule='T').mean()
#print("indici resample: ", tsr.index) #vediamo se ci da la frequenza
#print(tsr)
#tsr.plot() #vediamo graficamente la serie 
'''
print(tsr.size)
print(int(tsr.size*0.9)) #utilizzo il 90% dei campioni per fare l training
print((tsr.size-int(tsr.size*0.9)))
'''
#Split training and test data (80/20% 0r 90/10)
train_data = tsr.iloc[:int(tsr.size*0.9)] 
test_data = tsr.iloc[int(tsr.size*0.9):]
'''print(train_data.size)
print(test_data.size)'''

#test_data

from statsmodels.tsa.holtwinters import ExponentialSmoothing
tsmodel = ExponentialSmoothing(train_data, trend='add', seasonal=None).fit()
#tsmodel

prediction = tsmodel.forecast(10)
#prediction

plt.figure(figsize=(20,10))
#add axes labels and a title
plt.ylabel('Values', fontsize=14)
plt.xlabel('Time', fontsize=14)
plt.title('Values over time', fontsize=16)

plt.plot(train_data, "-", label = 'train')
plt.plot(test_data,"-",label = 'real')
plt.plot(prediction,"--",label = 'pred')
#add legend
plt.legend(title='Series')

plt.figure(figsize=(20,10))
#add axes labels and a title
plt.ylabel('Values', fontsize=14)
plt.xlabel('Time', fontsize=14)
plt.title('Values over time', fontsize=16)
plt.plot(test_data,"-",label = 'real')
plt.plot(prediction,"--",label = 'pred')
#add legend
plt.legend(title='Series')
'''
tsmodel = ExponentialSmoothing(tsr, trend='add', seasonal='add',seasonal_periods=1440*2/ricampionamento).fit()
prediction = tsmodel.forecast(1440/ricampionamento)

plt.figure(figsize=(24,10))
#add axes labels and a title
plt.ylabel('Values', fontsize=14)
plt.xlabel('Time', fontsize=14)
plt.title('Values over time', fontsize=16)

plt.plot(tsr, "-", label = 'timeserie')
#plt.plot(test_data,"-",label = 'real')
plt.plot(prediction,"--",label = 'pred')
#add legend
plt.legend(title='Series')

plt.show()'''
#%%----------------PUNTO 2: CALCOLO DEL VALORE DI MAX, MIN, AVG, DEV_STD DELLE METRICHE DI 1h, 3h, 12h------------------------
'''
#TODO:scegliere 10-20 metriche
metric_set = ["availableMem", "diskUsage"]

for metric in metric_set:
    metric_data = prom.get_metric_range_data(
        metric_name = metric,
        label_config = label_config,
        start_time = parse_datetime("1h"),
        end_time = end_time,
        chunk_size = timedelta(minutes=5),
    )

    metric_object_list = MetricsList(metric_data) 
                                                  
    for item in metric_object_list:
        print(item.metric_name, item.label_config, "\n") #stampo nome della metrica e label


    for i2 in range(len(metric_object_list)): #35 perchè per 5 settimane suddividiamo in serie(chunck) di un giorno
        my_metric_object = metric_object_list[i2] 
        print("my_metric_object")
        print(my_metric_object)
        metric_df = MetricRangeDataFrame(metric_data) #la convertiamo per manipolarla con pandas
        print(metric_df.head()) #stampa le prime n(default=5) righe per vedere se gli indici sono corretti 
        print(metric_df)
        param1 = metric
        max = metric_df['value'].max()
        min = metric_df['value'].min()
        avg = metric_df['value'].mean()
        dev_std = metric_df['value'].std()
        print(param1, max, min, avg, dev_std)



        my_metric_object.plot()
        print(my_metric_object)
        print("Tipo: ", my_metric_object.metric_values)
        print(type(my_metric_object.metric_values))
        print(type(my_metric_object))
'''
'''
    metric_data = prom.get_metric_range_data(
        metric_name = i,
        label_config = label_config,
        start_time = parse_datetime("3h"),
        end_time = end_time,
        chunk_size = timedelta(minutes=15),
    )

    metric_data = prom.get_metric_range_data(
        metric_name = i,
        label_config = label_config,
        start_time = parse_datetime("12h"),
        end_time = end_time,
        chunk_size = timedelta(hours=1),
    ) 
    
''' 