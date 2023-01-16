from prometheus_api_client import PrometheusConnect, MetricsList, MetricSnapshotDataFrame, MetricRangeDataFrame
from datetime import timedelta

from prometheus_api_client.utils import parse_datetime

import pandas as pd
import matplotlib.pyplot as plt
#%matplotlib inline

prom = PrometheusConnect(url="http://15.160.61.227:29090", disable_ssl=True)



#---------------CALCOLO SET DI METADATI-----------------------------------------------------------------------------------------------
#prende a partire da ora, la metrica, nei 10 minuti passati, avaibleMem relativa ai nodi con job = summary. Campiona 

label_config = {'job': 'summary'}
start_time = parse_datetime("5w") 
end_time = parse_datetime("now")
chunk_size = timedelta(days=1) #??discuterne con gli altri campione? oppure insieme di valori presi in un giorno? dagli appunti: Un chunks è una raccolta di campioni in un intervallo di tempo per una particolare serie.

metric_data = prom.get_metric_range_data(
        metric_name = "availableMem",
        label_config = label_config,
        start_time = start_time,
        end_time = end_time,
        chunk_size = chunk_size,
    )

#---------------PUNTO 3: PREDIZIONE DI MAX, MIN, AVG NEI SUCCESSIVI 10MIN---------------------------------------------------------------------
metric_object_list = MetricsList(metric_data) 
my_metric_object = metric_object_list[0]
my_metric_object.metric_values.to_csv('node_filesystem_avail_bytes.csv',index=False)
        #scriviamo i valori calcolati in un file
ts = pd.read_csv('./node_filesystem_avail_bytes.csv', header=0, parse_dates=[0], dayfirst=True, index_col=0) #leggo il file
ts.index #stampa i nomi delle righe, che nel caso nostro sono i timestamp

#esplicito la frequenza perchè non riesce a capire il tempo di campionameto?
ricampionamento = 60
tsr = ts.resample(rule=str(ricampionamento)+'T').mean()
tsr.index #vediamo se ci da la frequenza
tsr.plot() #vediamo graficamente la serie 
tsr

print(tsr.size)
print(int(tsr.size*0.9)) #utilizzo il 90% dei campioni per fare l training
print((tsr.size-int(tsr.size*0.9)))

#Split training and test data (80/20% 0r 90/10)
train_data = tsr.iloc[:int(tsr.size*0.9)]
test_data = tsr.iloc[int(tsr.size*0.9):]
print(train_data.size)
print(test_data.size)

test_data

from statsmodels.tsa.holtwinters import ExponentialSmoothing
tsmodel = ExponentialSmoothing(train_data, trend='add', seasonal='add',seasonal_periods=1440*2/ricampionamento).fit()

prediction = tsmodel.forecast(1440/ricampionamento)

plt.figure(figsize=(24,10))
#add axes labels and a title
plt.ylabel('Values', fontsize=14)
plt.xlabel('Time', fontsize=14)
plt.title('Values over time', fontsize=16)

plt.plot(train_data, "-", label = 'train')
plt.plot(test_data,"-",label = 'real')
plt.plot(prediction,"--",label = 'pred')
#add legend
plt.legend(title='Series')

plt.figure(figsize=(24,10))
#add axes labels and a title
plt.ylabel('Values', fontsize=14)
plt.xlabel('Time', fontsize=14)
plt.title('Values over time', fontsize=16)
plt.plot(test_data,"-",label = 'real')
plt.plot(prediction,"--",label = 'pred')
#add legend
plt.legend(title='Series')

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

plt.show()
#----------------PUNTO 2: CALCOLO DEL VALORE DI MAX, MIN, AVG, DEV_STD DELLE METRICHE DI 1h, 3h, 12------------------------

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


    
'''

ts = pd.read_csv('./node_filesystem_avail_bytes.csv', header=0, parse_dates=[0], dayfirst=True, index_col=0) #leggo il file
ts.index #stampa i nomi delle righe, che nel caso nostro sono i timestamp

#esplicito la frequenza perchè non riesce a capire il tempo di campionameto?
ricampionamento = 60
tsr = ts.resample(rule=str(ricampionamento)+'T').mean()
tsr.index #vediamo se ci da la frequenza
tsr.plot() #vediamo graficamente la serie 
tsr

print(tsr.size)
print(int(tsr.size*0.9)) #utilizzo il 90% dei campioni per fare l training
print((tsr.size-int(tsr.size*0.9)))

#Split training and test data (80/20% 0r 90/10)
train_data = tsr.iloc[:int(tsr.size*0.9)]
test_data = tsr.iloc[int(tsr.size*0.9):]
print(train_data.size)
print(test_data.size)

test_data

from statsmodels.tsa.holtwinters import ExponentialSmoothing
tsmodel = ExponentialSmoothing(train_data, trend='add', seasonal='add',seasonal_periods=1440*2/60).fit()

prediction = tsmodel.forecast(1440/60)


prediction

plt.figure(figsize=(24,10))
#add axes labels and a title
plt.ylabel('Values', fontsize=14)
plt.xlabel('Time', fontsize=14)
plt.title('Values over time', fontsize=16)

plt.plot(train_data, "-", label = 'train')
plt.plot(test_data,"-",label = 'real')
plt.plot(prediction,"--",label = 'pred')
#add legend
plt.legend(title='Series')

plt.figure(figsize=(24,10))
#add axes labels and a title
plt.ylabel('Values', fontsize=14)
plt.xlabel('Time', fontsize=14)
plt.title('Values over time', fontsize=16)
plt.plot(test_data,"-",label = 'real')
plt.plot(prediction,"--",label = 'pred')
#add legend
plt.legend(title='Series')

'''