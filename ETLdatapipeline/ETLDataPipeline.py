from prometheus_api_client import PrometheusConnect, MetricsList, MetricSnapshotDataFrame, MetricRangeDataFrame
from datetime import timedelta

from prometheus_api_client.utils import parse_datetime

from confluent_kafka import Producer, KafkaError, KafkaException
import json
import time
import sys


import pandas as pd
import matplotlib.pyplot as plt
#%matplotlib inline

def delivery_callback(err, msg):
        if err:
            sys.stderr.write('%% Message failed delivery: %s\n' % err)
        else:
            sys.stderr.write('%% Message delivered to %s, partition[%d] @ %d\n' %
                             (msg.topic(), msg.partition(), msg.offset()))

configuration = {'bootstrap.servers': 'broker_kafka:9092'}
topic = "prometheusdata" 

try: 
    producer = Producer(**configuration)
    print("producer connesso")
except KafkaException as error:
    print("Errore: ", error)
#print(producer)

print("Connessione in corso...")
prom = PrometheusConnect(url="http://15.160.61.227:29090", disable_ssl=True)
#prom = PrometheusConnect(url="http://prom:9090", disable_ssl=True)
print("Connessione avvenunata correttamente")


label_config = {'job' : 'summary'} #{'nodeName': 'sv192'} #{}
start_time = parse_datetime("2w") 
end_time = parse_datetime("now")
chunk_size = timedelta(days=1) #??discuterne con gli altri campione? oppure insieme di valori presi in un giorno? dagli appunti: Un chunks è una raccolta di campioni in un intervallo di tempo per una particolare serie.


metric_set = ["cpuLoad", "cpuTemp", "diskUsage"]
#metric_set = ["node"]
#metric = 0
while True:
    for metric in metric_set:

        metric_data = prom.get_metric_range_data (
            metric_name = metric,
            label_config = label_config,
            start_time = start_time,
            end_time = end_time,
            chunk_size = chunk_size,
        )

        #metric_object_list = MetricsList(metric_data)

        '''for item in metric_object_list:
            print(item.metric_name, item.label_config, "\n")'''

        '''metric_data = prom.get_metric_range_data (
                metric_name = ["cpuLoad", "cpuTemp", "diskUsage"],
                label_config = label_config,
                start_time = start_time,
                end_time = end_time,
                chunk_size = chunk_size,
        )'''

        print("metriche ricevute!")

#---------------PUNTO 2: calcoli il valore di max, min, avg, dev_std della metriche per 1h,3h, 12h --------------------


        for i in metric_data:
            metric_rdf = MetricRangeDataFrame(i)
            print(metric_rdf)
            metric_rdf_1h = metric_rdf.last("1h")
            min1 = metric_rdf_1h['value'].min()
            max1 = metric_rdf_1h['value'].max()
            avg1 = metric_rdf_1h['value'].mean()
            dev_std1 = metric_rdf_1h['value'].std()
            print('max1h:', max1,'min1h:', min1, 'media1h:',  avg1, 'deviazione standard1h:', dev_std1, "\n")
            metric_rdf_3h = metric_rdf.last("3h")
            min3 = metric_rdf_3h['value'].min()
            max3 = metric_rdf_3h['value'].max()
            avg3 = metric_rdf_3h['value'].mean()
            dev_std3 = metric_rdf_3h['value'].std()
            print('max3h:', max3,'min3h:', min3, 'media3h:',  avg3, 'deviazione standard3h:', dev_std3, "\n")
            metric_rdf_12h = metric_rdf.last("12h")
            min12 = metric_rdf_12h['value'].min()
            max12 = metric_rdf_12h['value'].max()
            avg12 = metric_rdf_12h['value'].mean()
            dev_std12 = metric_rdf_12h['value'].std()
            print('max12h:', max12,'min12h:', min12, 'media12h:',  avg12, 'deviazione standard12h:', dev_std12, "\n")
            dati_dictionary = {
                "metric_name" : i['metric'], 
                "max_1h" : max1,
                "max_3h" : max3,
                "max_12h" : max12,
                "min_1h" : min1,
                "min_3h" : min3,
                "min_12h" : min12,
                "avg_1h" : avg1,
                "avg_3h" : avg3,
                "avg_12h" : avg12,
                "devstd_1h" : dev_std1,
                "devstd_3h" : dev_std3,
                "devstd_12h" : dev_std12,
                "max_predicted" : 40.0, #valore a caso per provare
                "min_predicted" : 2.0, #valore a caso per provare
                "avg_predicted" : 24.0, #valore a caso per provare
                "autocorrelazione" : 11.1, #valore a caso per provare, avevo messo float nel db
                "stazionarieta" : 10.0, #avevo messo float nel db, per il momento è una prova
                "stagionalita" :    12.1 #//
            }
            

            try:
                producer.produce(topic, value = json.dumps(dati_dictionary) , callback = delivery_callback)
            except BufferError:
                print('%% coda piena! (%d messaggi in attesa): riprova\n' % len(producer))
            except KafkaException as e:
                print("Errore: ",e)
            producer.poll(0) #timeout impostato su 0 quindi la poll ritorna immediatamente

        time.sleep(10)

#%%---------------PUNTO 3: PREDIZIONE DI MAX, MIN, AVG NEI SUCCESSIVI 10MIN---------------------------------------------------------------------
'''metric_object_list = MetricsList(metric_data)
my_metric_object = metric_object_list[0] #ogni elemento della lista è la serie temporale di un'istanza di job
my_metric_object.metric_values.to_csv('./ETLDataPipeline/availableMem.csv',index=False)
        #scriviamo i valori calcolati in un file
ts = pd.read_csv('./ETLDataPipeline/availableMem.csv', header=0, parse_dates=[0], dayfirst=True, index_col=0) #leggo il file
#print("indici: ", ts.index) #stampa i nomi delle righe, che nel caso nostro sono i timestamp
#print(ts)
#ts.plot()'''

#%%----------------DECOMPOSIZIONE-------------------------------------------------------------------------------------------------------------
'''
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

#plt.show()

'''

#%%------------------------------FORECASTING---------------------------------------------------------------------------------------------------
'''
#tsr = ts.resample(rule='T').mean()
#print("indici resample: ", tsr.index) #vediamo se ci da la frequenza
#print(tsr)
#tsr.plot() #vediamo graficamente la serie 

#commento_sotto
print(tsr.size)
print(int(tsr.size*0.9)) #utilizzo il 90% dei campioni per fare l training
print((tsr.size-int(tsr.size*0.9)))


#Split training and test data (80/20% 0r 90/10)

train_data = tsr.iloc[:int(tsr.size*0.9)] 
test_data = tsr.iloc[int(tsr.size*0.9):]


#commento_sotto
print(train_data.size)
print(test_data.size)



#test_data

from statsmodels.tsa.holtwinters import ExponentialSmoothing
tsmodel = ExponentialSmoothing(train_data, trend='add', seasonal=None).fit()
#tsmodel

prediction = tsmodel.forecast(10)
#prediction

#error evaluation: confronto gli errori con la deviazione standard, se err<std buon modello(?)
from sklearn.metrics import mean_squared_error, mean_absolute_error
import numpy as np
error_list= [mean_absolute_error(test_data.iloc[:prediction.size], prediction), mean_squared_error(test_data.iloc[:prediction.size],prediction), np.sqrt(mean_squared_error(test_data.iloc[:prediction.size], prediction)), test_data.describe(), test_data.values.std()]
for j in range(len(error_list)):
    print(error_list[j])

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


#commento_sotto
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
'''