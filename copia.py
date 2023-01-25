from prometheus_api_client import PrometheusConnect, MetricsList, MetricSnapshotDataFrame, MetricRangeDataFrame
from datetime import timedelta
from prometheus_api_client.utils import parse_datetime
import os
import time
from confluent_kafka import Producer, KafkaError, KafkaException
import sys
import json

import grpc
import sla_pb2_grpc
import sla_pb2

from numpy.fft import rfft, rfftfreq
import math
import numpy as np
import matplotlib.pyplot as plt
from statsmodels.tsa.stattools import adfuller, acf
from statsmodels.tsa.seasonal import seasonal_decompose
from statsmodels.tsa.holtwinters import ExponentialSmoothing
from statsmodels.tsa.arima.model import ARIMA,ARIMAResults
from sklearn.metrics import mean_squared_error
from pmdarima import auto_arima
import pandas as pd


# Ignore harmless warnings
import warnings
warnings.filterwarnings("ignore")



from prometheus_client import Gauge

g1h = Gauge('etl_executiontime_1h', 'ETL execution time for calculating 1h values', ["metric_name"])
g3h = Gauge('etl_executiontime_3h', 'ETL execution time for calculating 3h values', ["metric_name"])
g12h = Gauge('etl_executiontime_12h', 'ETL execution time for calculating 12h values', ["metric_name"])

gMeta = Gauge('etl_executiontime_metadata', 'ETL execution time for calculating metadata', ["metric_name"])
gPre = Gauge('etl_executiontime_prediction', 'ETL execution time for calculating prediction', ["metric_name"])

conf = {'bootstrap.servers': os.environ["KAFKA_BROKER"]}
topic = os.environ["KAFKA_TOPIC"]

try: 
    p = Producer(**conf)
    print("Connesso con successo")
except KafkaException as ke:
    print("Errore: ", ke)
print(p)


def delivery_callback(err, msg):
    if err:
        # stampiamo un errore e non riproviamo. Accettiamo di perdere un campione
        sys.stderr.write('%% Message failed delivery: %s\n' % err)
    else:
        sys.stderr.write('%% Message delivered to %s, partition[%d] @ %d\n' %
                            (msg.topic(), msg.partition(), msg.offset()))


# Usiamo le variabili di ambiente per non dover ricreare l'immagine se dovesse cambiare l'endpoint
prom_url = "http://" + os.environ["PROMETHEUS"] + ":" + os.environ["PROMETHEUS_PORT"]
prom = PrometheusConnect(url=prom_url, disable_ssl=True)


metric_name= 'node_filesystem_avail_bytes'
label_config = {}#{'mountpoint':'/'}#{'nodeName': 'sv122', 'job': 'summary'}



def everyHour():
    start_time = parse_datetime("3d") #TODO
    end_time = parse_datetime("now")

    # Ottengo le metriche da prometheus
    try:
        metric_data = prom.get_metric_range_data(
            metric_name=metric_name,
            label_config=label_config,
            start_time=start_time,
            end_time=end_time,
        )
    except Exception as err:
        print("Errore di connessione: {}".format(err), file=sys.stderr)
        return

    print("Ho ricevuto i dati")
    # Controllo che il filtro "matchi" effettivamente qualche metrica
    if len(metric_data) == 0:
        # Ricostruisco il filtro come se fosse PromQL
        str = "Non è presente alcuna metrica che corrisponde al filtro: " + metric_name
        if len(label_config) > 0 :
            str = str + "{"
            for elem in label_config:
                str = str + elem + "='" + label_config[elem] + "', "
            str = str[:-2] + "}"
        print(str)
    else:
        # Per ogni metrica ritornata dal prometheus
        for metric_data_i in metric_data:
            ris = {}
            ris['name'] = metric_data_i['metric']
            df = MetricRangeDataFrame(metric_data_i)
            df = df.dropna()
            # TODO calcolare i metadati (in un altro task con periodo maggiore)

            start = time.time()

            if df['value'].min() != df['value'].max():
                # Stazionarietà
                stationarity = {}
                adft = adfuller(df['value'],autolag='AIC') 
                stationarity['stationary'] = bool(adft[1] < 0.05)
                stationarity['p_value'] = adft[1]
            
                # Autocorrelazione
                acf_res = {}
                a, b = acf(df['value'], alpha=.05)
                print(a)
                for i in range(0,len(a)):
                    if a[i] <= b[i][0]-a[i] or a[i] >= b[i][1]-a[i]:
                        acf_res[i] = a[i]


                # Stagionalità
                seasonality = {}
                fft_val = abs(rfft(df['value']))
                fft_freq = rfftfreq(len(df['value']), d=1.0)
                fft_val = fft_val[2:]
                fft_freq = fft_freq[2:]
                max_freq = fft_freq[np.argmax(fft_val)]
                period = int(1/max_freq)
                """
                err = {}
                # Proviamo a vedere se è effettivamente quello con errore minore. Controlliamo tutti i periodi che vanno da 
                # poco prima della metà del periodo a poco dopo del doppio
                for i in range (int(period/2.1), min(int(period*2.1), math.floor(df['value'].size/2)), 1):
                    result = seasonal_decompose(df['value'], model='add', period=i)  # model='add' also works 
                    err[i] = result.resid.abs().max()
                    print("add errore[{}]: {}".format(i, err[i]))

                indice = min(err, key=err.get)
                print("min add: ", indice)
                seasonality['add'] = {}
                seasonality['add']['period'] = indice
                seasonality['add']['error'] = err[indice]

                result = seasonal_decompose(df['value'], model='add', period=indice)  # model='add' also works 
                result.plot();
                plt.savefig("/file/{}.png".format(10)) 
                
                err = {}
                for i in range (int(period/2.1), min(int(period*2.1), math.floor(df['value'].size/2)), 1):
                    result = seasonal_decompose(df['value'], model='mul', period=i)  # model='add' also works 
                    err[i] = result.resid.subtract(1).abs().max()
                    print("mul errore[{}]: {}".format(i, err[i]))

                indice = min(err, key=err.get)
                print("min mul: ", indice)
                seasonality['mul'] = {}
                seasonality['mul']['period'] = indice
                seasonality['mul']['error'] = err[indice]

                result = seasonal_decompose(df['value'], model='mul', period=indice)  # model='add' also works 
                result.plot();
                plt.savefig("/file/{}.png".format(10)) 
                """
                seasonality['add'] = {}
                seasonality['add']['period'] = period
                result = seasonal_decompose(df['value'], model='add', period=period)  # model='add' also works 
                seasonality['add']['error'] = result.resid.abs().max()

                seasonality['mul'] = {}
                seasonality['mul']['period'] = period
                result = seasonal_decompose(df['value'], model='mul', period=period)  # model='add' also works 
                seasonality['mul']['error'] = result.resid.subtract(1).abs().max()

            
            else:
                stationarity = {}
                stationarity['stationary'] = False
                stationarity['p_value'] = 0
                acf_res = {}
                seasonality = {}
        
            ris['metadata'] = {'stationarity' : stationarity, 'autocorrelation': acf_res, 'seasonality': seasonality}
            executionTime = time.time() - start
            gMeta.labels(ris['name']).set(executionTime)

            try:
                # Provo a scrivere su kafka
                p.produce(topic, value=json.dumps(ris), callback=delivery_callback)
            except BufferError:
                # TODO Nel caso in cui il buffer fosse pieno mostro un errore
                print('%% Local producer queue is full (%d messages awaiting delivery): try again\n' % len(p), file=sys.stderr)
            except KafkaException as err:
                print("Error: {}".format(err), file=sys.stderr)

            p.poll(0)


def everyY():
    start_time = parse_datetime("12h")
    end_time = parse_datetime("now")
    
    
    # Ottengo le metriche da prometheus
    print("PRima prometheus")
    try:
        print("metric name: ", metric_name)
        print("label config: ", label_config)
        print("start time: ", start_time)
        print("end time: ", end_time)
        metric_data = prom.get_metric_range_data(
            metric_name=metric_name,
            label_config=label_config,
            start_time=start_time,
            end_time=end_time,
        )
        print("Dentro il try")
    except Exception as err:
        print("Errore di connessione: {}".format(err), file=sys.stderr)
        return

    print("Ho ricevuto i dati")
    # Controllo che il filtro "matchi" effettivamente qualche metrica
    if len(metric_data) == 0:
        # Ricostruisco il filtro come se fosse PromQL
        str = "Non è presente alcuna metrica che corrisponde al filtro: " + metric_name
        if len(label_config) > 0 :
            str = str + "{"
            for elem in label_config:
                str = str + elem + "='" + label_config[elem] + "', "
            str = str[:-2] + "}"
        print(str)
    else:
        # Chiedo (tramite gRPC in modalità sincrona) all'SLA manager di ritornarmi il set di metriche
        print("Prendo il set")
        sla_set = []
        with grpc.insecure_channel('{}:{}'.format(os.environ['GRPC_SERVER'], os.environ['GRPC_PORT'])) as channel:
            stub = sla_pb2_grpc.SlaServiceStub(channel)
            try:
                for response in stub.GetSLASet(sla_pb2.SLARequest()):  
                    sla_elem = {}
                    sla_elem['name'] = response.metric
                    sla_elem['seasonality'] = {}
                    sla_elem['seasonality']['add_period'] = response.seasonality.add
                    sla_elem['seasonality']['mul_period'] = response.seasonality.mul
                    sla_set.append(sla_elem) 
                print("Ho ottenuto il set", sla_set)
            except grpc.RpcError as e:
                print("[gRPC] Errore: ", e)
        

        # Per ogni metrica ritornata dal prometheus
        for metric_data_i in metric_data:
            ris = {}
            ris['name'] = metric_data_i['metric']
            ris['values'] = {}
            metric_df12 = MetricRangeDataFrame(metric_data_i)
            metric_df12 = metric_df12.dropna()

            # Calcolo i valori relativi all'ultima ora
            start = time.time()
            metric_df_1 = metric_df12.last("1h")
            min = metric_df_1['value'].min()
            max = metric_df_1['value'].max()
            mean = metric_df_1['value'].mean()
            dev = metric_df_1['value'].std()
            ris['values']['1h'] = {'min': min, 'max': max, 'avg': mean, 'std_dev': dev}
            executionTime = time.time() - start
            g1h.labels(ris['name']).set(executionTime)

            # Calcolo i valori relativi alle ultime 3 ore
            start = time.time()
            metric_df_3 = metric_df12.last("3h")
            min = metric_df_3['value'].min()
            max = metric_df_3['value'].max()
            mean = metric_df_3['value'].mean()
            dev = metric_df_3['value'].std()
            ris['values']['3h'] = {'min': min, 'max': max, 'avg': mean, 'std_dev': dev}
            executionTime = time.time() - start
            g3h.labels(ris['name']).set(executionTime)

            # Calcolo i valori relativi alle ultime 12 ore
            start = time.time()
            min = metric_df12['value'].min()
            max = metric_df12['value'].max()
            mean = metric_df12['value'].mean()
            dev = metric_df12['value'].std()
            ris['values']['12h'] = {'min': min, 'max': max, 'avg': mean, 'std_dev': dev}
            executionTime = time.time() - start
            g12h.labels(ris['name']).set(executionTime)

            # TODO cambiare dappertutto prevision con prediction
            # Verifico se la metrica dell'attuale iterazione fa parte dell'sla set  
            for elem in sla_set:
                if elem['name'] == ris['name']:
                    start = time.time()

                    print("ci sono")

                    # Per la data metrica richiediamo più campioni
                    
                    metric_name_prediction = elem['name']['__name__']
                    #print(metric_name_prediction)
                    del elem['name']['__name__']
                    label_config_prediction = elem['name']
                    # Ottengo le metriche da prometheus
                    try:
                        metric_data_prediction = prom.get_metric_range_data(
                            metric_name=metric_name_prediction,
                            label_config=label_config_prediction,
                            start_time=parse_datetime("3d"), # TODO 1w perchè abbiamo 5 giorni di dati
                            end_time=parse_datetime("now"),
                        )
                    except Exception as err:
                        print("Errore di connessione: {}".format(err), file=sys.stderr)
                        return

                    metric_df_prediction = MetricRangeDataFrame(metric_data_prediction)
                    
                    metric_df_prediction = metric_df_prediction['value'].dropna()
                    metric_df_prediction = metric_df_prediction.resample(rule='T').mean()
                    # Dividiamo 90/10
                    data_len = metric_df_prediction.size
                    train = metric_df_prediction.iloc[:-int(data_len*0.1)]
                    test = metric_df_prediction.iloc[-int(data_len*0.1):]

                    # TODO Predico i prossimi 10 minuti (f_s = 1/1 min => 10 campioni)
                    prediction_rmse = {}
                    prediction_add = ""
                    """
                    if elem['seasonality']['add_period'] != 0:
                        print("Ho il periodo add")
                        tsmodel = ExponentialSmoothing(train, trend='add', seasonal='add',seasonal_periods=elem['seasonality']['add_period']).fit()
                        prediction_add = tsmodel.forecast(test.size)
                        prediction_rmse['es_add'] = {}
                        prediction_rmse['es_add']['all_test'] = np.sqrt(mean_squared_error(test, prediction_add))
                        prediction_rmse['es_add']['10m'] = np.sqrt(mean_squared_error(test[:10], prediction_add[:10]))
                    else: 
                        tsmodel = ExponentialSmoothing(train, trend='add', seasonal='add',seasonal_periods=1800).fit()
                        prediction_add = tsmodel.forecast(test.size)
                        prediction_rmse['es_add'] = {}
                        print(prediction_add)
                        prediction_rmse['es_add']['all_test'] = np.sqrt(mean_squared_error(test, prediction_add))
                        prediction_rmse['es_add']['10m'] = np.sqrt(mean_squared_error(test[:10], prediction_add[:10]))
                    """
                    # Se non è costante
                    if elem['seasonality']['add_period'] != 0:
                        print("Ho il periodo add")
                        tsmodel = ExponentialSmoothing(metric_df_prediction, trend='add', seasonal='add',seasonal_periods=elem['seasonality']['add_period']).fit()
                        prediction_add = tsmodel.forecast(10)
                        # Non abbiamo modo di calcolare l'errore. Non ha senso calcolare quello additivo perché non avremmo come confrontarli

                    # Se è costante
                    else:
                        # Creiamo una previsione fittizia mantenendo l'ultimo valore
                        last_sample_time = metric_df_prediction.index[-1]
                        next_sample_time = last_sample_time + pd.DateOffset(minutes=1)
                        index = pd.date_range(start=next_sample_time, periods=10, freq='T')
                        prediction_add = pd.DataFrame(np.full(10, metric_df_prediction.tail(1)),index =index)
                    """
                    prediction_mul = ""
                    if elem['seasonality']['mul_period'] != 0:
                        print("Ho il periodo mul")
                        tsmodel = ExponentialSmoothing(train, trend='add', seasonal='mul',seasonal_periods=elem['seasonality']['mul_period']).fit()
                        prediction_mul = tsmodel.forecast(test.size)
                        prediction_rmse['es_mul'] = {}
                        prediction_rmse['es_mul']['all_test'] = np.sqrt(mean_squared_error(test, prediction_mul))
                        prediction_rmse['es_mul']['10m'] = np.sqrt(mean_squared_error(test[:10], prediction_mul[:10]))
                    """
                    # ARIMA
                    """"
                    arima_res = auto_arima(train)
                    model = ARIMA(train, order=arima_res.order).fit()
                    prediction_arima = model.predict(start=len(train), end=len(train)+len(test)-1, dynamic=False, typ='levels')
                    prediction_rmse['arima'] = {}
                    prediction_rmse['arima']['all_test'] = np.sqrt(mean_squared_error(test, prediction_arima))
                    prediction_rmse['arima']['10m'] = np.sqrt(mean_squared_error(test[:10], prediction_arima[:10]))
                    """
                    """
                    #define size
                    plt.figure(figsize=(24,10))
                    #add axes labels and a title
                    plt.ylabel('Values', fontsize=14)
                    plt.xlabel('Time', fontsize=14)
                    plt.title('Values over time', fontsize=16)
                    plt.plot(test,"-",label = 'real')
                    if elem['seasonality']['add_period'] != 0:
                        plt.plot(prediction_add,".",label = 'predAdd')
                    if elem['seasonality']['mul_period'] != 0:
                        plt.plot(prediction_mul,"*",label = 'predMul')
                    plt.plot(prediction_arima,"*",label = 'predArima')
                    #add legend
                    plt.legend(title='Series')
                    plt.savefig("file/{}.png".format(elem['name']['mountpoint'].replace("/", "_")))
                    
                    print("Errore predizione: ", prediction_rmse)
                    """
                    print("Prediction: ", prediction_add)
                    plt.figure(figsize=(24,10))
                    #add axes labels and a title
                    plt.ylabel('Values', fontsize=14)
                    plt.xlabel('Time', fontsize=14)
                    plt.title('Values over time', fontsize=16)
                    plt.plot(test.iloc[-100:],"-",label = 'real')
                    #if elem['seasonality']['add_period'] != 0:
                    plt.plot(prediction_add,".",label = 'predAdd')
                    #if elem['seasonality']['mul_period'] != 0:
                    #add legend
                    plt.legend(title='Series')
                    plt.savefig("file/{}.png".format(elem['name']['mountpoint'].replace("/", "_")))

                         
                    # Verificare quando cambia il set se viene rimossa la predizione dalle metriche del set vecchio
                    ris['prediction'] = {}
                    ris['prediction']['min'] = prediction_add.min()
                    ris['prediction']['max'] = prediction_add.max()
                    ris['prediction']['avg'] = prediction_add.mean()
                    ris['prediction']['values'] = prediction_add.to_csv()

                    executionTime = time.time() - start
                    gPre.labels(ris['name']).set(executionTime)
    
            try:
                # Provo a scrivere su kafka
                p.produce(topic, value=json.dumps(ris), callback=delivery_callback)
            except BufferError:
                # TODO Nel caso in cui il buffer fosse pieno mostro un errore
                print('%% Local producer queue is full (%d messages awaiting delivery): try again\n' % len(p), file=sys.stderr)
            except KafkaException as err:
                print("Error: {}".format(err), file=sys.stderr)

            p.poll(0)