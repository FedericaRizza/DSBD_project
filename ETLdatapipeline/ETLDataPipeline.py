from prometheus_api_client import PrometheusConnect, MetricsList, MetricSnapshotDataFrame, MetricRangeDataFrame
from datetime import timedelta, datetime

from prometheus_api_client.utils import parse_datetime

import mysql.connector

from confluent_kafka import Producer, KafkaError, KafkaException
import json
import time
import sys
import threading
import schedule

from statsmodels.tsa.stattools import adfuller, acf
from numpy.fft import rfft, rfftfreq
import numpy as np

import pandas as pd
import matplotlib.pyplot as plt

from statsmodels.tsa.holtwinters import ExponentialSmoothing, SimpleExpSmoothing
from sklearn.metrics import mean_squared_error
import numpy as np

import logging
from logging.handlers import RotatingFileHandler

#settaggio del sistema di monitoraggio tramite il modulo logging
logger = logging.getLogger('my_logger')
logger.setLevel(logging.INFO)
handler = RotatingFileHandler('metric_log.log', mode="a", maxBytes=100*1024*1024, backupCount=2) 
logger.addHandler(handler)


#defizione del set di metriche da monitorare
metric_set = ["cpuLoad", "cpuTemp", "diskUsage", "availableMem", "realUsedMem", "networkThroughput", "inodeUsage"]



def delivery_callback(err, msg):
        if err:
            sys.stderr.write('%% Message failed delivery: %s\n' % err)
        else:
            sys.stderr.write('%% Message delivered to %s, partition[%d] @ %d\n' %
                             (msg.topic(), msg.partition(), msg.offset()))



def run_thread(func, args):
    thread = threading.Thread(target=func, args = args, daemon=True)
    thread.start()


#thread per il calcolo dei metadata
def metadata(metric_set):

    logger = logging.getLogger('thread_logger')
    logger.setLevel(logging.INFO)
    handler = RotatingFileHandler('metric_log.log', mode="a", maxBytes=100*1024*1024, backupCount=2) 
    logger.addHandler(handler)

    label_config =  {'job' : 'summary'} 
    start_time = parse_datetime("1w") 
    end_time = parse_datetime("now")

    while True:
        try:
            prom = PrometheusConnect(url="http://15.160.61.227:29090", disable_ssl=True)
            print("Connessione a Prometheus avvenunata correttamente")
            break
        except Exception as prom_err:
            print("Errore: ", prom_err)
            time.sleep(5)

    configuration = {'bootstrap.servers': 'broker_kafka:9092'}
    topic = "prometheusdata" 

    while True:
        try: 
            producer = Producer(**configuration)
            print("Producer connesso")
            break
        except KafkaException as kafka_err:
            print("Errore: ", kafka_err)
            time.sleep(5)
    
    print("inizio calcolo metadata...")

    for metric in metric_set:  

        stime = time.perf_counter()
        try:
            metric_data = prom.get_metric_range_data (
                metric_name = metric,
                label_config = label_config,
                start_time = start_time,
                end_time = end_time,
                )
        except Exception as prom_err:
            print("Errore: ", prom_err)
            exit()

        etime = time.perf_counter() 
        long_time = str(timedelta(seconds=etime-stime)) 
        logger.info("\nTimestamp: " + str(datetime.now()) + "\tMetrica: " + metric  + "\t\t" + "\tDurata Prometheus query di 1w di dati: " + long_time + "\n")

        print("metriche ricevute!")
                
        
        #per ogni metrica 
        for i in metric_data:
            
            metric_rdf = MetricRangeDataFrame(i)

            st = None 
            acf_result = {} 
            seasonality_period = 0 

            stime = time.perf_counter()
            metric_rdf = metric_rdf.dropna() 
            seasonality_period = 0

            if metric_rdf['value'].min() != metric_rdf['value'].max(): 
                

                #stazionarietà
                adft = adfuller(metric_rdf['value'],autolag='AIC')
                if adft[1] <= 0.01: 
                    st = True
                else:
                    st = False


                #autocorrelazione
                a = acf(metric_rdf['value'])
                for j in range(0,len(a)):
                    if abs(a[j]) > 0.25:
                        acf_result[j] = a[j]


                #stagionalità
                if st == False:
                    fft = abs(rfft(metric_rdf['value'])) 
                    fft_freq = rfftfreq(len(metric_rdf['value']))
                    fft_freq2 = fft_freq[2:] 
                    fft2 = fft[2:]
                    freq_max = fft_freq2[np.argmax(fft2)] 
                    seasonality_period = int(1/freq_max)                
            else:
                st = True 
            
            etime = time.perf_counter() 
            long_time = str(timedelta(seconds=etime-stime)) 
            logger.info("\nTimestamp:" + str(datetime.now()) + "\tMetrica:" + str(i['metric']['__name__'] + "\t" + "Nodo:" + str(i['metric']['nodeId'])) + "\t" + "\tDurata calcolo metadati:" + long_time + "\n")
            

            dati_dictionary = {
                "datatype" : "metadata",
                "metric_name" : i['metric'],
                "autocorrelazione" : acf_result, 
                "stazionarieta": st,
                "stagionalita": seasonality_period
            }
            
            
            try:
                producer.produce(topic, value = json.dumps(dati_dictionary) , callback = delivery_callback)
            except BufferError:
                print('%% coda piena! (%d messaggi in attesa): riprova\n' % len(producer))
            except KafkaException as e:
                print("Errore: ",e)
            producer.poll(0)
    producer.flush()
    time.sleep(1)
    print("metadati inviati correttamente")


#implemetazione del processo princiale che si occuperà di calcolare massimo, minimo, avg e dev_std e di fare la predizione
def etl():

    label_config =  {'job' : 'summary'} 
    start_time = parse_datetime("12h") 
    end_time = parse_datetime("now")

    print("Connessione in corso...")
    while True:
        try:
            prom = PrometheusConnect(url="http://15.160.61.227:29090", disable_ssl=True)
            print("Connessione a Prometheus avvenuta correttamente")
            break
        except Exception as prom_err:
            print("Errore: ", prom_err)
            time.sleep(5)
        

    configuration = {'bootstrap.servers': 'broker_kafka:9092'}
    topic = "prometheusdata" 

    while True:
        try: 
            producer = Producer(**configuration)
            print("Producer connesso")
            break
        except KafkaException as kafka_err:
            print("Errore: ", kafka_err)
            time.sleep(5)

    thread = threading.Thread(target=metadata, args = [metric_set], daemon= True)
    thread.start()
    schedule.every().day.do(run_thread, metadata, [metric_set])

    print("inizio etl...")
    while True:        
        schedule.run_pending()

        for metric in metric_set:

            stime = time.perf_counter()
            try:
                metric_data = prom.get_metric_range_data (
                    metric_name = metric,
                    label_config = label_config,
                    start_time = start_time,
                    end_time = end_time,
                    )
            except Exception as prom_err:
                print("Errore: ", prom_err)
                exit()

            etime = time.perf_counter() 
            long_time = str(timedelta(seconds=etime-stime)) 
            logger.info("\nTimestamp: " + str(datetime.now()) + "\tMetrica: " + metric  + "\t\t" + "\tDurata Prometheus query di 12h di dati: " + long_time + "\n")

            print("metriche ricevute!")
                    

            #per ogni metrica 
            for i in metric_data:
                
                metric_rdf = MetricRangeDataFrame(i)

                #PUNTO 2: max, min, avg, dev_std per 1h, 3h, 12h
                stime = time.perf_counter() 
                metric_rdf_1h = metric_rdf.last("1h")
                min1 = metric_rdf_1h['value'].min()
                max1 = metric_rdf_1h['value'].max()
                avg1 = metric_rdf_1h['value'].mean()
                dev_std1 = metric_rdf_1h['value'].std()
                etime = time.perf_counter() 
                long_time = str(timedelta(seconds=etime-stime)) 
                logger.info("\nTimestamp:" + str(datetime.now()) + "\tMetrica:" + str(i['metric']['__name__']  + "\tNodo:" + str(i['metric']['nodeId'])) + "\t" + "\tDurata calcolo max, min, avg, dev_std per 1h:" + long_time + "\n")


                stime = time.perf_counter()
                metric_rdf_3h = metric_rdf.last("3h")
                min3 = metric_rdf_3h['value'].min()
                max3 = metric_rdf_3h['value'].max()
                avg3 = metric_rdf_3h['value'].mean()
                dev_std3 = metric_rdf_3h['value'].std()
                etime = time.perf_counter() 
                long_time = str(timedelta(seconds=etime-stime))
                logger.info("\nTimestamp:" + str(datetime.now()) + "\tMetrica:" + str(i['metric']['__name__'] + "\t" + "Nodo:" + str(i['metric']['nodeId'])) + "\t" + "\tDurata calcolo max, min, avg, dev_std per 3h:" + long_time + "\n")


                stime = time.perf_counter()
                metric_rdf_12h = metric_rdf.last("12h")
                min12 = metric_rdf_12h['value'].min()
                max12 = metric_rdf_12h['value'].max()
                avg12 = metric_rdf_12h['value'].mean()
                dev_std12 = metric_rdf_12h['value'].std()
                etime = time.perf_counter()
                long_time = str(timedelta(seconds=etime-stime)) 
                logger.info("\nTimestamp:" + str(datetime.now()) + "\tMetrica:" + str(i['metric']['__name__'] + "\t" + "Nodo:" + str(i['metric']['nodeId'])) + "\t" + "\tDurata calcolo max, min, avg, dev_std per 12h:" + long_time + "\n")

                
                #PUNTO 3: PREDIZIONE
                while True:
                    try:
                        db = mysql.connector.connect(
                            host='db',
                            user='user',
                            password='password',
                            database='prometheus_data'
                        )
                        break
                    except Exception as sql_err:
                        print("Errore: ", sql_err)
                        time.sleep(5)
                cursor=db.cursor()        
                try:
                    cursor.execute("SELECT metric_name FROM sla")
                    name_list=cursor.fetchall()                    
                except Exception as sql_err:
                    print("Errore: ", sql_err)

                max_pred=None
                min_pred=None
                avg_pred=None
                
                if (i['metric']['__name__'],) in name_list: 
                    stime = time.perf_counter()
                    
                    try:
                        metric_data_pred = prom.get_metric_range_data (
                        metric_name = i['metric']['__name__'],
                        label_config = dict(list(i['metric'].items())[1:]), 
                        start_time = parse_datetime("1w"),
                        end_time = parse_datetime("now") ,
                        )
                    except Exception as prom_err:
                        print("Errore: ", prom_err)
                        exit()
                    st= None
                    seasonality_period= None
                    try:
                        val=[json.dumps(i['metric'])]
                        cursor.execute("SELECT stazionarieta, stagionalita FROM datas WHERE metric_name = %s", val)
                        st,seasonality_period = cursor.fetchone()
                    except Exception as sql_err:
                        print("Errore: ", sql_err)

                    if seasonality_period > 1440:
                        seasonality_period = 0

                    ts = MetricRangeDataFrame(metric_data_pred)
                    ts = ts['value'].dropna()
                    tsr = ts.resample(rule='T').mean()
                    tsr = tsr.interpolate()
                    
                    train_data = tsr.iloc[:-10] 
                    test_data = tsr.iloc[-10:]

                    #se stazionaria
                    if st or seasonality_period == 0:

                        pred_model_1 = SimpleExpSmoothing(train_data,initialization_method="heuristic").fit(smoothing_level=0.2,optimized=False)
                        pred_model_2 = SimpleExpSmoothing(train_data,initialization_method="heuristic").fit(smoothing_level=0.6,optimized=False)
                        pred_model_3 = SimpleExpSmoothing(train_data,initialization_method="estimated").fit()

                        prediction_1 = pred_model_1.forecast(10)
                        prediction_2 = pred_model_2.forecast(10)
                        prediction_3 = pred_model_3.forecast(10)

                        
                        error1= np.sqrt(mean_squared_error(test_data, prediction_1))
                    
                    
                        error2= np.sqrt(mean_squared_error(test_data, prediction_2))
                    
                    
                        error3= np.sqrt(mean_squared_error(test_data, prediction_3))
                        

                        pred_model=""
                        if error1 < error2 and error1 < error3:
                            pred_model = SimpleExpSmoothing(tsr,initialization_method="heuristic").fit(smoothing_level=0.2,optimized=False)
                        if error2 < error1 and error2 < error3:
                            pred_model = SimpleExpSmoothing(tsr,initialization_method="heuristic").fit(smoothing_level=0.6,optimized=False)
                        if error3 < error1 and error3 < error2:
                            pred_model = SimpleExpSmoothing(tsr,initialization_method="estimated").fit()

                        prediction = pred_model.forecast(10)
                        max_pred = prediction.max()
                        min_pred = prediction.min()
                        avg_pred = prediction.mean()              

                    #se con trend e stagionalità
                    if seasonality_period != 0 and seasonality_period != None: 


                        pred_model_aa = ExponentialSmoothing(train_data,trend='add', seasonal='add', seasonal_periods=seasonality_period).fit()
                        pred_model_am = ExponentialSmoothing(train_data,trend='add', seasonal='mul', seasonal_periods=seasonality_period).fit()
                        pred_model_ma = ExponentialSmoothing(train_data,trend='mul', seasonal='add', seasonal_periods=seasonality_period).fit()
                        pred_model_mm = ExponentialSmoothing(train_data,trend='mul', seasonal='mul', seasonal_periods=seasonality_period).fit()

                        prediction_aa = pred_model_aa.forecast(10)
                        prediction_am = pred_model_am.forecast(10)
                        prediction_ma = pred_model_ma.forecast(10)
                        prediction_mm = pred_model_mm.forecast(10)

                        
                        error1= np.sqrt(mean_squared_error(test_data, prediction_aa))
                    
                        error2= np.sqrt(mean_squared_error(test_data, prediction_am))
                    
                        error3= np.sqrt(mean_squared_error(test_data, prediction_ma))
                    
                        error4= np.sqrt(mean_squared_error(test_data, prediction_mm))
                        

                        pred_model=""
                        if error1 < error2 and error1 < error3:
                            if error1 < error4:
                                pred_model = ExponentialSmoothing(tsr,trend='add', seasonal='add', seasonal_periods=seasonality_period).fit()
                            else:
                                pred_model = ExponentialSmoothing(tsr,trend='mul', seasonal='mul', seasonal_periods=seasonality_period).fit()
                        if error2 < error1 and error2 < error3:
                            if error2 < error4:
                                pred_model = ExponentialSmoothing(tsr,trend='add', seasonal='mul', seasonal_periods=seasonality_period).fit()
                            else:
                                pred_model = ExponentialSmoothing(tsr,trend='mul', seasonal='mul', seasonal_periods=seasonality_period).fit()
                        if error3 < error1 and error3 < error2:
                            if error3 < error4:
                                pred_model = ExponentialSmoothing(tsr,trend='mul', seasonal='add', seasonal_periods=seasonality_period).fit()
                            else:
                                pred_model = ExponentialSmoothing(tsr,trend='mul', seasonal='mul', seasonal_periods=seasonality_period).fit()

                        prediction = pred_model.forecast(10)
                        max_pred = prediction.max()
                        min_pred = prediction.min()
                        avg_pred = prediction.mean()
                        


                    etime = time.perf_counter() 
                    long_time = str(timedelta(seconds=etime-stime)) 
                    logger.info("\nTimestamp:" + str(datetime.now()) + "\tMetrica:" + str(i['metric']['__name__'] + "\t" + "Nodo:" + str(i['metric']['nodeId'])) + "\t" + "\tDurata forecasting e calcolo max, min, avg previsti nei prossimi 10min: " + long_time + "\n")
                
                cursor.close()


                #inoltro in un topic kafka "prometheusdata" di un messaggio contenenti i valori calcolati
                dati_dictionary = {
                    "datatype" : "data",
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
                    "max_predicted" : max_pred, 
                    "min_predicted" : min_pred, 
                    "avg_predicted" : avg_pred               
                }
                
                
                try:
                    producer.produce(topic, value = json.dumps(dati_dictionary) , callback = delivery_callback)
                except BufferError:
                    print('%% coda piena! (%d messaggi in attesa): riprova\n' % len(producer))
                except KafkaException as e:
                    print("Errore: ",e)
                producer.poll(0) 
        print("dati inviati correttamente!")
        time.sleep(300) 

if __name__ == '__main__':
       
    etl()
