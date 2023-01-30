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

logger = logging.getLogger('my_logger')
logger.setLevel(logging.INFO)
handler = RotatingFileHandler('metric_log.log', mode="a", maxBytes=100*1024*1024, backupCount=2) #il file pesa al massimo 100mb, quelli più vecchi li scarta
logger.addHandler(handler)

metric_set = ["cpuLoad", "cpuTemp", "diskUsage", "availableMem", "realUsedMem", "networkThroughput", "inodeUsage"]

#%matplotlib inline

def delivery_callback(err, msg):
        if err:
            sys.stderr.write('%% Message failed delivery: %s\n' % err)
        else:
            sys.stderr.write('%% Message delivered to %s, partition[%d] @ %d\n' %
                             (msg.topic(), msg.partition(), msg.offset()))



'''def services_connect():
    print("Connessione in corso...")

    while True:
        try:
            prom = PrometheusConnect(url="http://15.160.61.227:29090", disable_ssl=True)
            #prom = PrometheusConnect(url="http://prom:9090", disable_ssl=True)
            print("Connessione a Prometheus avvenunata correttamente")
            break
        except Exception as prom_err:
            print("Errore: ", prom_err)
            time.sleep(5)

    #configurazione per il punto 4
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
        #print(producer)

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

    
    
    #cursor.execute("DELETE FROM datas")
    #cursor.execute("DELETE FROM acf")
    #db.commit()
    #print("fatto")
'''

def run_thread(func, args):
    print("esecuzione job schedulato")
    thread = threading.Thread(target=func, args = args, daemon=True)
    thread.start()

'''
#se il file non esiste open lo creerà per noi 
log_file=open("log.txt","a") #apriamo il file log in modalità append in modo da scrivere in coda al contenuto attuale del file
'''


def metadata(metric_set):

    logger = logging.getLogger('my_logger')
    logger.setLevel(logging.INFO)
    handler = RotatingFileHandler('metric_log.log', mode="a", maxBytes=100*1024*1024, backupCount=2) #il file pesa al massimo 100mb, quelli più vecchi li scarta
    logger.addHandler(handler)

    label_config =  {'job' : 'summary'} # {} {'nodeName': 'sv192'} 
    start_time = parse_datetime("1w") 
    end_time = parse_datetime("now")

    while True:
        try:
            prom = PrometheusConnect(url="http://15.160.61.227:29090", disable_ssl=True)
            #prom = PrometheusConnect(url="http://prom:9090", disable_ssl=True)
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

    for metric in metric_set:  

        stime = time.perf_counter()
        try:
            metric_data = prom.get_metric_range_data (
                metric_name = metric,
                label_config = label_config,
                start_time = start_time,
                end_time = end_time,
                    #chunk_size = chunk_size,
                )
        except Exception as prom_err:
            print("Errore: ", prom_err)
            exit()

        etime = time.perf_counter() #mi prendo il tempo esatto di quando ho finito queste operazioni
        long_time = str(timedelta(seconds=etime-stime)) #timedelta per la manipolazione dei tempi
        #log_file.write("\nTimestamp:" + str(datetime.now()) + "\tMetrica:" + str(i['metric']['__name__'] + "\t" + str(i['metric']['nodeId'])) + "\tDurata calcolo metadati:" + long_time + "\n")
        logger.info("\nTimestamp: " + str(datetime.now()) + "\tMetrica: " + metric  + "\t\t" + "\tDurata Prometheus query di 1w di dati: " + long_time + "\n")

        print("metriche ricevute!")
                

        #per ogni metrica 
        for i in metric_data:
            
            metric_rdf = MetricRangeDataFrame(i)

            st = None #boolean
            acf_result = {} #dictionary
            seasonality_period = 0 #integer

            stime = time.perf_counter()
            #PUNTO 1: TODO separare metadati e valori/predizioni
            metric_rdf = metric_rdf.dropna() #rimuove le righe che hanno valore NULL dal DataFrame
            seasonality_period = 0
            if metric_rdf['value'].min() != metric_rdf['value'].max(): 
                

                #stazionarietà
                #Un modo per verificare la stazionarietà di una serie temporale è quello di eseguire il cosidetto test di Dickey-Fuller.
                #Nel primo parametro si mette la serie da testare, nel secondo per determinare la lunghezza (del ritardo?) tra i valori 0,1,...max, con AIC si cerca di minimizzzarlo?
                adft = adfuller(metric_rdf['value'],autolag='AIC')
                print("ADFTEST",adft[1])
                #Alla fine adfuller ci ritornerà un p-value, se quest'ultimo è più basso di un certo valore (come per esempio 0.05) si può concludere che la serie sia stazionaria
                if adft[1] <= 0.01: #cambiato da 0.05 a .01, mi dava stazionaria una serie palesemente con stagionalità
                    #st['stationarity'] = True
                    st = True
                else:
                    st = False


                #autocorrelazione
                #lags = len(metric_rdf['value']) #con lags forse si impostano i numeri di campioni 
                #acf ci permette di calcolare la funzione di autocorrelazione
                #nel primo parametro mettiamo la serie, con alpha invece viene impostata l'ampiezza dell'intervallo? la confidenza? alpha = confidenza ? e che è?
                a = acf(metric_rdf['value'])
                for j in range(1,len(a)):
                    #con questo controllo ritorno i valori esterni all'area che ci disegna la plotacf
                    if abs(a[j]) > 0.25:
                        acf_result[j] = a[j]
                        #acf_result.append(a[j]) #append aggiunge un item alla fine della lista
                print(acf_result)
                    #perchè in alcuni salta il 30?


                #stagionalità
                if st == False:
                    #mi faccio la trasformata di Fourier vedo tutte le componenti della frequenza e prendo quella con contributo maggiore
                    #con rfft calcoliamo la trasformata discreta di fourier
                    fft = abs(rfft(metric_rdf['value'])) #con abs torno il valore assoluto
                    #con rfftfreq torniamo le frequenze di campionamento della trasmormaya di fourier, ritorna un array float 
                    #gli passiamo la lunghezza della finestra
                    fft_freq = rfftfreq(len(metric_rdf['value']))
                    fft_freq2 = fft_freq[2:] #scarto i primi due valori (frequenza 0 e 1) dalla lista altrimenti il massimo sarebbe li sempre
                    fft2 = fft[2:]
                    freq_max = fft_freq2[np.argmax(fft2)] #argmax restituisce l'indice dei valori massimi
                    seasonality_period = int(1/freq_max) #mi prendo il periodo come inverso della frequenza
                    #ogni quanto la serie si ripete?
                    print("PERIODO", seasonality_period)
                    #alla fine avrò un i valori della trasmormata nella y mentre nelle c avrò le frequenze, mi devo trovare la x dove la y è massima
                    if seasonality_period > 1440: #piu di 1/3 della durata della serie 
                        seasonality_period = 0

            else:
                st = True 
            
            etime = time.perf_counter() #mi prendo il tempo esatto di quando ho finito queste operazioni
            long_time = str(timedelta(seconds=etime-stime)) #timedelta per la manipolazione dei tempi
            #log_file.write("\nTimestamp:" + str(datetime.now()) + "\tMetrica:" + str(i['metric']['__name__'] + "\t" + str(i['metric']['nodeId'])) + "\tDurata calcolo metadati:" + long_time + "\n")
            logger.info("\nTimestamp:" + str(datetime.now()) + "\tMetrica:" + str(i['metric']['__name__'] + "\t" + "Nodo:" + str(i['metric']['nodeId'])) + "\t" + "\tDurata calcolo metadati:" + long_time + "\n")
            
            dati_dictionary = {
                "datatype" : "metadata",
                "metric_name" : i['metric'],
                "autocorrelazione" : acf_result, #gli passo un dizionario #valore a caso per provare, avevo messo float nel db
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

def etl():

    label_config =  {'job' : 'summary'} # {} {'nodeName': 'sv192'} 
    start_time = parse_datetime("12h") 
    end_time = parse_datetime("now")

    print("Connessione in corso...")
    while True:
        try:
            prom = PrometheusConnect(url="http://15.160.61.227:29090", disable_ssl=True)
            #prom = PrometheusConnect(url="http://prom:9090", disable_ssl=True)
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
        #print(producer)

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

    thread = threading.Thread(target=metadata, args = [metric_set], daemon= True)
    thread.start()
    schedule.every().day.do(run_thread, metadata, [metric_set])

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
                        #chunk_size = chunk_size,
                    )
            except Exception as prom_err:
                print("Errore: ", prom_err)
                exit()

            etime = time.perf_counter() #mi prendo il tempo esatto di quando ho finito queste operazioni
            long_time = str(timedelta(seconds=etime-stime)) #timedelta per la manipolazione dei tempi
            #log_file.write("\nTimestamp:" + str(datetime.now()) + "\tMetrica:" + str(i['metric']['__name__'] + "\t" + str(i['metric']['nodeId'])) + "\tDurata calcolo metadati:" + long_time + "\n")
            logger.info("\nTimestamp: " + str(datetime.now()) + "\tMetrica: " + metric  + "\t\t" + "\tDurata Prometheus query di 12h di dati: " + long_time + "\n")

            print("metriche ricevute!")
                    

            #per ogni metrica 
            for i in metric_data:
                
                metric_rdf = MetricRangeDataFrame(i)

            #eventualmente fare un controllo in modo da non ripetere l'operazione dei metadati

                #PUNTO 2:
                #max, min, avg, dev_std per 1h, 3h, 12h

                #perf counter utilizza il counter migliore disponibile
                stime = time.perf_counter() #mi restituisce il valore (float) (in frazioni di secondi), include il tempo degli sleep, in questo modo setto il tempo di inizio
                metric_rdf_1h = metric_rdf.last("1h")
                min1 = metric_rdf_1h['value'].min()
                max1 = metric_rdf_1h['value'].max()
                avg1 = metric_rdf_1h['value'].mean()
                dev_std1 = metric_rdf_1h['value'].std()
                print('max1h:', max1,'min1h:', min1, 'media1h:',  avg1, 'deviazione standard1h:', dev_std1, "\n")
                #monitoraggio con log per i valori calcoli per 1 ora
                etime = time.perf_counter() #mi prendo il tempo esatto di quando ho finito queste operazioni
                long_time = str(timedelta(seconds=etime-stime)) #timedelta per la manipolazione dei tempi
                #log_file.write("\nTimestamp:" + str(datetime.now()) + "\tMetrica:" + str(i['metric']['__name__'] + "\t" + str(i['metric']['nodeId'])) + "\tDurata calcolo max, min, avg, dev_std per 1h:" + long_time + "\n")
                #non so se lasciare istance, in base a come decideremo di prendere le metriche alla fine
                logger.info("\nTimestamp:" + str(datetime.now()) + "\tMetrica:" + str(i['metric']['__name__']  + "\tNodo:" + str(i['metric']['nodeId'])) + "\t" + "\tDurata calcolo max, min, avg, dev_std per 1h:" + long_time + "\n")


                stime = time.perf_counter()
                metric_rdf_3h = metric_rdf.last("3h")
                min3 = metric_rdf_3h['value'].min()
                max3 = metric_rdf_3h['value'].max()
                avg3 = metric_rdf_3h['value'].mean()
                dev_std3 = metric_rdf_3h['value'].std()
                print('max3h:', max3,'min3h:', min3, 'media3h:',  avg3, 'deviazione standard3h:', dev_std3, "\n")
                etime = time.perf_counter() #mi prendo il tempo esatto di quando ho finito queste operazioni
                long_time = str(timedelta(seconds=etime-stime)) #timedelta per la manipolazione dei tempi
                #log_file.write("\nTimestamp:" + str(datetime.now()) + "\tMetrica:" + str(i['metric']['__name__'] + "\t" + str(i['metric']['nodeId'])) + "\tDurata calcolo max, min, avg, dev_std per 3h:" + long_time + "\n")
                logger.info("\nTimestamp:" + str(datetime.now()) + "\tMetrica:" + str(i['metric']['__name__'] + "\t" + "Nodo:" + str(i['metric']['nodeId'])) + "\t" + "\tDurata calcolo max, min, avg, dev_std per 3h:" + long_time + "\n")


                stime = time.perf_counter()
                metric_rdf_12h = metric_rdf.last("12h")
                min12 = metric_rdf_12h['value'].min()
                max12 = metric_rdf_12h['value'].max()
                avg12 = metric_rdf_12h['value'].mean()
                dev_std12 = metric_rdf_12h['value'].std()
                print('max12h:', max12,'min12h:', min12, 'media12h:',  avg12, 'deviazione standard12h:', dev_std12, "\n")
                etime = time.perf_counter() #mi prendo il tempo esatto di quando ho finito queste operazioni
                long_time = str(timedelta(seconds=etime-stime)) #timedelta per la manipolazione dei tempi
                #log_file.write("\nTimestamp:" + str(datetime.now()) + "\tMetrica:" + str(i['metric']['__name__'] + "\t" + str(i['metric']['nodeId'])) + "\tDurata calcolo max, min, avg, dev_std per 12h:" + long_time + "\n")
                logger.info("\nTimestamp:" + str(datetime.now()) + "\tMetrica:" + str(i['metric']['__name__'] + "\t" + "Nodo:" + str(i['metric']['nodeId'])) + "\t" + "\tDurata calcolo max, min, avg, dev_std per 12h:" + long_time + "\n")

                
    #---------------------PREDIZIONE: richiede dati di tutta la serie per fare train e test
                try:
                    cursor.execute("SELECT metric_name FROM sla")
                    name_list=cursor.fetchall()
                except Exception as sql_err:
                    print("Errore: ", sql_err)

                max_pred=None
                min_pred=None
                avg_pred=None
                print("-----------------------------")
                print(name_list, i['metric']['__name__'])
                if (i['metric']['__name__'],) in name_list: #se non ho settato sla set non entra nell if, nessun valore di predizione, name_list è lista di tuple
                    stime = time.perf_counter()
                    print("IF DI PREDIZIONE", i['metric'])
                    metric_data_pred = prom.get_metric_range_data (
                    metric_name = i['metric']['__name__'],
                    label_config = dict(list(i['metric'].items())[1:]), #tolgo il primo elemento del dizionario per non mettere di nuovo il nome metrica
                    start_time = parse_datetime("1w"),
                    end_time = parse_datetime("now") ,
                    #chunk_size = chunk_size,
                    )
                    st= None
                    seasonality_period= None
                    try:
                        cursor.execute("SELECT stazionarieta, stagionalita FROM datas WHERE metric_name = %s", [i['metric']])
                        st,seasonality_period = cursor.fetchone()
                    except Exception as sql_err:
                        print("Errore: ", sql_err)

                    ts = MetricRangeDataFrame(metric_data_pred)
                    ts = ts['value'].dropna()
                    tsr = ts.resample(rule='T').mean()
                    #Split training and test data (80/20% 0r 90/10)

                    train_data = tsr.iloc[:-10] 
                    test_data = tsr.iloc[-10:]

                    #se stazionaria
                    if st or seasonality_period == 0:
                        print("STAZIONARIO")

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

                    #se con trend???
                    #if not st and seasonality_period == 0:
                    #se con stagionalità???
                    #se con trend e stagionalità
                    #provare con diskUsage che ha trend e stagionalità
                    if seasonality_period != 0 and seasonality_period != None: #se period = None non posso fare confronti tra interi
                        print("SEASON")

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
                        


                    etime = time.perf_counter() #mi prendo il tempo esatto di quando ho finito queste operazioni
                    long_time = str(timedelta(seconds=etime-stime)) #timedelta per la manipolazione dei tempi
                    #log_file.write("\nTimestamp:" + str(datetime.now()) + "\tMetrica:" + str(i['metric']['__name__'] + "\t" + str(i['metric']['nodeId'])) + "\tDurata calcolo max, min, avg, dev_std per 12h:" + long_time + "\n")
                    logger.info("\nTimestamp:" + str(datetime.now()) + "\tMetrica:" + str(i['metric']['__name__'] + "\t" + "Nodo:" + str(i['metric']['nodeId'])) + "\t" + "\tDurata forecasting e calcolo max, min, avg previsti nei prossimi 10min: " + long_time + "\n")
                


                #PUNTO 4: inoltrare in un topic kafka "prometheusdata" un messaggio contenenti i valori calcolati

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
                    "max_predicted" : max_pred, #valore a caso per provare
                    "min_predicted" : min_pred, #valore a caso per provare
                    "avg_predicted" : avg_pred #valore a caso per provare                
                }
                
                
                try:
                    producer.produce(topic, value = json.dumps(dati_dictionary) , callback = delivery_callback)
                except BufferError:
                    print('%% coda piena! (%d messaggi in attesa): riprova\n' % len(producer))
                except KafkaException as e:
                    print("Errore: ",e)
                producer.poll(0) #timeout impostato su 0 quindi la poll ritorna immediatamente

        time.sleep(60) #TODO ricomincia le query dopo 5min

if __name__ == '__main__':
       
    etl()


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