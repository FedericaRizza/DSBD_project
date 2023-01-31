from flask import Flask

import mysql.connector

import json
import time

app = Flask(__name__)

while True:
    try:
        db = mysql.connector.connect(
        host = "db",
        user = "user",
        password = "password",
        database= "prometheus_data"
        )
        break
    except mysql.connector.Error as sqlerr:
        print ("Error code:", sqlerr.errno )       # error number
        print ("SQLSTATE value:", sqlerr.sqlstate) # SQLSTATE value
        print ("Error message:", sqlerr.msg)       # error message
        print ("Error:", sqlerr)                   # errno, sqlstate, msg values
        s = str(sqlerr)
        print ("Error:", s)
        time.sleep(10)




#---QUERY DI TUTTE LE METRICHE DISPONIBILI IN PROMETHEUS---------
@app.route("/metrics_available")
def metriche_disponibili():
    while True:
        try:
            db = mysql.connector.connect( 
            host = "db",
            user = "user",
            password = "password",
            database="prometheus_data"
            )
            break
        except Exception as sqlerr:
            print("Errore: ", sqlerr)
            time.sleep(10)
    cursor = db.cursor()  
    try:
        db.ping(reconnect=False, attempts = 1, delay=0)
    except:
        cursor.close()
        return "DB not available.", 504
    try:
        cursor.execute("SELECT ID_metrica, metric_name FROM datas")
        metrics_available  = cursor.fetchall()
        cursor.close()
    except mysql.connector.Error as sql_execute_err:
        cursor.close()
        print("Errore:", sql_execute_err) 
    return json.dumps(metrics_available)




#---QUERY DEI METADATI PER LA METRICA CON ID_metrica = id----------
@app.route("/metrics_available/<id_metrica>/metadata")
def metadata(id_metrica):
    while True:
        try:
            db = mysql.connector.connect( 
            host = "db",
            user = "user",
            password = "password",
            database="prometheus_data"
            )
            break
        except Exception as sqlerr:
            print("Errore: ", sqlerr)
            time.sleep(10) 
    cursor = db.cursor()
    try:
        db.ping(reconnect=False, attempts = 1, delay=0)
    except:
        cursor.close()
        return "DB not available.", 504
    try:
        sql = ("""SELECT acf.acf_lag, acf.acf_value, datas.stazionarieta, datas.stagionalita 
                FROM datas 
                INNER JOIN acf ON datas.ID_metrica = acf.ID_metrica 
                WHERE datas.ID_metrica = %s""")
        val = [id_metrica]  
        cursor.execute(sql, val)
        metadati = cursor.fetchall()
        cursor.close()
        if metadati == None:
            return "Metric not available!", 400
        return json.dumps(metadati)
    except mysql.connector.Error as sql_execute_err:
        cursor.close()
        print(sql_execute_err)
        return "Errore!"
    
   


#---QUERY DEI VALORI MAX, MIN, AVG, DEV_STD PER LE ULTIME 1,3,12 ORE------------
@app.route("/metrics_available/<id_metrica>/values")
def valori(id_metrica):
    while True:
        try:
            db = mysql.connector.connect( 
            host = "db",
            user = "user",
            password = "password",
            database="prometheus_data"
            )
            break
        except Exception as sqlerr:
            print("Errore: ", sqlerr)
            time.sleep(10)
    cursor = db.cursor()  
    try:
        db.ping(reconnect=False, attempts = 1, delay=0)
    except:
        cursor.close()
        return "DB not available.", 504
    try:
        sql = ("SELECT max_1h, max_3h, max_12h, min_1h, min_3h, min_12h, avg_1h, avg_3h, avg_12h, devstd_1h, devstd_3h, devstd_12h FROM datas WHERE ID_metrica = %s")
        val = [id_metrica]
        cursor.execute(sql, val)
        values = cursor.fetchall()
        if values == None:
            return "Metric not available!", 400
        descr = cursor.description
        val_dict = {}
        for i in range(len(descr)):
            for k in range (len(values)):
                for j in range(len(values[k])):
                    val_dict[descr[j][0]] = values[k][j]
        cursor.close()
        return json.dumps(val_dict)
    except mysql.connector.Error as sql_execute_err:
        cursor.close()
        print(sql_execute_err)
        return "Errore!"  
    
    



#---QUERY DEI VALORI PREDETTI------------------------------------------------------
@app.route("/metrics_available/<id_metrica>/prediction_values")
def prediction(id_metrica):
    while True:
        try:
            db = mysql.connector.connect( 
            host = "db",
            user = "user",
            password = "password",
            database="prometheus_data"
            )
            break
        except Exception as sqlerr:
            print("Errore: ", sqlerr)
            time.sleep(10)
    cursor = db.cursor()          
    try:
        db.ping(reconnect=False, attempts = 1, delay=0)
    except:
       return "DB not available.", 504
    try:
        sql = ("SELECT max_predicted, min_predicted , avg_predicted FROM datas WHERE ID_metrica = %s")
        cursor.execute(sql,[id_metrica])
        prediction_values = cursor.fetchall()
        if prediction_values == None:
            return "Metric not available!", 400
        descr = cursor.description
        val_dict = {}
        for i in range(len(descr)):
            for k in range (len(prediction_values)):
                for j in range(len(prediction_values[k])):
                    val_dict[descr[j][0]] = prediction_values[k][j]
        cursor.close()
        return json.dumps(val_dict)
    except mysql.connector.Error as sql_execute_err:
        cursor.close()
        print(sql_execute_err)
        return "Errore!"  
