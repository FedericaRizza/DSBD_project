from flask import Flask

import mysql.connector
#from mysql.connector import errorcode
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

cursor = db.cursor()

@app.route("/prova")
def prova():
   return "ciao2"

'''
@app.errorhandler(404)
def not_found(error=None):
    message = {
        'status': 404,
        'message': 'Record not found: ' + request.url,
    }
    respone = jsonify(message)
    respone.status_code = 404
    return respone
'''


'''
@app.errorhandler(404) 
def invalid_route(): 
    return "404 Not Found!."
'''


#---QUERY DI TUTTE LE METRICHE DISPONIBILI IN PROMETHEUS-----------------------------------
@app.route("/metrics_available")
def metriche_disponibili():
    '''try:
        db.ping(reconnect=False, attempts = 1, delay=0)
    except:
       return "Gateway Timeout! DB not available.", 504'''
    try:
        cursor.execute("SELECT ID_metrica, metric_name FROM datas")
        metrics_available  = cursor.fetchall()
    except mysql.connector.Error as sql_execute_err:
        print("Errore:", sql_execute_err) 
    #print(metrics_available)
    #db.commit() #credo non ci sia di bisogno 
    return json.dumps(metrics_available)




#---QUERY DEI METADATI PER LA METRICA CON ID_metrica = id-----------------------------------
@app.route("/metrics_available/<id_metrica>/metadata")
def metadata(id_metrica):
    #global metadati 
    try:
        db.ping(reconnect=False, attempts = 1, delay=0)
    except:
       return "Gateway Timeout! DB not available.", 504
    try:
        sql = ("SELECT autocorrelazione, stazionarieta, stagionalita FROM datas WHERE ID_metrica = %s")
        val = [id_metrica] #ci voglio le quadre oppure tonda e , 
        cursor.execute(sql, val)
        metadati = cursor.fetchall()
        if metadati == None:
            return "Metric not available!", 400
        return json.dumps(metadati)
    except mysql.connector.Error as sql_execute_err:
        #db.rollback()
        print(sql_execute_err)
        return "Errore!"


#---QUERY DEI VALORI MAX, MIN, AVG, DEV_STD PER LE ULTIME 1,3,12 ORE-----------------------------------
@app.route("/metrics_available/<id_metrica>/values")
def valori(id_metrica):
    try:
        db.ping(reconnect=False, attempts = 1, delay=0)
    except:
       return "Gateway Timeout! DB not available.", 504
    try:
        sql = ("SELECT max_1h, max_3h, max_12h, min_1h, min_3h, min_12h, avg_1h, avg_3h, avg_12h, devstd_1h, devstd_3h, devstd_12h FROM datas WHERE ID_metrica = %s")
        val = [id_metrica]
        cursor.execute(sql, val)
        values = cursor.fetchall()
        if values == None:
            return "Metric not available!", 400
        return json.dumps(values)
    except mysql.connector.Error as sql_execute_err:
        print(sql_execute_err)
        return "Errore!"  
    
    



#---QUERY DEI VALORI PREDETTI-------------------------------------------------------------
@app.route("/metrics_available/<id_metrica>/prediction_values")
def prediction(id_metrica):
    try:
        db.ping(reconnect=False, attempts = 1, delay=0)
    except:
       return "Gateway Timeout! DB not available.", 504
    try:
        sql = ("SELECT max_predicted, min_predicted , avg_predicted FROM datas WHERE ID_metrica = %s")
        cursor.execute(sql,[id_metrica])
        prediction_values = cursor.fetchall()
        if prediction_values == None:
            return "Metric not available!", 400
        return json.dumps(prediction_values)
    except mysql.connector.Error as sql_execute_err:
        print(sql_execute_err)
        return "Errore!"  

cursor.close