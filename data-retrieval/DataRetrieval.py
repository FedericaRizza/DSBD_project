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
    except Exception as sqlerr:
        print("Errore: ", sqlerr)
        time.sleep(10)

cursor = db.cursor()

@app.route("/prova")
def prova():
   return "ciao2"




@app.route("/metricavailable")
def metriche_disponibili():
    cursor.execute("SELECT ID_metrica, metric_name FROM datas")
    metrics_available  = cursor.fetchall()
    print(metrics_available)
    db.commit() #credo non ci sia di bisogno 
    return json.dumps(metrics_available)

@app.route("/metricavailable/<id_metric>/metadata")
def metadata(id):
    sql = ("SELECT autocorrelazione, stazionarieta, stagionalita FROM datas WHERE ID_metrica = %s") 
    val = (id)
    cursor.execute(sql,val)
    metadati = cursor.fetchall()
    return json.dumps(metadati)


@app.route("/metrichedisponibili/<id_metrica>/values")
def valori():
    sql = cursor.execute("SELECT max_1h, max_3h, max_12, min_1h, min_1h, min_3h, min_12h, avg_1h, avg_3h, avg_12h, devstd_1h, devstd_3h, devstd_12h FROM datas WHERE metric_name = %s ")
    val = (id)
    cursor.execute(sql, val)
    values = cursor.fetchall()
    return json.dumps(values)

@app.route("/metrichedisponibili/<id_metrica>/prediction_values")
def prediction():
    sql = ("SELECT max_predicted, min_predicted , avg_predicted FROM datas WHERE ID_metrica = %s")
    val = (id)
    cursor.execute(sql,val)
    prediction_values = cursor.fetchall()
    return json.dumps(prediction_values)

cursor.close