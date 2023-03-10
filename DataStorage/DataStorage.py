from confluent_kafka import Consumer
import json
import mysql.connector
import time

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

consumer = Consumer({
    'bootstrap.servers': 'broker_kafka:9092',
    'group.id': 'DataStorage',
    'auto.offset.reset': 'earliest' 
})

cursor = db.cursor()

consumer.subscribe(['prometheusdata'])

try:
    while True:
        msg = consumer.poll(1.0) 
        if msg is None: 
            print("Waiting for message or event/error in poll()")
            continue
        elif msg.error():
            print('error: {}'.format(msg.error()))
        else:
            record_value = msg.value()
            data = json.loads(record_value) 
            
            if data['datatype'] == "data":

                sql = """INSERT INTO datas (
                    metric_name,  
                    max_1h, max_3h, max_12h,
                    min_1h, min_3h, min_12h,
                    avg_1h, avg_3h, avg_12h,
                    devstd_1h, devstd_3h, devstd_12h,
                    max_predicted, min_predicted, avg_predicted)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE max_1h = %s, max_3h =  %s, max_12h = %s,
                    min_1h = %s, min_3h = %s, min_12h = %s, 
                    avg_1h = %s, avg_3h = %s, avg_12h = %s,
                    devstd_1h = %s, devstd_3h = %s, devstd_12h = %s,
                    max_predicted = %s, min_predicted = %s, avg_predicted = %s;"""
                val = (json.dumps(data['metric_name']),  data['max_1h'], data['max_3h'], data['max_12h'], data['min_1h'], data['min_3h'], data['min_12h'], data['avg_1h'], data['avg_3h'],
                data['avg_12h'], data['devstd_1h'], data['devstd_3h'], data['devstd_12h'], data['max_predicted'], data['min_predicted'], data['avg_predicted'], 
                data['max_1h'], data['max_3h'], data['max_12h'], data['min_1h'], data['min_3h'], data['min_12h'], data['avg_1h'], data['avg_3h'],
                data['avg_12h'], data['devstd_1h'], data['devstd_3h'], data['devstd_12h'], data['max_predicted'], data['min_predicted'], data['avg_predicted'] 
                )
                try:
                    cursor.execute(sql, val) 
                    db.commit()
                    print("insert datas ok!")  
                except Exception as sql_execute_err:
                    print("Errore: ", sql_execute_err)

            if data['datatype'] == "metadata":
                acf_data = data['autocorrelazione']
                sql = """INSERT INTO datas (metric_name, stazionarieta, stagionalita) VALUES (%s, %s, %s)
                    ON DUPLICATE KEY UPDATE stazionarieta = %s, stagionalita = %s;"""
                val = (json.dumps(data['metric_name']), data['stazionarieta'], data['stagionalita'], data['stazionarieta'], data['stagionalita'])
                
                try:
                    cursor.execute(sql, val) 
                    db.commit()
                except Exception as sql_execute_err:
                    print("Errore: ", sql_execute_err)

                last_id = cursor.lastrowid
                try:
                    cursor.execute("DELETE FROM acf WHERE ID_metrica = %s", [cursor.lastrowid])
                    db.commit()
                except Exception as sql_execute_err:
                    print("Errore: ", sql_execute_err)

                for item in acf_data:

                    sql1 = """INSERT INTO acf (ID_metrica, acf_lag, acf_value)
                            VALUES (%s, %s, %s)"""                            
                    val1 = (last_id, item, acf_data[item])
                    try:
                        cursor.execute(sql1, val1) 
                        db.commit()
                    except Exception as sql_execute_err:
                        print("Errore: ", sql_execute_err)
                print("insert acf ok!") 
                print("insert metadatas ok!")
                
except KeyboardInterrupt:
    pass
finally:    
    consumer.close()
