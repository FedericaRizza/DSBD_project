from confluent_kafka import Consumer
import json
import mysql.connector
import time

while True:
    try:
        db = mysql.connector.connect( #controllare se è giusto se dopo 20 min si scollega
        host = "db",
        user = "user",
        password = "password",
        database="prometheus_data"
        )
        break
    except Exception as sqlerr:
        print("Errore: ", sqlerr)
        time.sleep(10)

'''def execute_query(connection, query):
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        connection.commit()
        print("Query successful")
    except Error as err:
        print(f"Error: '{err}'")'''

#preparazione del consumer kafka
consumer = Consumer({
    'bootstrap.servers': 'broker_kafka:9092',
    'group.id': 'DataStorage',
    'auto.offset.reset': 'earliest' 
})

#Creating an instance of 'cursor' class
# which is used to execute the 'SQL'
# statements in 'Python' cursor.execute("SQLCODE")
cursor = db.cursor()

#iscrizione al topic
consumer.subscribe(['prometheusdata'])

#total_count = 0
try:
    while True:
        msg = consumer.poll(1.0) #timeout di un secondo? #pool = none se non ci sono messaggi nel topic
        if msg is None: 
            print("Waiting for message or event/error in poll()")
            continue
        elif msg.error():
            print('error: {}'.format(msg.error()))
        else:
            record_value = msg.value()
            data = json.loads(record_value) #ritorna un dictionary?
            if data['datatype'] == 'data':
                print(data['metric_name'])
                print("lunghezza", len(data['metric_name']))

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
            #provare a cambiare inset in qualcosa che lo aggiorna, devo inserire la prima volta e poi per le stessa metrica 
                val = (json.dumps(data['metric_name']),  data['max_1h'], data['max_3h'], data['max_12h'], data['min_1h'], data['min_3h'], data['min_12h'], data['avg_1h'], data['avg_3h'],
                data['avg_12h'], data['devstd_1h'], data['devstd_3h'], data['devstd_12h'], data['max_predicted'], data['min_predicted'], data['avg_predicted'], 
                data['max_1h'], data['max_3h'], data['max_12h'], data['min_1h'], data['min_3h'], data['min_12h'], data['avg_1h'], data['avg_3h'],
                data['avg_12h'], data['devstd_1h'], data['devstd_3h'], data['devstd_12h'], data['max_predicted'], data['min_predicted'], data['avg_predicted'] 
                )
                #print(val)
                try:
                    cursor.execute(sql, val) #controllare se lancia eccezioni e controllare
                except Exception as sql_execute_err:
                    print("Errore: ", sql_execute_err)
                db.commit()
                print("insert datas ok!")  

            if data['datatype'] == 'metadata':
                acf_data = data['autocorrelazione']
                print (acf_data)
                sql = """INSERT INTO datas (metric_name, stazionarieta, stagionalita) VALUES (%s, %s, %s)
                    ON DUPLICATE KEY UPDATE stazionarieta = %s, stagionalita = %s;"""
                val = (json.dumps(data['metric_name']), data['stazionarieta'], data['stagionalita'], data['stazionarieta'], data['stagionalita'])
                
                try:
                    cursor.execute(sql, val) #controllare se lancia eccezioni e controllare
                except Exception as sql_execute_err:
                    print("Errore: ", sql_execute_err)
                db.commit()

                last_id = cursor.lastrowid
                #print(last_id)

                cursor.execute("DELETE FROM acf WHERE ID_metrica = %s", [cursor.lastrowid])
                db.commit()

                for item in acf_data:

                    sql1 = """INSERT INTO acf (ID_metrica, acf_lag, acf_value)
                            VALUES (%s, %s, %s)"""
                            #ON DUPLICATE KEY UPDATE acf_value = %s
                    val1 = (last_id, item, acf_data[item])
                    print(item)
                    print(last_id)
                    try:
                        cursor.execute(sql1, val1) #controllare se lancia eccezioni e controllare
                    except Exception as sql_execute_err:
                        print("Errore: ", sql_execute_err)
                    db.commit()
                    print("insert acf ok!") 
                print("insert metadatas ok!")
                

#per prendere il SIGINT -> guardare compose     
except KeyboardInterrupt:
    pass
finally:
    # Leave group and commit final offsets
    consumer.close()
