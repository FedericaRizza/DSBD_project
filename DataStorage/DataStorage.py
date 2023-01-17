from confluent_kafka import Consumer
import json
import mysql.connector

db = mysql.connector.connect(
  host = "localhost",
  user = "user",
  password = "password",
  database="prometheus_data"
)

consumer = Consumer({
    'bootstrap.servers': 'localhost:29092',
    'group.id': 'DataStorage',
    'auto.offset.reset': 'latest' 
})

#Creating an instance of 'cursor' class
# which is used to execute the 'SQL'
# statements in 'Python' cursor.execute("SQLCODE")
cursor = db.cursor()

consumer.subscribe(['prometheusdata'])

#total_count = 0
try:
    while True:
        msg = consumer.poll(1.0) #timeout di un secondo?
        if msg is None:
                # No message available within timeout.
                # Initial message consumption may take up to
                # `session.timeout.ms` for the consumer group to
                # rebalance and start consuming
            print("Waiting for message or event/error in poll()")
            continue
        elif msg.error():
            print('error: {}'.format(msg.error()))
        else:
            #record_key = msg.key()
            record_value = msg.value()
            data = json.loads(record_value) #ritorna un dictionary?
            sql = """INSERT INTO datas 
                    (ID_metrica, 
                    max_1h, max_3h, max_12h,
                    min_1h, min_3h, min_12h,
                    avg_1h, avg_3h, avg_12h,
                    devstd_1h, devstd_3h, devstd_12h,
                    max_predicted, min_predicted, avg_predicted,
                    autocorrelazione, stazionarieta, stagionalita)
                    VALUES (%f, %f, %f, %f, %f, %f, %f, %f, %f, %f, %f, %f, %f, %f, %f, %f, %f, %f);"""
            val = (data['max_1h'], data['max_3h'], data['max_12h'], data['min_1h'], data['min_3h'], data['min_12h'], data['avg_1h'], data['avg_3h'],
             data['avg_12h'], data['devstd_1h'], data['devstd_3h'], data['devstd_12h'], data['max_predicted'], data['min_predicted'], data['avg_predicted'], data['autocorrelazione'], data['stazionarieta'], data['stagionalita'])
            cursor.execute(sql,val)
            #count = data['count']
            #total_count += count
            #print("Consumed record with key {} and value {}, and updated total count to {}".format(record_key, record_value, total_count))
except KeyboardInterrupt:
    pass
finally:
    # Leave group and commit final offsets
    consumer.close()
