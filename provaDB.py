
import json
import mysql.connector

while True:
    try:
        db = mysql.connector.connect(
        host = "127.0.0.1",
        port = 3310,
        user = "user",
        password = "password",
        database="prometheus_data"
        )
        break
    except Exception as sqlerr:
        print("Errore: ", sqlerr)

'''def execute_query(connection, query):
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        connection.commit()
        print("Query successful")
    except Error as err:
        print(f"Error: '{err}'")'''



#Creating an instance of 'cursor' class
# which is used to execute the 'SQL'
# statements in 'Python' cursor.execute("SQLCODE")
cursor = db.cursor()




sql = """INSERT INTO datas ( 
        max_1h, max_3h, max_12h,
        min_1h, min_3h, min_12h,
        avg_1h, avg_3h, avg_12h,
        devstd_1h, devstd_3h, devstd_12h,
        max_predicted, min_predicted, avg_predicted,
        autocorrelazione, stazionarieta, stagionalita)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"""

val = (1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18)
cursor.execute(sql, val) #controllare se lancia eccezioni e controllare
db.commit()
print("Inserito!")
            