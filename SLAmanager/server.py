from concurrent import futures
import logging
import mysql.connector

import grpc
import sla_pb2
import sla_pb2_grpc
#-----
import time

class SlaService(sla_pb2_grpc.SlaServiceServicer):

    def SetSla(self, request, context):
        #cursor = db.cursor()
        #cursor.execute("SELECT metric_name FROM sla")
        #metric_list = cursor.fetchall()
        #while True:
            #if len(metric_list)<5: 
                sql = "INSERT INTO sla (metric_name, min, max) VALUES (%s,%s,%s), ON DUPLICATE KEY UPDATE min = %s, max = %s;"
                val= (request.metric_name, request.min, request.max)
                try:
                    cursor.execute(sql, val)
                except Exception as sql_execute_err:
                    print("Errore: ", sql_execute_err)
                db.commit()
                return sla_pb2.SlaReply(f'Valori per {request.metric_name} inseriti correttamente')
                #serve fare pure delete/sostituzione per tenerne solol 5 come richiesto?
                '''else:#sostituisci uno
                    val = (request.min, request.max)
                    try:
                        cursor.execute("UPDATE sla SET min WHERE metric_name = '%s' (metric_name, min, max) VALUES (%s,%s,%s)", val)
                    except Exception as sql_execute_err:
                    return sla_pb2.SlaReply('Range della metrica inserito correttamente') #devo mettere msg= ?
                '''
    def SlaStatus(self, request, context):
        #sql = "SELECT * FROM sla WHERE metric_name LIKE VALUES(%s)"
        #val = '%s' + request.metric_name + '%s'
        sql = "SELECT * FROM sla"
        try:
            cursor.execute(sql) #, val)
            list = cursor.fetchall() #torna una lista di tuple
        except Exception as sql_execute_err:
            print("Errore: ", sql_execute_err)
        msg =''
        for row in list:
            msg += ', '.join(row) 
        #msg= f'Nome: {list[1]}, max 1h: {list[3]}, max 3h: {list[4]}, max 12h: {list[5]}, min 1h: {list[6]}, min 3h: {list[7]}, min 12h: {list[8]}, media 1h: {list[9]}, media 3h: {list[10]}, media 12h: {list[11]}, deviazione standard 1h  '
        return sla_pb2.SlaReply(msg)

    def GetViolation(self, request, context):
        sql = "SELECT * FROM sla"
        try:
            cursor.execute(sql)
            slalist= cursor.fetchall()
        except Exception as sql_err:
            print("Errore: ", sql_err)
        for metric in slalist:
            sql = "SELECT metric_name, max_1h, max_3h, max_12h, min_1h, min_3h, min_12h FROM datas WHERE metric_name LIKE VALUES(%s)"
            val = '%s' + metric[0] + '%s'
            try:
                cursor.execute(sql,val)
                values_list = cursor.fetchall()
            except Exception as sql_err:
                print("Errore: ", sql_err)
            for item in values_list:
                count = 0
                if float(item[4])<float(metric[1]):
                    value = float(item[4])-float(metric[1])
                    count+=1
                    yield sla_pb2.Violation(item[0], value, 1)
                if float(item[5])<float(metric[1]):
                    value = float(item[5])-float(metric[1])
                    count+=1
                    yield sla_pb2.Violation(item[0], value, 3)
                if float(item[6])<float(metric[1]):
                    value = float(item[6])-float(metric[1])
                    count+=1
                    yield sla_pb2.Violation(item[0], value, 12)
                if float(item[1])>float(metric[2]):
                    value = float(item[1])-float(metric[2])
                    count+=1
                    yield sla_pb2.Violation(item[0], value, 1)
                if float(item[2])>float(metric[2]):
                    value = float(item[2])-float(metric[2])
                    count+=1
                    yield sla_pb2.Violation(item[0], value, 3)
                if float(item[3])>float(metric[2]):
                    value = float(item[3])-float(metric[2])
                    count+=1
                    yield sla_pb2.Violation(item[0], value, 12)
                yield sla_pb2.Violation(f'Violazioni di {metric[0]}: ', 0.0, count)

    def GetFutureViolation(self, request, context):
        sql = "SELECT * FROM sla"
        try:
            cursor.execute(sql)
            slalist= cursor.fetchall()
        except Exception as sql_err:
            print("Errore: ", sql_err)
        for metric in slalist:
            sql = "SELECT metric_name, max_predicted, min_predicted FROM datas WHERE metric_name LIKE VALUES(%s)"
            val = '%s' + metric[0] + '%s'
            try:
                cursor.execute(sql,val)
                value_list = cursor.fetchall()
            except Exception as sql_err:
                print("Errore: ", sql_err)
            for item in value_list:
                count = 0
                if float(item[1])>float(metric[2]):
                    value= float(item[1])-float(metric[2])
                    count += 1
                    yield sla_pb2.Violation(item[0],value)
                if float(item[2])<float(metric[1]):
                    value= float(item[2])-float(metric[1])
                    count += 1
                    yield sla_pb2.Violation(item[0],value)
                yield sla_pb2.Violation(f'Violazioni future di {metric[0]}: ', 0.0, count)

'''class EchoService(echo_pb2_grpc.EchoServiceServicer):

    def EchoMessage(self, request, context):
        time.sleep(5)
        return echo_pb2.EchoReply(content='echo: %s!' % request.content)'''

def dbconnect():
    while True:
        try:
            db = mysql.connector.connect(
                host = "db",
                user = "user", #va bene lo stesso user di datastorage?
                password = "password",
                database = "prometheus_data"
            )
            print("Connessione col db effettuata")
            break
        except Exception as sqlerr:
            print("Errore: ", sqlerr)
    cursor = db.cursor()
    #spostare in database.sql per creare la tabella all'avvio
    cursor.execute("""CREATE TABLE IF NOT EXIST sla (
        metric_name VARCHAR(255) NOT NULL, 
        min INT, 
        max INT, 
        PRIMARY_KEY (metric_name)""")
    return db, cursor

def serve():
    port = '50051'
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    sla_pb2_grpc.add_EchoServiceServicer_to_server(SlaService(), server)
    server.add_insecure_port('[::]:' + port)
    server.start()
    print("SLA Manager started, listening on " + port)
    server.wait_for_termination()


if __name__ == '__main__':
    logging.basicConfig()
    db, cursor = dbconnect()
    serve()
