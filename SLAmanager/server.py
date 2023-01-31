from concurrent import futures
import logging
import mysql.connector

import grpc
import sla_pb2
import sla_pb2_grpc

import time

class SlaService(sla_pb2_grpc.SlaServiceServicer):

    def SetSla(self, request, context):
        sql= "SELECT metric_name FROM datas WHERE metric_name LIKE %s LIMIT 1"
        val=['%"'+request.metric_name+'"%']
        print(f'metrica inserita: {request.metric_name} {request.min} {request.max}')
        try:
            db.ping()
            cursor = db.cursor()
            cursor.execute(sql,val)
            result=cursor.fetchone()
        except Exception as sql_execute_err:
            print("Errore: ", sql_execute_err)
            cursor.close()
            return sla_pb2.SlaReply(msg=f'Errore: {sql_execute_err}, {type(sql_execute_err)}')
        if result == None:
            cursor.close()
            return sla_pb2.SlaReply(msg=f'Non ci sono metriche disponibili chiamate {request.metric_name}')
            
        try:
            cursor.execute("SELECT metric_name FROM sla")
            slalist= cursor.fetchall()
        except Exception as sql_execute_err:
            print("Errore: ", sql_execute_err)
            cursor.close()
            return sla_pb2.SlaReply(msg=f'Errore: {sql_execute_err}, {type(sql_execute_err)}')

        if len(slalist) >=5:
            if (request.metric_name,) in slalist:
                sql = "UPDATE sla SET min = %s, max = %s WHERE metric_name = %s"
                val = (request.min, request.max, request.metric_name)
            else:
                cursor.execute("DELETE FROM sla")
                sql = "INSERT INTO sla (metric_name, min, max) VALUES (%s,%s,%s)"
                val = (request.metric_name, request.min, request.max)                
        else:
            sql = "INSERT INTO sla (metric_name, min, max) VALUES (%s,%s,%s) ON DUPLICATE KEY UPDATE min = %s, max = %s;"
            val= (request.metric_name, request.min, request.max, request.min, request.max)
        try:
            db.ping()
            cursor.execute(sql, val)
            db.commit()
        except Exception as sql_execute_err:
            print("Errore: ", sql_execute_err)
            cursor.close()
            return sla_pb2.SlaReply(msg=f'Errore: {sql_execute_err}, {type(sql_execute_err)}')
        cursor.close()
        return sla_pb2.SlaReply(msg=f'Valori per {request.metric_name} inseriti correttamente')
             
    def SlaStatus(self, request, context):
        sql = "SELECT * FROM sla"
        try:
            db.ping()
            cursor = db.cursor()
            cursor.execute(sql) 
            list = cursor.fetchall() 
            print('SLA set: ',list)
            cursor.close()
        except Exception as sql_execute_err:
            print("Errore: ", sql_execute_err)
            cursor.close()
            return sla_pb2.SlaReply(msg=f'Errore: {sql_execute_err}, {type(sql_execute_err)}')

        msg =''
        for row in list: 
            for i in row:
                msg += str(i) + ' '
            print('Msg: ',msg)
            msg += '\n'
        return sla_pb2.SlaReply(msg=msg)

    def GetViolation(self, request, context):
        sql = "SELECT * FROM sla"
        try:
            cursor = db.cursor()
            cursor.execute(sql)
            slalist= cursor.fetchall()
        except Exception as sql_err:
            print("Errore: ", sql_err)
            cursor.close()
            yield sla_pb2.Violation(metric_name=f'Errore: {sql_err}', value=-1.0, num=-1)
        if len(slalist) == 0:
            yield sla_pb2.Violation(metric_name="Nessuna metrica inserita in SLA set", value=-1.0, num=-1)
        for metric in slalist:
            sql = "SELECT metric_name, max_1h, max_3h, max_12h, min_1h, min_3h, min_12h FROM datas WHERE metric_name LIKE %s"
            val = ['%' + metric[0] + '%']
            try:
                cursor.execute(sql,val)
                values_list = cursor.fetchall()
            except Exception as sql_err:
                print("Errore: ", sql_err)
                cursor.close()
                yield sla_pb2.Violation(metric_name=f'Errore: {sql_err}', value=-1.0, num=-1)
                
            count = 0
            for item in values_list:
                if float(item[4])<float(metric[1]):
                    value = float(item[4])-float(metric[1])
                    count+=1
                    yield sla_pb2.Violation(metric_name=item[0], value=value, num=1)
                if float(item[5])<float(metric[1]):
                    value = float(item[5])-float(metric[1])
                    count+=1
                    yield sla_pb2.Violation(metric_name=item[0], value=value, num=3)
                if float(item[6])<float(metric[1]):
                    value = float(item[6])-float(metric[1])
                    count+=1
                    yield sla_pb2.Violation(metric_name=item[0], value=value, num=12)
                if float(item[1])>float(metric[2]):
                    value = float(item[1])-float(metric[2])
                    count+=1
                    yield sla_pb2.Violation(metric_name=item[0], value=value, num=1)
                if float(item[2])>float(metric[2]):
                    value = float(item[2])-float(metric[2])
                    count+=1
                    yield sla_pb2.Violation(metric_name=item[0], value=value, num=3)
                if float(item[3])>float(metric[2]):
                    value = float(item[3])-float(metric[2])
                    count+=1
                    yield sla_pb2.Violation(metric_name=item[0], value=value, num=12)
            yield sla_pb2.Violation(metric_name=f'Violazioni di {metric[0]}', value=0.0, num=count)
        cursor.close()

    def GetFutureViolation(self, request, context):
        sql = "SELECT * FROM sla"
        try:
            cursor = db.cursor()
            cursor.execute(sql)
            slalist= cursor.fetchall()
        except Exception as sql_err:
            cursor.close()
            print("Errore: ", sql_err)
            yield sla_pb2.Violation(metric_name=f'Errore: {sql_err}', value=-1.0, num=-1)
        if len(slalist) == 0:
            yield sla_pb2.Violation(metric_name="Nessuna metrica inserita in SLA set", value=-1.0, num=-1)
        for metric in slalist:
            sql = "SELECT metric_name, max_predicted, min_predicted FROM datas WHERE metric_name LIKE %s"
            val = ['%' + metric[0] + '%']
            try:
                cursor.execute(sql,val)
                value_list = cursor.fetchall()
            except Exception as sql_err:
                print("Errore: ", sql_err)
                cursor.close()
                yield sla_pb2.Violation(metric_name=f'Errore: {sql_err}', value=-1.0, num=-1)
            
            count = 0
            for item in value_list:
                if float(item[1])>float(metric[2]):
                    value= float(item[1])-float(metric[2])
                    count += 1
                    yield sla_pb2.Violation(metric_name=item[0],value=value)
                if float(item[2])<float(metric[1]):
                    value= float(item[2])-float(metric[1])
                    count += 1
                    yield sla_pb2.Violation(metric_name=item[0],value=value)
            yield sla_pb2.Violation(metric_name=f'Violazioni di {metric[0]} nei prossimi 10 minuti', value=0.0, num=count)
        cursor.close()

def dbconnect():
    while True:
        try:
            db = mysql.connector.connect(
                host = "db",
                user = "user", 
                password = "password",
                database = "prometheus_data"
            )
            print("Connessione col db effettuata")
            break
        except Exception as sqlerr:
            print("Errore: ", sqlerr)
            time.sleep(10)
    return db

def serve():
    port = '50051'
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    sla_pb2_grpc.add_SlaServiceServicer_to_server(SlaService(), server)
    server.add_insecure_port('[::]:' + port)
    server.start()
    print("SLA Manager started, listening on " + port)
    server.wait_for_termination()


if __name__ == '__main__':
    time.sleep(10)
    db = dbconnect()
    serve()
