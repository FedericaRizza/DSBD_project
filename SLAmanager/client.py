from __future__ import print_function

import logging

import grpc
import sla_pb2
import sla_pb2_grpc

def SetSla(name, min, max):
    print('Inserimento SLA')
    with grpc.insecure_channel('localhost:50051') as channel:
        stub = sla_pb2_grpc.SlaServiceStub(channel)
        request=sla_pb2.MetricValue(metric_name=name,min=min,max=max)

        print(f'{request.metric_name} {request.min} {request.max}')
        i = input ('Confermare? y/n ')
        if i == 'n':
            return
        response = stub.SetSla(request)
    return response

def Slastatus():
    print("Richiesta SLA Status in corso...")
    with grpc.insecure_channel('localhost:50051') as channel:
        stub = sla_pb2_grpc.SlaServiceStub(channel)
        response = stub.SlaStatus(sla_pb2.SlaRequest())
    print("SLA Status (nome, min, max):\n" + response.msg)

def GetViolation():
    print("Recupero violazioni SLA in corso...\n")
    with grpc.insecure_channel('localhost:50051') as channel:
        stub = sla_pb2_grpc.SlaServiceStub(channel)
        for item in stub.GetViolation(sla_pb2.SlaRequest()):
            if item.value == -1.0 and item.num == -1:
                print(f'{item.metric_name}')
                break
            if item.value == 0.0 and item.num > 0:
                print(f'{item.metric_name} totali: {item.num}\n\n')
            if item.value < 0.0 and item.num > 0:
                print(f'Nome:\n{item.metric_name}\nSoglia minima superata nelle precedenti {item.num} ore di {-item.value}\n')
            if item.value > 0.0:
                print(f'Nome:\n{item.metric_name}\nSoglia massima superata nelle precedenti {item.num} ore di {item.value}\n')

def GetFutureViolation():
    print("Recupero violazioni future SLA in corso...\n")
    with grpc.insecure_channel('localhost:50051') as channel:
        stub = sla_pb2_grpc.SlaServiceStub(channel)
        for item in stub.GetFutureViolation(sla_pb2.SlaRequest()):
            if item.value == -1.0 and item.num == -1:
                print(f'{item.metric_name}\n')
                break
            if item.value == 0.0 and item.num > 0:
                print(f'{item.metric_name} totali: {item.num}\n\n')
            if item.value < 0.0:
                print(f'Nome:\n{item.metric_name}\nPrevisto superamento soglia minima nei prossimi 10 minuti di {-item.value}\n')
            if item.value > 0.0:
                print(f'Nome:\n{item.metric_name}\nPrevisto superamento soglia massima nei prossimi 10 minuti di {item.value}\n')


if __name__ == '__main__':
    logging.basicConfig()
    while True:
        i = input('1) Aggiungi/modifica SLA  2) Mostra SLA  3) Mostra violazioni  4) Mostra violazioni future  0) Exit\n')
        if i=='1':
            name, min, max=input("Inserire il nome della metrica, il minimo e il massimo separati da spazi\n").split()
            response=SetSla(name, int(min), int(max))
            print(response.msg)
        elif i=='2':
            Slastatus()
        elif i=='3':
            GetViolation()
        elif i=='4':
            GetFutureViolation()
        elif i=='0':            
            break
        else:
            print ("Inserire un numero da 1 a 4!")
