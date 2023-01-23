from __future__ import print_function

import logging

import grpc
import sla_pb2
import sla_pb2_grpc


def run():
    print("Richiesta SLA Status in corso...")
    with grpc.insecure_channel('localhost:50051') as channel:
        stub = sla_pb2_grpc.SlaServiceStub(channel)
        response = stub.SlaStatus(sla_pb2.SlaRequest())
    print("SLA Status: " + response.msg)


if __name__ == '__main__':
    logging.basicConfig()
    run()
