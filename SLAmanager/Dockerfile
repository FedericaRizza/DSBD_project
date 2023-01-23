FROM python:3.9-slim-bullseye
RUN pip install mysql-connector-python grpcio grpcio-tools

WORKDIR /usr/src/app

COPY server.py sla_pb2_grpc.py sla_pb2.py sla_pb2.pyi .

ENTRYPOINT [ "python" ]
CMD ["server.py"]