FROM python:3.9-slim-bullseye
RUN pip install prometheus_api_client confluent-kafka mysql-connector-python

WORKDIR /usr/src/app

ENTRYPOINT ["python"]
CMD ["DataStorage.py"]