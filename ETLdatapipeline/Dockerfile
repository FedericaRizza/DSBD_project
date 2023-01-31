FROM python:3.9-slim-bullseye
RUN pip install schedule prometheus_api_client confluent-kafka statsmodels mysql-connector-python -U scikit-learn

WORKDIR /usr/src/app


COPY ETLDataPipeline.py .

ENTRYPOINT [ "python" ]
CMD ["ETLDataPipeline.py"]