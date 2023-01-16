FROM python:3.11.1-slim-bullseye

WORKDIR /usr/src/app

RUN pip install prometheus-api-client

COPY . .

CMD [ "python", "./ETLDataPipeline.py" ]