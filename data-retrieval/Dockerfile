FROM python:slim
RUN python3 -m venv venv && . venv/bin/activate
RUN pip install flask mysql-connector-python



WORKDIR /usr/src/app

COPY DataRetrieval.py /usr/src/app/

ENV FLASK_APP=DataRetrieval.py
ENV FLASK_RUN_HOST=0.0.0.0
ENV FLASK_RUN_PORT=5000
ENV FLASK_ENV=development

ENTRYPOINT ["flask", "run"]

