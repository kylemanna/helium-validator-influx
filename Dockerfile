FROM python:3.10-alpine

RUN pip install --upgrade pip

RUN adduser -D worker
USER worker
WORKDIR /app

COPY requirements.txt /tmp/requirements.txt
RUN pip3 install --no-warn-script-location --user --no-cache -r /tmp/requirements.txt

COPY --chown=worker:worker . .

CMD [ "python3", "bridge-influx.py"]

