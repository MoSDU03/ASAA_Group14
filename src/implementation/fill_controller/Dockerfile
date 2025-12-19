FROM python:3.9-slim

WORKDIR /app

RUN pip install --no-cache-dir paho-mqtt psycopg2-binary

COPY controller.py .

CMD ["python", "-u", "controller.py"]