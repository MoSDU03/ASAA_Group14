FROM python:3.9-slim

WORKDIR /app

RUN pip install --no-cache-dir paho-mqtt

COPY sensor_sim.py .

CMD ["python", "-u", "sensor_sim.py"]
