##  Implementation

```bash
# 1. Navigate to implementation folder
cd implementation/

# 2. Start all services
docker-compose up --build

# 3. Watch the logs! 
```

### Stop Everything
```bash
# Ctrl+C to stop
# Then clean up:
docker-compose down
```

---

##  Experiment

### Prerequisites
- Python 3.9+ installed
- MQTT broker running (use implementation's broker)


```bash
# 1. Navigate to experiment folder
cd experiment/

# 2. Install Python dependencies
pip install -r requirements.txt

# 3. Start MQTT broker (in separate terminal)
cd ../implementation
docker-compose up mqtt-broker

# 4. Start echo servers (in 2 separate terminals)
# Terminal 1:
python mqtt_echo_server.py

# Terminal 2:
python http_server.py

# 5. Run experiment (in main terminal)
python test_harness.py
```

```bash
# Run statistical analysis
python analysis.py
```


##  Accessing Services

### MQTT Broker
```bash
# Subscribe to all topics (for debugging)
mosquitto_sub -h localhost -p 1883 -t '#' -v

# Publish test message
mosquitto_pub -h localhost -p 1883 -t test/topic -m "Hello"
```

### PostgreSQL Database
```bash
# Connect to database
docker exec -it postgres-db psql -U admin -d can_filling_db

# View production data
SELECT * FROM production_events ORDER BY event_timestamp DESC LIMIT 10;

# View summary statistics
SELECT * FROM production_summary;

# Exit
\q
```

### HTTP Server
```bash
# Health check
curl http://localhost:5000/health

# Send test message
curl -X POST http://localhost:5000/message \
  -H "Content-Type: application/json" \
  -d '{"id": 1, "data": "test"}'
```

##  Troubleshooting

### Issue: "Port 1883 already in use"
**Solution:**
```bash
# Kill existing MQTT broker
docker stop mqtt-broker
# Or use different port in docker-compose.yml
```

### Issue: "Connection refused" from Python scripts
**Solution:**
```bash
# Make sure MQTT broker is running
docker-compose ps

# Check if port is open
nc -zv localhost 1883
```

### Issue: Database connection fails
**Solution:**
```bash
# Wait for database to be ready (healthcheck)
docker-compose logs postgres

# Manually test connection
docker exec -it postgres-db psql -U admin -d can_filling_db -c "SELECT 1;"
```

### Issue: Python packages not found
**Solution:**
```bash
# Reinstall dependencies
pip install -r requirements.txt

# Or use virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install -r requirements.txt
```