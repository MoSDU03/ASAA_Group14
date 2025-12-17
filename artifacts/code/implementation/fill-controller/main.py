#/usr/bin/env python3
"""
Fill Controller for Can Filling System
Implements state machine for fill control logic
"""

import time
import json
import os
from enum import Enum
from datetime import datetime
import paho.mqtt.client as mqtt
import psycopg2
from psycopg2.extras import RealDictCursor

# Configuration
MQTT_BROKER = os.getenv('MQTT_BROKER', 'localhost')
MQTT_PORT = int(os.getenv('MQTT_PORT', 1883))
DB_HOST = os.getenv('DB_HOST', 'localhost')
DB_PORT = int(os.getenv('DB_PORT', 5432))
DB_NAME = os.getenv('DB_NAME', 'can_filling_db')
DB_USER = os.getenv('DB_USER', 'admin')
DB_PASSWORD = os.getenv('DB_PASSWORD', 'secret123')

TARGET_VOLUME = 330
VOLUME_TOLERANCE = 5
FILL_TIMEOUT = 3000  # ms

class FillState(Enum):
    IDLE = "IDLE"
    WAITING_POSITION = "WAITING_POSITION"
    FILLING = "FILLING"
    CLOSING_VALVE = "CLOSING_VALVE"
    COMPLETE = "COMPLETE"
    TIMEOUT = "TIMEOUT"

class FillController:
    def __init__(self):
        self.state = FillState.IDLE
        self.current_can_id = None
        self.fill_level = 0
        self.cycle_start_time = None
        self.position_valid = False
        self.seal_complete = False
        
        # MQTT setup
        self.client = mqtt.Client("FillController")
        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message
        
        # Database connection
        self.db_conn = None
        
    def connect_db(self):
        """Connect to PostgreSQL database"""
        # Wait a bit for postgres to be ready
        time.sleep(3)
        
        try:
            # Use connection string to be explicit
            conn_string = f"host={DB_HOST} port={DB_PORT} dbname={DB_NAME} user={DB_USER} password={DB_PASSWORD}"
            self.db_conn = psycopg2.connect(conn_string)
            print(f" Connected to database: {DB_NAME}@{DB_HOST}")
        except Exception as e:
            print(f" Database connection failed: {e}")
            print("  (Continuing without database logging)")
            self.db_conn = None
            
    def on_connect(self, client, userdata, flags, rc):
        print(f" Connected to MQTT broker")
        # Subscribe to sensor topics
        client.subscribe("sensor/position")
        client.subscribe("sensor/position_confirmed")
        client.subscribe("sensor/fill_level")
        client.subscribe("sensor/pressure")
        client.subscribe("status/quality")
        print(" Subscribed to sensor topics")
        
    def on_message(self, client, userdata, msg):
        """Handle incoming MQTT messages"""
        try:
            payload = json.loads(msg.payload.decode())
            topic = msg.topic
            
            if topic == "sensor/position":
                self.handle_position(payload)
            elif topic == "sensor/position_confirmed":
                self.handle_position_confirmed(payload)
            elif topic == "sensor/fill_level":
                self.handle_fill_level(payload)
            elif topic == "sensor/pressure":
                self.handle_pressure(payload)
            elif topic == "status/quality":
                self.handle_quality_result(payload)
                
        except Exception as e:
            print(f" Error processing message: {e}")
            
    def handle_position(self, data):
        """Handle can position detection"""
        if self.state == FillState.IDLE and data.get('valid'):
            print(f"\n[CONTROLLER] Can detected at position ({data['x']}, {data['y']})")
            self.state = FillState.WAITING_POSITION
            self.cycle_start_time = time.time()
            
    def handle_position_confirmed(self, data):
        """Handle position validation"""
        if self.state == FillState.WAITING_POSITION:
            self.current_can_id = data.get('can_id')
            self.position_valid = True
            print(f"[CONTROLLER] Position confirmed for {self.current_can_id}")
            self.start_filling()
            
    def start_filling(self):
        """Transition to FILLING state"""
        if self.state == FillState.WAITING_POSITION and self.position_valid:
            self.state = FillState.FILLING
            self.fill_level = 0
            print(f"[CONTROLLER]   Starting fill cycle for {self.current_can_id}")
            
            # Publish valve open command
            self.client.publish("control/valve/command", json.dumps({
                "action": "OPEN",
                "can_id": self.current_can_id,
                "timestamp": int(time.time() * 1000)
            }))
            
    def handle_fill_level(self, data):
        """Handle fill level updates"""
        if self.state == FillState.FILLING:
            self.fill_level = data.get('volume_ml', 0)
            
            # Check if target reached
            if self.fill_level >= (TARGET_VOLUME - VOLUME_TOLERANCE):
                self.close_valve()
                
            # Check timeout
            if self.cycle_start_time:
                elapsed_ms = (time.time() - self.cycle_start_time) * 1000
                if elapsed_ms > FILL_TIMEOUT:
                    self.handle_timeout()
                    
    def close_valve(self):
        """Close valve and transition to CLOSING_VALVE state"""
        if self.state == FillState.FILLING:
            self.state = FillState.CLOSING_VALVE
            print(f"[CONTROLLER]   Target reached ({self.fill_level:.1f}ml), closing valve")
            
            # Publish valve close command
            self.client.publish("control/valve/command", json.dumps({
                "action": "CLOSE",
                "can_id": self.current_can_id,
                "timestamp": int(time.time() * 1000)
            }))
            
            # Transition to complete after brief delay
            time.sleep(0.05)
            self.state = FillState.COMPLETE
            
            cycle_time_ms = int((time.time() - self.cycle_start_time) * 1000)
            print(f"[CONTROLLER]  Fill complete: {self.fill_level:.1f}ml in {cycle_time_ms}ms")
            
            # Publish fill complete status
            self.client.publish("status/fill_complete", json.dumps({
                "can_id": self.current_can_id,
                "fill_level": self.fill_level,
                "cycle_time_ms": cycle_time_ms,
                "timestamp": int(time.time() * 1000)
            }))
            
    def handle_timeout(self):
        """Handle fill timeout"""
        self.state = FillState.TIMEOUT
        print(f"[CONTROLLER]   TIMEOUT: Fill operation exceeded {FILL_TIMEOUT}ms")
        
        # Close valve emergency
        self.client.publish("control/valve/command", json.dumps({
            "action": "CLOSE",
            "emergency": True,
            "timestamp": int(time.time() * 1000)
        }))
        
        # Log failure
        self.log_production(quality="FAIL", reject_reason="TIMEOUT")
        self.reset()
        
    def handle_pressure(self, data):
        """Handle pressure readings (seal phase)"""
        if self.state == FillState.COMPLETE:
            seal_ok = data.get('seal_integrity', False)
            if seal_ok:
                print(f"[CONTROLLER]  Seal verified OK")
            else:
                print(f"[CONTROLLER]   Seal failed")
                
    def handle_quality_result(self, data):
        """Handle final quality check result"""
        if self.state == FillState.COMPLETE:
            result = data.get('result')
            can_id = data.get('can_id')
            
            if result == "PASS":
                print(f"[CONTROLLER] {can_id}: Quality PASS")
                self.log_production(quality="PASS")
            else:
                print(f"[CONTROLLER]  {can_id}: Quality FAIL")
                self.log_production(quality="FAIL", reject_reason="QUALITY_CHECK")
                
            self.reset()
            
    def log_production(self, quality, reject_reason=None):
        """Log production event to database"""
        if not self.db_conn or not self.current_can_id:
            return
            
        try:
            cycle_time_ms = int((time.time() - self.cycle_start_time) * 1000) if self.cycle_start_time else 0
            
            with self.db_conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO production_events 
                    (can_id, cycle_time_ms, fill_level_ml, seal_verified, quality_result, reject_reason)
                    VALUES (%s, %s, %s, %s, %s, %s)
                """, (
                    self.current_can_id,
                    cycle_time_ms,
                    int(self.fill_level),
                    True,  # Simplified: assume seal checked
                    quality,
                    reject_reason
                ))
                self.db_conn.commit()
                
        except Exception as e:
            print(f" Database logging error: {e}")
            
    def reset(self):
        """Reset for next can"""
        self.state = FillState.IDLE
        self.current_can_id = None
        self.fill_level = 0
        self.cycle_start_time = None
        self.position_valid = False
        self.seal_complete = False
        
    def run(self):
        """Main controller loop"""
        print("\n" + "="*60)
        print("FILL CONTROLLER - Starting")
        print("="*60)
        
        # Connect to database
        self.connect_db()
        
        # Connect to MQTT
        print(f"Connecting to MQTT broker at {MQTT_BROKER}:{MQTT_PORT}...")
        self.client.connect(MQTT_BROKER, MQTT_PORT, 60)
        
        print("\n Fill Controller ready")
        print("  Waiting for cans...\n")
        
        try:
            self.client.loop_forever()
        except KeyboardInterrupt:
            print("\n\n Controller stopped by user")
        finally:
            self.client.disconnect()
            if self.db_conn:
                self.db_conn.close()

if __name__ == "__main__":
    controller = FillController()
    controller.run()
