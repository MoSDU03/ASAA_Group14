import paho.mqtt.client as mqtt
import json
import time
import random
from datetime import datetime

class SensorSimulator:
    def __init__(self, broker='localhost', port=1883):
        self.broker = broker
        self.port = port
        self.client = mqtt.Client(client_id="sensor_simulator")
        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message
        
        self.can_detected = False
        self.filling_active = False
        self.current_level = 0
        self.position_mm = 0
        self.can_counter = 0
        
    def on_connect(self, client, userdata, flags, rc):
        print(f"[{datetime.now()}] Sensor Simulator connected to MQTT broker (rc={rc})")
        client.subscribe("valve/command")
        client.subscribe("system/control")
        
    def on_message(self, client, userdata, msg):
        try:
            payload = json.loads(msg.payload.decode())
            
            if msg.topic == "valve/command":
                if payload.get("action") == "open":
                    self.filling_active = True
                    self.current_level = 0
                    print(f"[{datetime.now()}] Valve opened - starting fill")
                elif payload.get("action") == "close":
                    self.filling_active = False
                    print(f"[{datetime.now()}] Valve closed - final level: {self.current_level}ml")
                    
        except Exception as e:
            print(f"Error processing message: {e}")
    
    def generate_can_arrival(self):
        time.sleep(random.uniform(0.8, 2.5))
        
        self.can_counter += 1
        self.can_detected = True
        self.position_mm = random.gauss(0, 0.8)
        
        message = {
            "timestamp": datetime.now().isoformat(),
            "can_id": self.can_counter,
            "detected": True
        }
        
        self.client.publish("sensor/can_detected", json.dumps(message), qos=1)
        print(f"[{datetime.now()}] Can #{self.can_counter} detected")
        
        time.sleep(0.02)
        
        position_message = {
            "timestamp": datetime.now().isoformat(),
            "can_id": self.can_counter,
            "position_mm": round(self.position_mm, 2),
            "valid": abs(self.position_mm) <= 2.0
        }
        
        self.client.publish("sensor/position", json.dumps(position_message), qos=1)
        print(f"[{datetime.now()}] Position: {self.position_mm:.2f}mm (valid={position_message['valid']})")
        
    def simulate_filling(self):
        while self.filling_active and self.current_level < 340:
            time.sleep(0.05)
            
            fill_rate = random.gauss(1.5, 0.2)
            self.current_level += fill_rate
            
            level_message = {
                "timestamp": datetime.now().isoformat(),
                "can_id": self.can_counter,
                "level_ml": round(self.current_level, 2),
                "target": 330.0,
                "tolerance": 5.0
            }
            
            self.client.publish("sensor/level", json.dumps(level_message), qos=1)
            
            if self.current_level >= 325:
                print(f"[{datetime.now()}] Level: {self.current_level:.1f}ml")
    
    def run(self):
        self.client.connect(self.broker, self.port, 60)
        self.client.loop_start()
        
        print(f"[{datetime.now()}] Sensor Simulator started")
        print("Simulating can arrival every 0.8-2.5 seconds")
        print("=" * 60)
        
        try:
            while True:
                if not self.can_detected:
                    self.generate_can_arrival()
                
                if self.filling_active:
                    self.simulate_filling()
                    
                if self.can_detected and not self.filling_active and self.current_level > 0:
                    self.can_detected = False
                    self.current_level = 0
                    print(f"[{datetime.now()}] Can #{self.can_counter} released")
                    print("-" * 60)
                    
                time.sleep(0.1)
                
        except KeyboardInterrupt:
            print("Shutting down sensor simulator...")
            self.client.loop_stop()
            self.client.disconnect()

if __name__ == "__main__":
    import os
    broker = os.getenv('MQTT_BROKER', 'localhost')
    port = int(os.getenv('MQTT_PORT', 1883))
    
    time.sleep(5)
    
    simulator = SensorSimulator(broker=broker, port=port)
    simulator.run()
