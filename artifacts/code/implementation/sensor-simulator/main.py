#/usr/bin/env python3
"""
Sensor Simulator for Can Filling System
Simulates sensor readings and publishes to MQTT
"""

import time
import json
import random
import os
from datetime import datetime
import paho.mqtt.client as mqtt

# Configuration from environment
MQTT_BROKER = os.getenv('MQTT_BROKER', 'localhost')
MQTT_PORT = int(os.getenv('MQTT_PORT', 1883))
SIMULATION_SPEED = float(os.getenv('SIMULATION_SPEED', 1.0))  # 1.0 = real-time

class SensorSimulator:
    def __init__(self):
        self.client = mqtt.Client("SensorSimulator")
        self.client.on_connect = self.on_connect
        self.fill_level = 0
        self.can_count = 0
        
    def on_connect(self, client, userdata, flags, rc):
        print(f" Connected to MQTT broker at {MQTT_BROKER}:{MQTT_PORT}")
        
    def connect(self):
        print(f"Connecting to MQTT broker at {MQTT_BROKER}:{MQTT_PORT}...")
        self.client.connect(MQTT_BROKER, MQTT_PORT, 60)
        self.client.loop_start()
        time.sleep(1)  # Wait for connection
        
    def publish(self, topic, payload):
        """Publish message to MQTT with logging"""
        message = json.dumps(payload)
        self.client.publish(topic, message)
        
    def simulate_can_cycle(self):
        """Simulate one complete can filling cycle"""
        self.can_count += 1
        print(f"\n{'='*60}")
        print(f" Can #{self.can_count} - Starting cycle")
        print(f"{'='*60}")
        
        # 1. Can arrival detection
        print(" Detecting can position...")
        time.sleep(0.1 / SIMULATION_SPEED)
        position = {
            "x": round(125.0 + random.uniform(-2, 2), 2),
            "y": round(50.0 + random.uniform(-1, 1), 2),
            "valid": True,
            "timestamp": int(time.time() * 1000)
        }
        self.publish("sensor/position", position)
        print(f"   Position: ({position['x']}, {position['y']}) mm")
        
        time.sleep(0.05 / SIMULATION_SPEED)
        
        # 2. Position validation confirmation
        self.publish("sensor/position_confirmed", {
            "can_id": f"CAN{self.can_count:05d}",
            "timestamp": int(time.time() * 1000)
        })
        print("    Position valid, ready to fill")
        
        # 3. Filling phase
        print("Starting fill cycle...")
        self.fill_level = 0
        target_volume = 330  # ml
        fill_rate = random.uniform(180, 220)  # ml/second
        
        cycle_start = time.time()
        
        while self.fill_level < target_volume:
            time.sleep(0.1 / SIMULATION_SPEED)  # 100ms updates
            
            increment = fill_rate * 0.1  # ml in 100ms
            self.fill_level = min(self.fill_level + increment, target_volume)
            
            # Publish fill level
            fill_data = {
                "volume_ml": round(self.fill_level, 1),
                "timestamp": int(time.time() * 1000),
                "can_id": f"CAN{self.can_count:05d}"
            }
            self.publish("sensor/fill_level", fill_data)
            
            # Publish flow rate
            flow_data = {
                "ml_per_sec": round(fill_rate + random.uniform(-5, 5), 1),
                "timestamp": int(time.time() * 1000)
            }
            self.publish("sensor/flow_rate", flow_data)
            
            if int(self.fill_level) % 50 == 0 and self.fill_level > 0:
                print(f"   Fill level: {int(self.fill_level)} ml")
        
        cycle_time = time.time() - cycle_start
        print(f"    Fill complete: {round(self.fill_level, 1)} ml in {cycle_time:.2f}s")
        
        # 4. Sealing phase
        print(" Starting seal cycle...")
        time.sleep(0.2 / SIMULATION_SPEED)  # Seal positioning
        
        # Apply pressure
        pressure = random.uniform(2.3, 2.7)  # bar
        pressure_data = {
            "bar": round(pressure, 2),
            "timestamp": int(time.time() * 1000),
            "can_id": f"CAN{self.can_count:05d}"
        }
        self.publish("sensor/pressure", pressure_data)
        print(f"   Pressure applied: {pressure_data['bar']} bar")
        
        # Hold test (500ms)
        time.sleep(0.5 / SIMULATION_SPEED)
        
        # Verify seal (95% success rate)
        seal_ok = random.random() < 0.95
        
        final_pressure = pressure if seal_ok else pressure * 0.7
        pressure_data = {
            "bar": round(final_pressure, 2),
            "timestamp": int(time.time() * 1000),
            "can_id": f"CAN{self.can_count:05d}",
            "seal_integrity": seal_ok
        }
        self.publish("sensor/pressure", pressure_data)
        
        if seal_ok:
            print("    Seal integrity confirmed")
        else:
            print("     Seal failed - pressure drop detected")
        
        # 5. Quality result
        time.sleep(0.1 / SIMULATION_SPEED)
        
        quality = "PASS" if (325 <= self.fill_level <= 335 and seal_ok) else "FAIL"
        quality_data = {
            "can_id": f"CAN{self.can_count:05d}",
            "result": quality,
            "fill_level": round(self.fill_level, 1),
            "seal_ok": seal_ok,
            "timestamp": int(time.time() * 1000)
        }
        self.publish("status/quality", quality_data)
        
        total_time = (time.time() - cycle_start) * 1000
        
        if quality == "PASS":
            print(f"Quality check: PASS (cycle time: {total_time:.0f}ms)")
        else:
            print(f" Quality check: FAIL (cycle time: {total_time:.0f}ms)")
            
        return quality
        
    def simulate_fault(self):
        """Occasionally simulate a sensor fault (5% chance)"""
        if random.random() < 0.05:
            print("\n  FAULT: Sensor timeout detected")
            fault_data = {
                "fault_id": f"F{int(time.time())}",
                "sensor_id": random.choice(["position", "fill_level", "pressure"]),
                "fault_type": "TIMEOUT",
                "severity": "HIGH",
                "timestamp": int(time.time() * 1000)
            }
            self.publish("fault/detected", fault_data)
            time.sleep(2 / SIMULATION_SPEED)  # Recovery time
            
            # Clear fault
            self.publish("fault/cleared", {
                "fault_id": fault_data["fault_id"],
                "timestamp": int(time.time() * 1000)
            })
            print("    Fault cleared, resuming operation")
            
    def run(self):
        """Main simulation loop"""
        self.connect()
        
        print("\n" + "="*60)
        print(" CAN FILLING SYSTEM - SENSOR SIMULATOR")
        print("="*60)
        print(f"MQTT Broker: {MQTT_BROKER}:{MQTT_PORT}")
        print(f"Simulation Speed: {SIMULATION_SPEED}x")
        print(f"Target: 60-100 cans/minute")
        print("="*60 + "\n")
        
        passed = 0
        failed = 0
        
        try:
            while True:
                # Simulate one can
                result = self.simulate_can_cycle()
                
                if result == "PASS":
                    passed += 1
                else:
                    failed += 1
                
                # Occasional faults
                self.simulate_fault()
                
                # Print statistics every 10 cans
                if self.can_count % 10 == 0:
                    pass_rate = (passed / self.can_count) * 100
                    print(f"\n Statistics: {self.can_count} cans | "
                          f"{passed} passed | {failed} failed | "
                          f"Pass rate: {pass_rate:.1f}%\n")
                
                # Delay between cans (simulate ~80 cans/min = 0.75s/can)
                time.sleep(0.75 / SIMULATION_SPEED)
                
        except KeyboardInterrupt:
            print("\n\n Simulation stopped by user")
            print(f"Total cans processed: {self.can_count}")
            print(f"Passed: {passed} | Failed: {failed}")
            print(f"Pass rate: {(passed/self.can_count)*100:.1f}%")
            
        finally:
            self.client.loop_stop()
            self.client.disconnect()

if __name__ == "__main__":
    simulator = SensorSimulator()
    simulator.run()