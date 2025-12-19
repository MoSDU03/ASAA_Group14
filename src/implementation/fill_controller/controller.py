import paho.mqtt.client as mqtt
import psycopg2
from psycopg2.extras import RealDictCursor
import json
import time
from datetime import datetime
from enum import Enum

class SystemState(Enum):
    IDLE = "idle"
    WAITING_POSITION = "waiting_position"
    FILLING = "filling"
    CLOSING_VALVE = "closing_valve"
    COMPLETE = "complete"
    FAULT = "fault"

class FillController:
    def __init__(self, broker='localhost', port=1883, db_config=None):
        self.broker = broker
        self.port = port
        self.db_config = db_config
        
        self.client = mqtt.Client(client_id="fill_controller")
        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message
        
        self.state = SystemState.IDLE
        self.current_can_id = None
        self.position_mm = None
        self.current_level = 0
        self.target_level = 330
        self.tolerance = 5
        
        self.cycle_start_time = None
        self.fill_start_time = None
        
        self.position_timeout = 0.2
        self.sensor_timeout = 0.2
        self.max_fill_time = 3.0
        
        self.db_conn = None
        
    def connect_database(self):
        try:
            self.db_conn = psycopg2.connect(**self.db_config)
            print(f"[{datetime.now()}] Connected to database")
        except Exception as e:
            print(f"Database connection error: {e}")
            
    def log_event(self, event_type, **kwargs):
        if not self.db_conn:
            return
            
        try:
            cursor = self.db_conn.cursor()
            
            query = """
                INSERT INTO filling_events 
                (event_type, can_id, position_mm, fill_level_ml, cycle_time_ms, 
                 fill_duration_ms, valve_state, sensor_status, fault_code, 
                 fault_description, system_state)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            
            values = (
                event_type,
                kwargs.get('can_id', self.current_can_id),
                kwargs.get('position_mm', self.position_mm),
                kwargs.get('fill_level', self.current_level),
                kwargs.get('cycle_time_ms'),
                kwargs.get('fill_duration_ms'),
                kwargs.get('valve_state'),
                kwargs.get('sensor_status'),
                kwargs.get('fault_code'),
                kwargs.get('fault_description'),
                self.state.value
            )
            
            cursor.execute(query, values)
            self.db_conn.commit()
            cursor.close()
            
        except Exception as e:
            print(f"Error logging event: {e}")
            if self.db_conn:
                self.db_conn.rollback()
    
    def on_connect(self, client, userdata, flags, rc):
        print(f"[{datetime.now()}] Fill Controller connected to MQTT broker (rc={rc})")
        client.subscribe("sensor/can_detected")
        client.subscribe("sensor/position")
        client.subscribe("sensor/level")
        
    def on_message(self, client, userdata, msg):
        try:
            payload = json.loads(msg.payload.decode())
            
            if msg.topic == "sensor/can_detected":
                self.handle_can_detected(payload)
            elif msg.topic == "sensor/position":
                self.handle_position_data(payload)
            elif msg.topic == "sensor/level":
                self.handle_level_data(payload)
                
        except Exception as e:
            print(f"Error processing message: {e}")
    
    def handle_can_detected(self, data):
        if self.state == SystemState.IDLE:
            self.current_can_id = data.get('can_id')
            self.cycle_start_time = time.time()
            self.state = SystemState.WAITING_POSITION
            print(f"[{datetime.now()}] State: IDLE -> WAITING_POSITION (Can #{self.current_can_id})")
            
            self.log_event('can_detected', can_id=self.current_can_id)
            
    def handle_position_data(self, data):
        if self.state == SystemState.WAITING_POSITION:
            self.position_mm = data.get('position_mm')
            is_valid = data.get('valid', False)
            
            elapsed = time.time() - self.cycle_start_time
            
            if elapsed > self.position_timeout:
                self.enter_fault_state('position_timeout', 
                    f"Position detection timeout ({elapsed*1000:.0f}ms > {self.position_timeout*1000}ms)")
                return
                
            if not is_valid:
                self.enter_fault_state('invalid_position', 
                    f"Position out of tolerance: {self.position_mm:.2f}mm")
                return
            
            self.state = SystemState.FILLING
            self.fill_start_time = time.time()
            print(f"[{datetime.now()}] State: WAITING_POSITION -> FILLING")
            print(f"[{datetime.now()}] Position validated: {self.position_mm:.2f}mm")
            
            valve_cmd = {
                "timestamp": datetime.now().isoformat(),
                "action": "open",
                "can_id": self.current_can_id
            }
            self.client.publish("valve/command", json.dumps(valve_cmd), qos=1)
            
            self.log_event('fill_start', position_mm=self.position_mm, valve_state='opening')
            
    def handle_level_data(self, data):
        if self.state == SystemState.FILLING:
            self.current_level = data.get('level_ml', 0)
            
            elapsed = time.time() - self.fill_start_time
            
            if elapsed > self.max_fill_time:
                self.enter_fault_state('fill_timeout', 
                    f"Fill timeout ({elapsed*1000:.0f}ms > {self.max_fill_time*1000}ms)")
                return
            
            if self.current_level >= (self.target_level - self.tolerance):
                self.state = SystemState.CLOSING_VALVE
                print(f"[{datetime.now()}] State: FILLING -> CLOSING_VALVE")
                print(f"[{datetime.now()}] Target reached: {self.current_level:.1f}ml")
                
                valve_cmd = {
                    "timestamp": datetime.now().isoformat(),
                    "action": "close",
                    "can_id": self.current_can_id,
                    "final_level": self.current_level
                }
                self.client.publish("valve/command", json.dumps(valve_cmd), qos=1)
                
                time.sleep(0.03)
                
                self.verify_completion()
    
    def verify_completion(self):
        if self.state == SystemState.CLOSING_VALVE:
            within_tolerance = (self.target_level - self.tolerance <= self.current_level <= 
                               self.target_level + self.tolerance)
            
            cycle_time = (time.time() - self.cycle_start_time) * 1000
            fill_time = (time.time() - self.fill_start_time) * 1000
            
            if within_tolerance:
                self.state = SystemState.COMPLETE
                print(f"[{datetime.now()}] State: CLOSING_VALVE -> COMPLETE")
                print(f"[{datetime.now()}] SUCCESS: Can #{self.current_can_id}")
                print(f"  Final level: {self.current_level:.1f}ml (target: {self.target_level}±{self.tolerance}ml)")
                print(f"  Cycle time: {cycle_time:.0f}ms")
                print(f"  Fill time: {fill_time:.0f}ms")
                
                self.log_event('fill_complete', 
                    fill_level=self.current_level,
                    cycle_time_ms=int(cycle_time),
                    fill_duration_ms=int(fill_time),
                    valve_state='closed',
                    sensor_status='normal')
                
                time.sleep(0.5)
                self.reset_for_next_can()
            else:
                self.enter_fault_state('out_of_tolerance', 
                    f"Final level {self.current_level:.1f}ml outside tolerance ({self.target_level}±{self.tolerance}ml)")
    
    def enter_fault_state(self, fault_code, description):
        self.state = SystemState.FAULT
        print(f"[{datetime.now()}] State: -> FAULT")
        print(f"  Fault code: {fault_code}")
        print(f"  Description: {description}")
        
        valve_cmd = {
            "timestamp": datetime.now().isoformat(),
            "action": "close",
            "reason": "fault",
            "can_id": self.current_can_id
        }
        self.client.publish("valve/command", json.dumps(valve_cmd), qos=1)
        
        self.log_event('fault_detected', 
            fault_code=fault_code,
            fault_description=description,
            valve_state='emergency_close')
        
        time.sleep(2)
        self.reset_for_next_can()
    
    def reset_for_next_can(self):
        self.state = SystemState.IDLE
        self.current_can_id = None
        self.position_mm = None
        self.current_level = 0
        self.cycle_start_time = None
        self.fill_start_time = None
        print(f"[{datetime.now()}] State: -> IDLE (ready for next can)")
        print("=" * 70)
    
    def run(self):
        self.connect_database()
        
        self.client.connect(self.broker, self.port, 60)
        self.client.loop_start()
        
        print(f"[{datetime.now()}] Fill Controller started")
        print(f"Configuration:")
        print(f"  Target level: {self.target_level}±{self.tolerance}ml")
        print(f"  Max fill time: {self.max_fill_time}s")
        print(f"  Position timeout: {self.position_timeout}s")
        print("=" * 70)
        
        try:
            while True:
                time.sleep(0.1)
                
        except KeyboardInterrupt:
            print("Shutting down fill controller...")
            if self.db_conn:
                self.db_conn.close()
            self.client.loop_stop()
            self.client.disconnect()

if __name__ == "__main__":
    import os
    
    broker = os.getenv('MQTT_BROKER', 'localhost')
    port = int(os.getenv('MQTT_PORT', 1883))
    
    db_config = {
        'host': os.getenv('DB_HOST', 'localhost'),
        'port': int(os.getenv('DB_PORT', 5432)),
        'database': os.getenv('DB_NAME', 'filling_db'),
        'user': os.getenv('DB_USER', 'filling_user'),
        'password': os.getenv('DB_PASSWORD', 'filling_pass')
    }
    
    time.sleep(10)
    
    controller = FillController(broker=broker, port=port, db_config=db_config)
    controller.run()
