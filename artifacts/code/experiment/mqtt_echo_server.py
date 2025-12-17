#/usr/bin/env python3
"""
Simple MQTT echo server for performance comparison
Subscribes to test/request and echoes to test/response
"""

import paho.mqtt.client as mqtt
import json

MQTT_BROKER = "localhost"
MQTT_PORT = 1883

message_count = 0

def on_connect(client, userdata, flags, rc):
    print(f" Connected to MQTT broker")
    client.subscribe("test/request")
    print(" Subscribed to test/request")

def on_message(client, userdata, msg):
    """Echo message back"""
    global message_count
    
    try:
        # Receive message
        data = json.loads(msg.payload.decode())
        message_count += 1
        
        # Echo back immediately
        client.publish("test/response", msg.payload)
        
        if message_count % 100 == 0:
            print(f"Processed {message_count} messages")
            
    except Exception as e:
        print(f"Error: {e}")

def main():
    print(" Starting MQTT echo server...")
    
    client = mqtt.Client("MQTTEchoServer")
    client.on_connect = on_connect
    client.on_message = on_message
    
    client.connect(MQTT_BROKER, MQTT_PORT, 60)
    
    print(f" MQTT echo server ready at {MQTT_BROKER}:{MQTT_PORT}")
    print("  Listening on: test/request")
    print("  Publishing to: test/response\n")
    
    try:
        client.loop_forever()
    except KeyboardInterrupt:
        print(f"\n\n Server stopped")
        print(f"Total messages processed: {message_count}")
        client.disconnect()

if __name__ == "__main__":
    main()