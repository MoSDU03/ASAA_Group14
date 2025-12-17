#/usr/bin/env python3
"""
Performance Comparison Test Harness: MQTT vs HTTP
Measures message latency and throughput
"""

import time
import json
import statistics
import csv
from datetime import datetime
import paho.mqtt.client as mqtt
import requests

# Test configuration
MQTT_BROKER = "localhost"
MQTT_PORT = 1883
HTTP_SERVER = "http://localhost:5000"
MESSAGE_COUNT = 10000
WARMUP_COUNT = 100
TEST_RUNS = 10

class PerformanceTester:
    def __init__(self):
        self.results = {
            'mqtt': [],
            'http': []
        }
        
    def test_mqtt_latency(self, message_count=MESSAGE_COUNT):
        """Test MQTT message latency"""
        print(f"\n Testing MQTT latency ({message_count} messages)...")
        
        latencies = []
        received_count = [0]  # Use list for closure
        
        def on_message(client, userdata, msg):
            receive_time = time.time()
            data = json.loads(msg.payload.decode())
            send_time = data['timestamp']
            latency_ms = (receive_time - send_time) * 1000
            latencies.append(latency_ms)
            received_count[0] += 1
            
        # Setup MQTT client
        client = mqtt.Client("PerformanceTester")
        client.on_message = on_message
        client.connect(MQTT_BROKER, MQTT_PORT, 60)
        client.subscribe("test/response")
        client.loop_start()
        
        time.sleep(0.5)  # Wait for connection
        
        # Send messages
        start_time = time.time()
        for i in range(message_count):
            payload = {
                'id': i,
                'timestamp': time.time(),
                'data': 'x' * 100  # 100 bytes payload
            }
            client.publish("test/request", json.dumps(payload))
            time.sleep(0.001)  # Small delay to avoid overwhelming
            
        # Wait for all responses
        timeout = 10
        wait_start = time.time()
        while received_count[0] < message_count and (time.time() - wait_start) < timeout:
            time.sleep(0.01)
            
        total_time = time.time() - start_time
        
        client.loop_stop()
        client.disconnect()
        
        if len(latencies) < message_count:
            print(f"  Only received {len(latencies)}/{message_count} responses")
        
        return {
            'latencies': latencies,
            'mean': statistics.mean(latencies) if latencies else 0,
            'median': statistics.median(latencies) if latencies else 0,
            'stdev': statistics.stdev(latencies) if len(latencies) > 1 else 0,
            'min': min(latencies) if latencies else 0,
            'max': max(latencies) if latencies else 0,
            'throughput': len(latencies) / total_time if total_time > 0 else 0
        }
        
    def test_http_latency(self, message_count=MESSAGE_COUNT):
        """Test HTTP REST message latency"""
        print(f"\n Testing HTTP latency ({message_count} messages)...")
        
        latencies = []
        
        # Warm up
        try:
            requests.get(f"{HTTP_SERVER}/health", timeout=2)
        except:
            print(f"  HTTP server not responding at {HTTP_SERVER}")
            return None
        
        # Send messages
        start_time = time.time()
        for i in range(message_count):
            payload = {
                'id': i,
                'data': 'x' * 100  # 100 bytes payload
            }
            
            try:
                send_time = time.time()
                response = requests.post(
                    f"{HTTP_SERVER}/message",
                    json=payload,
                    timeout=2
                )
                receive_time = time.time()
                
                if response.status_code == 200:
                    latency_ms = (receive_time - send_time) * 1000
                    latencies.append(latency_ms)
                    
            except Exception as e:
                print(f" Request {i} failed: {e}")
                
        total_time = time.time() - start_time
        
        if not latencies:
            print(" No successful HTTP requests")
            return None
        
        return {
            'latencies': latencies,
            'mean': statistics.mean(latencies),
            'median': statistics.median(latencies),
            'stdev': statistics.stdev(latencies) if len(latencies) > 1 else 0,
            'min': min(latencies),
            'max': max(latencies),
            'throughput': len(latencies) / total_time
        }
        
    def run_experiment(self):
        """Run complete experiment with multiple runs"""
        print("="*70)
        print(" MQTT vs HTTP PERFORMANCE COMPARISON EXPERIMENT")
        print("="*70)
        print(f"Configuration:")
        print(f"  - Message count per run: {MESSAGE_COUNT}")
        print(f"  - Test runs: {TEST_RUNS}")
        print(f"  - Warmup messages: {WARMUP_COUNT}")
        print(f"  - Payload size: 100 bytes")
        print("="*70)
        
        # Warmup
        print("\n Warming up...")
        self.test_mqtt_latency(WARMUP_COUNT)
        
        mqtt_results = []
        http_results = []
        
        # Run tests
        for run in range(TEST_RUNS):
            print(f"\n Run {run + 1}/{TEST_RUNS}")
            
            # Test MQTT
            mqtt_result = self.test_mqtt_latency(MESSAGE_COUNT)
            if mqtt_result:
                mqtt_results.append(mqtt_result)
                print(f"   MQTT: {mqtt_result['mean']:.2f}ms mean, "
                      f"{mqtt_result['throughput']:.1f} msg/s")
            
            time.sleep(1)
            
            # Test HTTP
            http_result = self.test_http_latency(MESSAGE_COUNT)
            if http_result:
                http_results.append(http_result)
                print(f"   HTTP: {http_result['mean']:.2f}ms mean, "
                      f"{http_result['throughput']:.1f} msg/s")
            
            time.sleep(1)
        
        # Save raw data
        self.save_raw_data(mqtt_results, http_results)
        
        # Print summary
        self.print_summary(mqtt_results, http_results)
        
    def save_raw_data(self, mqtt_results, http_results):
        """Save all raw latency data to CSV"""
        filename = f"raw_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        
        with open(filename, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['run_id', 'protocol', 'latency_ms', 'timestamp'])
            
            for run_id, result in enumerate(mqtt_results):
                for latency in result['latencies']:
                    writer.writerow([run_id + 1, 'MQTT', f"{latency:.3f}", datetime.now().isoformat()])
                    
            for run_id, result in enumerate(http_results):
                for latency in result['latencies']:
                    writer.writerow([run_id + 1, 'HTTP', f"{latency:.3f}", datetime.now().isoformat()])
        
        print(f"\n Raw data saved to: {filename}")
        
    def print_summary(self, mqtt_results, http_results):
        """Print experiment summary statistics"""
        print("\n" + "="*70)
        print(" EXPERIMENT RESULTS SUMMARY")
        print("="*70)
        
        if mqtt_results:
            all_mqtt_latencies = []
            for r in mqtt_results:
                all_mqtt_latencies.extend(r['latencies'])
                
            mqtt_mean = statistics.mean(all_mqtt_latencies)
            mqtt_stdev = statistics.stdev(all_mqtt_latencies)
            mqtt_median = statistics.median(all_mqtt_latencies)
            
            print(f"\nMQTT Results (n={len(all_mqtt_latencies)}):")
            print(f"   Mean latency:    {mqtt_mean:.2f} ms")
            print(f"   Std deviation:   {mqtt_stdev:.2f} ms")
            print(f"   Median latency:  {mqtt_median:.2f} ms")
            print(f"   Min/Max:         {min(all_mqtt_latencies):.2f} / {max(all_mqtt_latencies):.2f} ms")
            
        if http_results:
            all_http_latencies = []
            for r in http_results:
                all_http_latencies.extend(r['latencies'])
                
            http_mean = statistics.mean(all_http_latencies)
            http_stdev = statistics.stdev(all_http_latencies)
            http_median = statistics.median(all_http_latencies)
            
            print(f"\nHTTP Results (n={len(all_http_latencies)}):")
            print(f"   Mean latency:    {http_mean:.2f} ms")
            print(f"   Std deviation:   {http_stdev:.2f} ms")
            print(f"   Median latency:  {http_median:.2f} ms")
            print(f"   Min/Max:         {min(all_http_latencies):.2f} / {max(all_http_latencies):.2f} ms")
            
        if mqtt_results and http_results:
            speedup = http_mean / mqtt_mean
            improvement_pct = ((http_mean - mqtt_mean) / http_mean) * 100
            
            print(f"\n Comparison:")
            print(f"   MQTT is {speedup:.2f}x faster than HTTP")
            print(f"   HTTP latency is {improvement_pct:.1f}% higher")
            print(f"   Absolute difference: {http_mean - mqtt_mean:.2f} ms")
            
        print("\n" + "="*70)

if __name__ == "__main__":
    tester = PerformanceTester()
    tester.run_experiment()
