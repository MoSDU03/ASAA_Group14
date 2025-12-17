#/usr/bin/env python3
"""
Simple HTTP REST server for performance comparison
"""

from flask import Flask, request, jsonify
import time

app = Flask(__name__)

message_count = 0

@app.route('/health', methods=['GET'])
def health():
    """Health check endpoint"""
    return jsonify({"status": "ok"})

@app.route('/message', methods=['POST'])
def handle_message():
    """Handle incoming message - echo back"""
    global message_count
    
    data = request.json
    message_count += 1
    
    # Simulate minimal processing
    response = {
        "received_id": data.get('id'),
        "message_count": message_count,
        "timestamp": time.time()
    }
    
    return jsonify(response)

@app.route('/stats', methods=['GET'])
def stats():
    """Get server statistics"""
    return jsonify({
        "total_messages": message_count
    })

if __name__ == "__main__":
    print(" Starting HTTP server on port 5000...")
    app.run(host='0.0.0.0', port=5000, debug=False)
