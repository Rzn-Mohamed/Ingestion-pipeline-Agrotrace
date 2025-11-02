"""
WebSocket Sensor Simulator for AgroTrace
Generates real-time sensor data with realistic issues to test the ingestion pipeline
"""

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from datetime import datetime
import asyncio
import random
import json
import uvicorn
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="AgroTrace Sensor Simulator")

# Sensor configuration
SENSOR_IDS = ["TEMP001", "HUM001", "SOIL001", "PH001", "LIGHT001"]
SEND_INTERVAL_MIN = 2  # Minimum seconds between messages
SEND_INTERVAL_MAX = 5  # Maximum seconds between messages
MESSY_DATA_PROBABILITY = 0.3  # 30% chance of data issues


def generate_sensor_data(include_issues: bool = True) -> dict:
    """
    Generate sensor data matching the CapteurData model
    
    Args:
        include_issues: If True, randomly inject data quality issues
    
    Returns:
        Dictionary with sensor data
    """
    # Generate clean base data
    base_data = {
        "capteur_id": random.choice(SENSOR_IDS),
        "timestamp": datetime.utcnow().isoformat(),
        "temperature": round(random.uniform(15.0, 35.0), 2),
        "humidite": round(random.uniform(30.0, 90.0), 2),
        "humidite_sol": round(random.uniform(20.0, 80.0), 2),
        "niveau_ph": round(random.uniform(5.5, 8.0), 2),
        "luminosite": round(random.uniform(0.0, 100000.0), 2)
    }
    
    # Inject random issues to simulate real-world problems
    if include_issues and random.random() < MESSY_DATA_PROBABILITY:
        issue_type = random.choice([
            "missing_field",
            "null_value",
            "out_of_range",
            "wrong_type",
            "malformed_timestamp",
            "negative_value",
            "extreme_value"
        ])
        
        logger.warning(f"Injecting issue: {issue_type}")
        
        if issue_type == "missing_field":
            # Remove a random optional field
            field_to_remove = random.choice([
                "temperature", "humidite", "humidite_sol", 
                "niveau_ph", "luminosite"
            ])
            del base_data[field_to_remove]
            
        elif issue_type == "null_value":
            # Set a random field to None
            field = random.choice([
                "temperature", "humidite", "humidite_sol", 
                "niveau_ph", "luminosite"
            ])
            base_data[field] = None
            
        elif issue_type == "out_of_range":
            # Values that are technically valid but unrealistic
            choice = random.choice([
                ("temperature", 150.0),
                ("humidite", 150.0),
                ("niveau_ph", 15.0),
                ("humidite_sol", -50.0)
            ])
            base_data[choice[0]] = choice[1]
            
        elif issue_type == "wrong_type":
            # Send string instead of number
            field = random.choice([
                "temperature", "humidite", "luminosite"
            ])
            base_data[field] = "invalid_string_value"
            
        elif issue_type == "malformed_timestamp":
            # Invalid timestamp format
            base_data["timestamp"] = "2024-13-45T99:99:99"
            
        elif issue_type == "negative_value":
            # Negative value where it shouldn't be
            field = random.choice(["luminosite", "humidite_sol"])
            base_data[field] = -random.uniform(1, 100)
            
        elif issue_type == "extreme_value":
            # Sensor malfunction - extreme values
            base_data["temperature"] = random.choice([-999.9, 9999.9])
    
    return base_data


@app.get("/")
async def root():
    """Root endpoint with simulator info"""
    return {
        "name": "AgroTrace Sensor Simulator",
        "status": "running",
        "websocket_endpoint": "/ws/sensor-stream",
        "sensors": SENSOR_IDS,
        "messy_data_rate": f"{MESSY_DATA_PROBABILITY * 100}%"
    }


@app.websocket("/ws/sensor-stream")
async def sensor_stream(websocket: WebSocket):
    """
    WebSocket endpoint that streams sensor data in real-time
    
    Client connects to: ws://localhost:8001/ws/sensor-stream
    """
    await websocket.accept()
    client_id = id(websocket)
    logger.info(f"Client {client_id} connected to sensor stream")
    
    try:
        message_count = 0
        while True:
            # Generate sensor data
            data = generate_sensor_data(include_issues=True)
            message_count += 1
            
            # Send to client
            await websocket.send_json(data)
            logger.info(f"Sent message {message_count} to client {client_id}: {data['capteur_id']}")
            
            # Wait random interval to simulate real sensor behavior
            interval = random.uniform(SEND_INTERVAL_MIN, SEND_INTERVAL_MAX)
            await asyncio.sleep(interval)
            
    except WebSocketDisconnect:
        logger.info(f"Client {client_id} disconnected (sent {message_count} messages)")
    except Exception as e:
        logger.error(f"Error in sensor stream for client {client_id}: {e}")


@app.get("/test/single")
async def test_single_message():
    """Test endpoint to get a single sensor reading"""
    return generate_sensor_data(include_issues=False)


@app.get("/test/messy")
async def test_messy_message():
    """Test endpoint to get a single messy sensor reading"""
    return generate_sensor_data(include_issues=True)


if __name__ == "__main__":
    logger.info("Starting AgroTrace Sensor Simulator...")
    logger.info(f"Simulating sensors: {SENSOR_IDS}")
    logger.info(f"Messy data rate: {MESSY_DATA_PROBABILITY * 100}%")
    logger.info(f"Send interval: {SEND_INTERVAL_MIN}-{SEND_INTERVAL_MAX} seconds")
    
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=8001,
        log_level="info"
    )
