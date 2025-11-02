"""
WebSocket Consumer - Connects to the simulator and forwards data to the ingestion API
"""

import asyncio
import websockets
import json
import logging
import requests
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuration
SIMULATOR_WS_URL = "ws://localhost:8001/ws/sensor-stream"
INGESTION_API_URL = "http://localhost:8000/ingest"


async def consume_sensor_data():
    """
    Connect to WebSocket simulator and consume real-time sensor data
    Forwards each message to the ingestion API
    """
    retry_count = 0
    max_retries = 5
    
    while retry_count < max_retries:
        try:
            logger.info(f"Connecting to sensor simulator at {SIMULATOR_WS_URL}...")
            
            async with websockets.connect(SIMULATOR_WS_URL) as websocket:
                logger.info("✅ Connected to sensor stream!")
                retry_count = 0  # Reset on successful connection
                
                message_count = 0
                error_count = 0
                
                while True:
                    try:
                        # Receive message from simulator
                        message = await websocket.recv()
                        data = json.loads(message)
                        message_count += 1
                        
                        logger.info(f"📊 Received message {message_count} from sensor {data.get('capteur_id')}")
                        
                        # Forward to ingestion API
                        try:
                            response = requests.post(
                                INGESTION_API_URL,
                                json=data,
                                timeout=5
                            )
                            
                            if response.status_code == 200:
                                logger.info(f"✅ Message {message_count} successfully sent to ingestion API")
                            else:
                                logger.error(f"❌ Failed to send message {message_count}: {response.status_code}")
                                error_count += 1
                                
                        except requests.exceptions.RequestException as e:
                            logger.error(f"❌ Error sending to ingestion API: {e}")
                            error_count += 1
                        
                        # Log statistics periodically
                        if message_count % 10 == 0:
                            success_rate = ((message_count - error_count) / message_count) * 100
                            logger.info(f"📈 Stats - Total: {message_count}, Errors: {error_count}, Success Rate: {success_rate:.1f}%")
                    
                    except json.JSONDecodeError as e:
                        logger.error(f"❌ Invalid JSON received: {e}")
                        error_count += 1
                    
                    except Exception as e:
                        logger.error(f"❌ Error processing message: {e}")
                        error_count += 1
        
        except websockets.exceptions.WebSocketException as e:
            retry_count += 1
            logger.error(f"❌ WebSocket error: {e}")
            logger.info(f"Retry {retry_count}/{max_retries} in 5 seconds...")
            await asyncio.sleep(5)
        
        except KeyboardInterrupt:
            logger.info("⚠️ Shutting down consumer...")
            break
        
        except Exception as e:
            retry_count += 1
            logger.error(f"❌ Unexpected error: {e}")
            logger.info(f"Retry {retry_count}/{max_retries} in 5 seconds...")
            await asyncio.sleep(5)
    
    logger.error(f"❌ Max retries ({max_retries}) reached. Exiting.")


async def main():
    """Main entry point"""
    logger.info("🚀 Starting WebSocket Consumer for AgroTrace")
    logger.info(f"Simulator: {SIMULATOR_WS_URL}")
    logger.info(f"Ingestion API: {INGESTION_API_URL}")
    logger.info("Press Ctrl+C to stop")
    logger.info("-" * 50)
    
    await consume_sensor_data()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("👋 Consumer stopped by user")
