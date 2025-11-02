import asyncio
import websockets
import json

async def test_sensor_stream():
    uri = "ws://localhost:8001/ws/sensor-stream"
    print(f"Connecting to {uri}...")
    
    async with websockets.connect(uri) as websocket:
        print("Connected! Receiving messages...")
        
        # Receive 10 messages then disconnect
        for i in range(10):
            message = await websocket.recv()
            data = json.loads(message)
            print(f"\nMessage {i+1}:")
            print(json.dumps(data, indent=2))

if __name__ == "__main__":
    asyncio.run(test_sensor_stream())