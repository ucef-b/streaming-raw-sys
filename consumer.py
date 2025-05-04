
import asyncio
import uvicorn

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from aiokafka import AIOKafkaConsumer
import json
import logging
from contextlib import asynccontextmanager
from typing import Set


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP_SERVERS = "localhost:29092"
KAFKA_TOPIC = "uav.tiles.tiff"
KAFKA_CONSUMER_GROUP = "image_display_group_v2" 

clients: Set[WebSocket] = set()

async def consume_messages():
    consumer = AIOKafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=KAFKA_CONSUMER_GROUP,
        auto_offset_reset='earliest',
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        fetch_max_bytes=52428800,
        max_partition_fetch_bytes=52428800,  
    )
    await consumer.start()
    logger.info("Kafka Consumer started.")
    try:
        async for msg in consumer:
            logger.info(f"Received message from Kafka partition {msg.partition} offset {msg.offset}")
            try:
                data = msg.value
                image_id = data.get('image_id', 'unknown')
                metadata = data.get('metadata', {})
                
                image_data_uri = data.get('image_data')

                if not image_data_uri:
                    logger.warning(f"No 'image_data' (data URI) found for image_id: {image_id}")
                    continue

                
                
                message_to_send = json.dumps({
                    "imageId": image_id,
                    "metadata": metadata,
                    "imageData": image_data_uri
                })

                
                results = await asyncio.gather(
                    *[client.send_text(message_to_send) for client in clients],
                    return_exceptions=True
                )

                disconnected_clients = set()
                for i, result in enumerate(results):
                     
                     current_clients = list(clients)
                     if i < len(current_clients):
                        client = current_clients[i]
                        if isinstance(result, Exception):
                            logger.warning(f"Failed to send message to client {client}: {result}. Removing client.")
                            disconnected_clients.add(client)
                     else:
                        logger.warning("Client list changed during broadcast, potential missed disconnect handling.")


                clients.difference_update(disconnected_clients)

            except json.JSONDecodeError as e:
                logger.error(f"Failed to decode Kafka message value: {e}")
            except Exception as e:
                logger.error(f"Error processing Kafka message: {e}", exc_info=True)

    except asyncio.CancelledError:
        logger.info("Kafka consumption task cancelled.")
    except Exception as e:
        logger.error(f"Kafka consumer error: {e}", exc_info=True)
    finally:
        logger.info("Stopping Kafka consumer...")
        await consumer.stop()
        logger.info("Kafka Consumer stopped.")


kafka_task = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    global kafka_task
    logger.info("Starting up application...")
    kafka_task = asyncio.create_task(consume_messages())
    yield
    logger.info("Shutting down application...")
    if kafka_task:
        kafka_task.cancel()
        try:
            await kafka_task
        except asyncio.CancelledError:
            logger.info("Kafka task successfully cancelled.")
    logger.info(f"Closing {len(clients)} WebSocket connections...")
    await asyncio.gather(
        *[client.close(code=1000, reason="Server shutting down") for client in clients],
        return_exceptions=True
    )
    clients.clear()
    logger.info("WebSocket connections closed.")

app = FastAPI(lifespan=lifespan)

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    clients.add(websocket)
    logger.info(f"Client connected: {websocket.client}. Total clients: {len(clients)}")
    try:
        while True:
            await websocket.receive_text() 
    except WebSocketDisconnect:
        logger.info(f"Client disconnected: {websocket.client}")
    except Exception as e:
        logger.error(f"WebSocket error for {websocket.client}: {e}", exc_info=True)
    finally:
        clients.discard(websocket)
        logger.info(f"Client removed: {websocket.client}. Total clients: {len(clients)}")

@app.get("/")
async def read_root():
    return {"message": "Image Streaming Service v2 is running. Connect via WebSocket at /ws"}

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000, log_level="info")