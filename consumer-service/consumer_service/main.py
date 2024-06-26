# main.py
from contextlib import asynccontextmanager
from fastapi import FastAPI, Depends
from typing import AsyncGenerator
from aiokafka import AIOKafkaConsumer
from sqlmodel import SQLModel, Field, create_engine, Session, select
from consumer_service import settings, product_pb2
import asyncio
from typing import Annotated, List, Optional
import logging

class Products(SQLModel, table=True):
    id:Optional[int] = Field(default= None, primary_key= True)
    name: str = Field()
    price:float = Field()

connection_string=str(settings.DATABASE_URL)
engine = create_engine(connection_string, pool_recycle=300, echo=True)

def create_tables():
    SQLModel.metadata.create_all(engine)

async def consume_messages():
    # Create a consumer instance.
    consumer = AIOKafkaConsumer(
        settings.KAFKA_ORDER_TOPIC,
        bootstrap_servers=settings.BOOTSTRAP_SERVER,
        group_id=settings.KAFKA_CONSUMER_GROUP_ID,
        auto_offset_reset='earliest',
        # session_timeout_ms=30000,  # Example: Increase to 30 seconds
        # max_poll_records=10,
    )
    # Start the consumer.
    await consumer.start()
    print("consumer started")
    try:
        # Continuously listen for messages.
        async for msg in consumer:
            if msg is not None:
                try:
                    product = product_pb2.product()
                    product.ParseFromString(msg.value)            
                    data_from_producer=Products(id=product.id, name= product.name, price=product.price)
                    logging.info(f"Received message from Kafka: {msg.value}")  
                    print(f"Received message from Kafka: {msg.value}")
                    # Here you can add code to process each message.
                    with Session(engine) as session:
                        session.add(data_from_producer)
                        session.commit()
                        session.refresh(data_from_producer)
                        print(f'''Stored Protobuf data in database with ID: {data_from_producer.id}''')
                except Exception as e:  # Catch deserialization errors
                    print(f"Error processing message: {e}")
            else:
                print("No message received from Kafka.")
    finally:
        # Ensure to close the consumer when done.
        await consumer.stop()

@asynccontextmanager
async def lifespan(app: FastAPI):
    print('Creating Tables')
    create_tables()
    print("Tables Created")
    loop = asyncio.get_event_loop()
    task = loop.create_task(consume_messages())
    yield
    task.cancel()
    await task


app = FastAPI(lifespan=lifespan, title="Kafka consumer with database", 
    version="0.0.01",)

# Kafka Producer as a dependency
# async def get_kafka_producer():
#     producer = AIOKafkaProducer(bootstrap_servers='broker:19092')
#     await producer.start()
#     try:
#         yield producer
#     finally:
#         await producer.stop()