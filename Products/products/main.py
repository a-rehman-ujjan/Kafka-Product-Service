from typing import Annotated
from fastapi import Depends, FastAPI, BackgroundTasks, HTTPException
from contextlib import asynccontextmanager
from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
from aiokafka.admin import AIOKafkaAdminClient, NewTopic
from products import settings
from sqlmodel import SQLModel, Field, create_engine, Session, select
from products import product_pb2
from typing import Annotated, List, Optional
from sqlalchemy import create_engine

connection_string=str(settings.DATABASE_URL)
engine = create_engine(connection_string, echo=True, pool_pre_ping=True)

class Products (SQLModel, table=True):
    id:Optional[int] = Field(default= None, primary_key= True)
    name: str = Field()
    price:float = Field()

# Dependency to get the session
def get_session():
    with Session(engine) as session:
        yield session

async def create_topic():
    admin_client = AIOKafkaAdminClient(
        bootstrap_servers=settings.BOOTSTRAP_SERVER)
    await admin_client.start()
    topic_list = [NewTopic(name=settings.KAFKA_ORDER_TOPIC,
                           num_partitions=2, replication_factor=1)]
    try:
        await admin_client.create_topics(new_topics=topic_list, validate_only=False)
        print(f"Topic '{settings.KAFKA_ORDER_TOPIC}' created successfully")
    except Exception as e:
        print(f"Failed to create topic '{settings.KAFKA_ORDER_TOPIC}': {e}")
    finally:
        await admin_client.close()

@asynccontextmanager
async def lifespan(app: FastAPI):
    print("Fastapi app started...")
    await create_topic()
    yield

app = FastAPI(lifespan=lifespan,
              title="FastAPI Producer Service...",
              version='1.0.0'
              )

@app.get('/')
async def root():
    return {"message": "Welcome to the Zia Mart"}

async def kafka_producer():
    producer = AIOKafkaProducer(bootstrap_servers=settings.BOOTSTRAP_SERVER)
    await producer.start()
    try:
        yield producer
    finally:
        await producer.stop()

@app.post('/create_product', response_model=Products)
async def Create_product(
    New_product:Products,
    producer: Annotated[AIOKafkaProducer, Depends(kafka_producer)]
):
    pb_product = product_pb2.product()
    pb_product.id=New_product.id
    pb_product.name=New_product.name
    pb_product.price=New_product.price
    serialized_product = pb_product.SerializeToString()
    await producer.send_and_wait(settings.KAFKA_ORDER_TOPIC, serialized_product)

    return New_product
    
# GET endpoint to fetch all products
@app.get('/products', response_model=List[Products])
async def get_products(session: Annotated[Session, Depends(get_session)]):
    products = session.exec(select(Products)).all()
    return products

# GET endpoint to fetch a single product by ID
@app.get('/products/{product_id}', response_model=Products)
async def get_product(product_id: int, session: Annotated[Session, Depends(get_session)]):
    product = session.get(Products, product_id)
    if product is None:
        raise HTTPException(status_code=404, detail="Product not found")
    return product

# PUT endpoint to update a product
@app.put('/products/{product_id}', response_model=Products)
async def update_product(
    product_id: int,
    updated_product: Products,
    session: Annotated[Session, Depends(get_session)],
    producer: Annotated[AIOKafkaProducer, Depends(kafka_producer)]
):
    product = session.get(Products, product_id)
    if product is None:
        raise HTTPException(status_code=404, detail="Product not found")
    
    product.name = updated_product.name
    product.price = updated_product.price
    session.commit()
    session.refresh(product)
    
    # Serialize and send updated product to Kafka
    pb_product = product_pb2.product()
    pb_product.id = product.id
    pb_product.name = product.name
    pb_product.price = product.price
    serialized_product = pb_product.SerializeToString()
    await producer.send_and_wait(settings.KAFKA_ORDER_TOPIC, serialized_product)
    
    return product

# DELETE endpoint to delete a product by ID
@app.delete('/products/{product_id}', response_model=dict)
async def delete_product(
    product_id: int,
    session: Annotated[Session, Depends(get_session)],
    producer: Annotated[AIOKafkaProducer, Depends(kafka_producer)]
):
    product = session.get(Products, product_id)
    if product is None:
        raise HTTPException(status_code=404, detail="Product not found")
    
    session.delete(product)
    session.commit()
    
    # Serialize and send delete info to Kafka
    pb_product = product_pb2.product()
    pb_product.id = product.id
    pb_product.name = product.name
    pb_product.price = product.price
    serialized_product = pb_product.SerializeToString()
    await producer.send_and_wait(settings.KAFKA_ORDER_TOPIC, serialized_product)
    
    return {"message": "Product deleted successfully"}