# Kafka-Product-Service
Microservice of an E-mart named Zia mart

## Overview

The **Kafka Product Service** is a microservice architecture that leverages Apache Kafka for event streaming and PostgreSQL for persistent storage. The service includes:

- **Kafka Producer** with API Endpoints: Handles CRUD operations for products.
- **Kafka Consumer**: Processes Kafka messages and updates the PostgreSQL database.
- **PostgreSQL Database**: Stores product data.
- **Apache Kafka**: Message broker for event streaming.
- **Zookeeper**: Manages Kafka.

## Architecture

The architecture comprises the following Docker containers:
- **Producer**: A Flask-based API server that produces messages to Kafka.
- **Consumer**: A Kafka consumer that processes messages and updates the PostgreSQL database.
- **PostgreSQL**: A relational database for storing product information.
- **Kafka**: A message broker for handling the streams of events.
- **Zookeeper**: Manages Kafka brokers.


## Getting Started

### Prerequisites

- **Docker**: Ensure Docker is installed on your machine.
- **Docker Compose**: Ensure Docker Compose is installed.

### Setup

1. **Clone the Repository**:
   ```sh
   git clone https://github.com/yourusername/kafka-product-service.git
   cd kafka-product-service
   ```

2. **Build and Start the Services**:
   ```sh
   docker compose up -d
   ```

3. **Access the API**:
   - The API is accessible at `http://localhost:8000/docs`.
