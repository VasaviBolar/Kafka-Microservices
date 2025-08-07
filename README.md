# Kafka Microservice with FastAPI

This project demonstrates a simple microservices architecture using **Apache Kafka** as a message broker and **FastAPI** for building the producer and consumer services.

KafkaTest/
├── kafka_lib/                        # Shared Kafka logic
│   ├── __init__.py                   # Init file
│   ├── admin.py                      # Kafka topic management (creation/config)
│   ├── consumer.py                   # aiokafka-based consumer logic
│   ├── producer.py                   # Kafka producer logic
│   └── schemas.py                    # Pydantic data models for messages
│
├── producer_service/                # Sends messages to Kafka
│   ├── main.py                       # FastAPI app for producing messages
│   └── Dockerfile                    # Docker setup for producer
│
├── consumer_service/                # Consumes messages from Kafka
│   ├── main.py                       # FastAPI app for consuming messages
│   └── Dockerfile                    # Docker setup for consumer
│
├── input_api_service/              # External API gateway
│   ├── main.py                       # FastAPI app integrating producer/consumer
│   └── Dockerfile                    # Docker setup for input API
│
├── Docker-Kafka/                   # Kafka infrastructure
│   └── docker-compose-kafka/        # Standalone Kafka-Zookeeper setup
│
├── docker-compose.yml              # Orchestrates all services + Kafka
├── requirements.txt                # All Python dependencies
├── restart.bat                     # Batch file for restarting services
├── .gitignore                      # Ignore unnecessary files/folders
└── README.md                       # You're reading it!


markdown
Copy
Edit

## 🚀 Features

1. Microservice Architecture:
   *The system is split into modular, independent services:producer_service, consumer_service, and input_api_service, each running as a separate FastAPI app.
   *Promotes scalability, maintainability, and separation of concerns.
2. Apache Kafka Integration:
   *Kafka is used as a message broker to decouple communication between services.
   *Ensures asynchronous, fault-tolerant, and real-time data streaming between services.
3. Producer Service:
   *Accepts HTTP POST requests and sends structured messages (query, prompt, answer) to Kafka topics.
   *Uses Kafka Admin client to create topics dynamically at startup
4. Consumer Service
   *Consumes Kafka messages using aiokafka (asynchronous).
   *Provides REST endpoints (/messages) to fetch the next unconsumed message in FIFO order based on committed offsets.
   *Includes profanity filtering for query messages using
5. Input API Gateway
   *Acts as a WebSocket and HTTP interface for external clients.
   *Routes queries to the producer, fetches responses from the consumer, and handles real-time chat functionality.
   *Integrates RAG-based (Retrieval Augmented Generation) pipeline with Redis-backed session management.
6. WebSocket Support
   *Allows real-time, bidirectional communication for interactive chat applications.
   *Ensures low latency in delivering responses via Kafka.
7. Kafka Offset Management
   *Strict FIFO processing: each /messages?type=... call fetches the next unconsumed message using committed offsets.
   *Avoids reprocessing or skipping messages, ensuring consistent message flow.
8. Dynamic Topic Handling
   *Automatically creates Kafka topics (query, prompt, answer) with specific configurations like 2-minute retention time.
9. Dockerized Deployment
   *Uses docker-compose.yml to orchestrate Kafka, Zookeeper, and all services.
   *Ensures consistent, containerized environments for development and deployment
10. Clean Codebase with Modular Kafka Library
    * All Kafka logic is abstracted into kafka_lib/ containing:
          *producer.py: Kafka producer logic.
          *consumer.py: Asynchronous consumer with topic-based message fetch.
          *admin.py:   Topic management using Kafka Admin API.
          *schemas.py: Pydantic models for message validation.
          *__init_.py: initial kafka key classes
11. Extensible for AI Applications
   *The system is split into modular, independent services: producer_service, consumer_service, and input_api_service, each running as a separate FastAPI app.
   *Promotes scalability, maintainability, and separation of concerns.


## 🐳 Getting Started (Docker)

```bash
# Start all services
docker-compose build --no-cache
docker-compose up -d

#Stop all services
docker-compose down
