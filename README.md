# 🔄 Real-Time Data Streaming Pipeline

Welcome to the **Real-Time Data Streaming Pipeline** repository! 🚀  
This project demonstrates a robust real-time data pipeline architecture built using open-source technologies such as **Apache Kafka**, **Apache Spark**, **Apache Cassandra**, and **Apache Airflow** — all orchestrated in **Docker**.

---

## 🏗️ System Architecture

![System Architecture](System%20Architecture.png)

### Components:

- **API** – Simulates real-time user data generation.
- **Airflow** – Orchestrates the workflow and schedules data generation tasks.
- **Kafka** – Ingests and distributes user data messages in real-time.
- **Zookeeper** – Coordinates and manages Kafka brokers.
- **Kafdrop** – UI tool to monitor Kafka topics.
- **Schema Registry** – Ensures consistent message structure in Kafka topics.
- **Spark Structured Streaming** – Consumes Kafka messages, applies transformations and validation.
- **Cassandra** – Stores the cleaned, structured data for querying.
- **PostgreSQL** – Used by Airflow as metadata DB.
- **Docker Compose** – Manages all services as containers.

---

## 📦 Pipeline Flow Overview

1. **User Generator API** produces synthetic user profiles.
2. **Airflow DAG** schedules the data generation and pushes it into Kafka.
3. **Kafka** acts as a real-time message broker.
4. **Spark**:
   - Subscribes to Kafka topics
   - Applies schema to parse user events
   - Filters invalid UUIDs and malformed data
5. **Cassandra** stores processed data for analytics or visualization.

---

## 👤 Sample User Fields

Each message represents a user with the following attributes:

| Field            | Description                  |
|------------------|------------------------------|
| `id`             | Unique UUID                  |
| `first_name`     | First name                   |
| `last_name`      | Last name                    |
| `email`          | Email address                |
| `phone`          | Contact number               |
| `gender`         | Gender                       |
| `dob`            | Date of birth                |
| `address`        | Street address               |
| `postcode`       | Postal/ZIP code              |
| `username`       | Username                     |
| `registered_date`| Timestamp of user creation   |
| `picture`        | URL to profile picture       |

---

## 🛠️ Tech Stack

| Layer               | Technology                  |
|--------------------|-----------------------------|
| Orchestration       | Apache Airflow              |
| Ingestion           | Kafka + Zookeeper           |
| Monitoring          | Kafdrop                     |
| Schema Management   | Schema Registry (Confluent) |
| Processing          | Apache Spark Structured Streaming |
| Storage             | Apache Cassandra            |
| Containerization    | Docker + Docker Compose     |

