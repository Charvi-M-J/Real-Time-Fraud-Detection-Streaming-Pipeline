# Real-Time-Spark-Streaming-Project

🚀 Project Overview

This project demonstrates a real-time Data Engineering solution for detecting fraudulent financial transactions using Apache Kafka, Apache Spark Structured 
Streaming, Databricks, Lakeflow Spark Declarative Pipelines, PostgreSQL, and Delta Lake. The pipeline continuously ingests transaction data from Kafka and 
streaming JSON files, processes and transforms the data through a Bronze-Silver-Gold architecture, enriches transactions using customer and fraud watchlist data,
and identifies potentially fraudulent transactions in real time. The project also implements real-time email alerts and pipeline orchestration using Lakeflow Jobs.

📸 Screenshots

![image alt](https://github.com/Charvi-M-J/Real-Time-Spark-Streaming-Project/blob/4a7094f8a30b6e741aec905fe5cd8b81a3c4af03/screenshots/ChatGPT%20Image%20Sep%2011%2C%202026%2C%2011_30_14%20AM.png)

![image alt](https://github.com/Charvi-M-J/Real-Time-Spark-Streaming-Project/blob/e891e17cc191ecca065e928ee05b634f73e3d6dc/screenshots/Screenshot%202026-09-11%20114454.png)

![image alt](https://github.com/Charvi-M-J/Real-Time-Spark-Streaming-Project/blob/992cd6a78ec3f9c373d7ca32e78590002a6181f9/screenshots/Screenshot%202026-09-08%20105923.png)


🛠️ Technologies used:
🔹Apache Kafka – Produces and streams real-time transaction data
🔹Confluent Kafka – Provides the Kafka cloud platform and manages Kafka topics and streaming infrastructure
🔹Apache Spark Structured Streaming – Consumes and processes streaming data
🔹Databricks – Provides the environment for real-time data processing
🔹Lakeflow Spark Declarative Pipelines – Builds and manages streaming data pipelines
🔹Auto Loader – Automatically ingests newly arriving JSON files
🔹PostgreSQL – Stores historical customer and transaction data
🔹Lakeflow Connect – Ingests batch data from PostgreSQL
🔹Delta Lake – Stores data using Bronze, Silver, and Gold layers
🔹Unity Catalog – Provides data governance and access control

🏗️ Project Architecture

              Kafka / Streaming JSON Files
                         |
                         v
              Spark Structured Streaming
                         |
                         v
                  Bronze Layer
                  Delta Lake
                         |
                         v
                  Silver Layer
                         |
             +-----------+-----------+
             |                       |
             v                       v
      Customer Data           Fraud Watchlist
      PostgreSQL              Auto Loader
             |                       |
             v                       v
      Lakeflow Connect       Streaming Data
             |                       |
             +-----------+-----------+
                         |
                         v
              Stream-Static / Stream-Stream
                       Joins
                         |
                         v
                 Fraud Detection
                         |
                         v
                   Gold Layer
                   Delta Lake
                         |
                         v
                 Real-Time Alerts
                         |
                         v
                    Gmail SMTP
                         |
                         v
                   Email Alert


🔑 Key Learnings
🔹Real-Time Data Streaming: Learned to build real-time streaming pipelines using Apache Kafka and Spark Structured Streaming for continuous transaction processing.
🔹Kafka Integration: Gained hands-on experience setting up Kafka producers, topics, and connecting Kafka streams with Spark for real-time data consumption.
🔹Streaming Data Processing: Implemented Spark Structured Streaming concepts including streaming triggers, stateless and stateful processing, windowing, 
   aggregations, and watermarking.
🔹Medallion Architecture: Designed Bronze, Silver, and Gold layers using Delta Lake to organize raw, cleansed, and business-ready data.
🔹Stream-Static Join: Joined streaming transaction data with static customer information to enrich transaction records.
🔹Stream-Stream Join: Implemented stream-stream joins between transaction data and fraud watchlist data for real-time fraud detection.
🔹Auto Loader: Used Auto Loader to automatically detect and ingest newly arriving JSON files without manually processing each file.
🔹Fraud Detection: Developed streaming logic to identify suspicious financial transactions based on transaction and fraud watchlist information.
🔹Real-Time Alerts: Implemented automated email notifications for detected fraudulent transactions.
🔹Batch Data Integration: Ingested historical customer data from PostgreSQL using Lakeflow Connect.
🔹Data Governance: Used Unity Catalog for managing and governing data assets.


Python / PySpark – Performs data transformation and processing
Gmail SMTP – Sends real-time fraud alert emails
