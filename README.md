# 🚨 Real-Time Credit Card Fraud Detection ETL Pipeline

A robust real-time ETL pipeline to detect fraudulent credit card transactions using **Apache Kafka**, **Apache Spark**, **PostgreSQL**, and **Power BI**. Designed to ingest and process thousands of records per minute, this project enables real-time fraud flagging and dashboard analytics.

![Real-Time Fraud Detection Pipeline](ETL-Page-1.drawio.jpeg)

---

## 🔎 Project Description

This project simulates a high-volume **credit card transaction system** that detects anomalies using a pre-trained machine learning model and real-time streaming tools.

### ✨ Key Highlights:

- Processes **5,000+ transactions per minute**
- Real-time data ingestion via **Apache Kafka**
- Streaming ETL processing using **Apache Spark Structured Streaming**
- Anomaly detection using a pre-trained **SVM classifier**
- Data orchestration with **Apache Airflow**
- Reporting and monitoring with **Power BI**

---

## 🛠 Tech Stack

| Tool / Tech         | Description                                      |
|---------------------|--------------------------------------------------|
| Apache Kafka         | Real-time event streaming                        |
| Apache Spark         | Streaming ETL and fraud detection model          |
| PostgreSQL           | Data sink for clean and flagged records          |
| Apache Airflow       | DAG-based orchestration                          |
| Jupyter Notebook     | Model development and pipeline testing           |
| Power BI             | Dashboard and fraud monitoring                   |
| Docker               | Containerization of all services                 |
| Python               | Glue logic and data preprocessing                |

---

## 🧠 ML Model

- **Model Type**: Support Vector Machine (SVM)
- **Accuracy**: 99.6%
- **Training**: Based on real-world anonymized credit card transaction dataset
- **Integration**: Model is used within Spark to flag fraud in streaming batches

---

## 📊 Power BI Dashboard

- View fraud detection in real-time
- Filter transactions by location, amount, status (fraud/not fraud)
- Monitor daily/weekly trends in flagged activity

---

## 📂 Project Structure





  
