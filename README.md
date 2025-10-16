# Handson-L8-Spark-SQL_Streaming

**Cloud Computing for Data Analysis (ITCS 6190/8190, Fall 2025)**  
Instructor: *Marco Vieira*

## Overview
This project implements a real-time ride-sharing analytics pipeline using Apache Spark Structured Streaming.  
It includes three tasks and a data generator for continuous JSON ride data.

### Tasks
**Task 1 – Basic Streaming Ingestion and Parsing**
- Reads data from `localhost:9999`
- Parses JSON messages into columns
- Prints parsed data to the console

**Task 2 – Real-Time Aggregations (Driver-Level)**
- Calculates `SUM(fare_amount)` and `AVG(distance_km)` grouped by `driver_id`
- Streams output to CSV files under `outputs/task2_outputs/`

**Task 3 – Windowed Time-Based Analytics**
- Converts timestamp to proper `TimestampType`
- Performs a 5-minute window aggregation on `fare_amount`, sliding by 1 minute, with a 1-minute watermark
- Outputs CSV results under `outputs/task3_outputs/`

## Running the Project
```bash
pip install pyspark
python data_generator.py    # Run first (produces continuous data stream)
python task1.py             # Run in another terminal
python task2.py             # Run in another terminal
python task3.py             # Run in another terminal
```

Check the `outputs/` folder for CSV outputs. Each task folder includes several `part-...csv` files and `_SUCCESS` markers.

## Approach, Results, and Observations
### Approach
- The pipeline follows a producer-consumer model.
- The `data_generator.py` produces synthetic data and streams it via socket.
- Each task consumes this stream and performs different levels of processing using Spark Structured Streaming.

### Results Summary
- **Task 1:** Successfully parsed incoming JSON lines; console displayed structured ride records.
- **Task 2:** Produced per-driver aggregations of total fare and average trip distance in real time.
- **Task 3:** Demonstrated window-based analysis; generated rolling 5-minute fare totals.

### Observations
- Proper checkpoint directories (`/tmp/checkpoints/`) are required for reliable stateful streaming.
- Task 3 may produce empty batches until enough data is processed for a full 5-minute window.
- Running for multiple batches (>35) ensures visible CSV outputs for windowed results.
- The Spark output format was retained (multiple `part-` CSV files and `_SUCCESS` files) for submission compliance.

## Project Structure
Handson-L8-Spark-SQL_Streaming/
├── outputs/
│   ├── task1_outputs/
│   ├── task2_outputs/
│   └── task3_outputs/
├── data_generator.py
├── task1.py
├── task2.py
├── task3.py
├── requirements.txt
├── .gitignore
└── README.md
