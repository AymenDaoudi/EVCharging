# EVCharging

## Description

This is a data engineering project that simulates the journey of electric vehicle charging session data, from virtual charging stations and vehicles to a final destination in a data warehouse.

The setup simulates thousands of electric vehicles initiating randomly and in parallel, millions of charging sessions from thousands of charging stations, and sending the charging progress data to the system.

The data is continuously sent, stream-processed, and sent to a versioned data lake. Just like a Git repository, the data is continuously added to the data lake first as temporary branches, which, after verification, are merged into the final state (a main branch). Every time data is added to the main branch, it undergoes transformation and then loading to the final data warehouse, where we can finally conduct data analysis or any other useful use of this precious data.

## Tools Used

The following tools are used in this setup:

- **Python and Simpy**: The main application uses the charging stations and electric vehicles data to initiate millions of charging session events randomly and in parallel.
- **Kafka**: Used to stream the charging session events.
- **Apache Spark**: Stream-processes the charging session events, reads them from Kafka, and loads them to the data lake. Additionally, it is used to batch process the data from the data lake, clean it, transform it, and load it to the data warehouse.
- **LakeFS**: A data lake management platform that turns your object storage into a Git-like repository, enabling version control and collaborative data workflows.
- **MinIO**: An open-source, high-performance object storage system that is fully compatible with Amazon S3 APIs. LakeFS stores data here under the hood.
- **ClickHouse**: Used for data warehousing, it is a column-oriented database management system optimized for fast, scalable real-time analytical processing on large datasets.
- **CH-UI**: A tool to visually interact with ClickHouse.
- **MongoDB**: Used to store data related to EVs and charging stations.

## Source of Data

### Charging Stations

The charging stations data is taken from this [Kaggle dataset](https://www.kaggle.com/datasets/vivekattri/global-ev-charging-stations-dataset/data).

### Electric Vehicles

The electric vehicles data is taken from this [Kaggle dataset](https://www.kaggle.com/datasets/fatihilhan/electric-vehicle-specifications-and-prices).
