<b> DigitalXC ITSM Project </b>

This project is about creating a data pipeline to analyze the ServiceNow Ticket data, from ingestion and transformation. It leverages several tools including DBT, PostgresDB and Apache Airflow.

<b>
Project Overview </b>

The pipeline processes a ticket dump, applies transformations, and delivers key metrics. 

Implemented tasks include:


Data Ingestion & Transformation (DBT & PostgresDB): Raw ticket data is loaded into a PostgreSQL database. DBT performs data cleaning, transformations, and creates aggregated tables for analysis.

Workflow Orchestration (Apache Airflow): Airflow coordinates the entire workflow—from data ingestion to executing DBT transformations and performing validation.


Technologies Used: 

1. VS Code for coding
2. PostgresDB: Database for storing the raw and transformed ticket data.
3. DBT (Data Build Tool): For data transformation and modeling.
4. Apache Airflow: For workflow orchestration and scheduling.
