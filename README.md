# Weather-Sales Analysis Pipeline

This project implements an Apache Airflow pipeline for collecting weather data, processing sales transactions, and analyzing the relationship between weather conditions and sales performance.

## Overview

The pipeline consists of three main DAGs (Directed Acyclic Graphs):

1. **Weather Data Collector**: Collects daily weather data for store locations from the Open-Meteo API
2. **Sales Weather ETL**: Processes weather data and sales transactions, and creates a fact table for analysis
3. **Sales Weather Insights**: Generates insights on how weather affects sales across different regions, products, and store types

## Project Structure

```
final_exam/
├── config/               # Configuration files
├── dags/                 # Airflow DAG definitions
│   ├── sales_weather_etl.py
│   ├── sales_weather_insights.py
│   ├── weather_data_collector.py
├── data/                 # Data storage
│   ├── final/            # Final reports and insights
│   ├── processed/        # Intermediate processed data
│   ├── raw/              # Raw input data
├── logs/                 # Airflow logs
├── plugins/              # Airflow plugins
├── scripts/              # Utility scripts
├── docker-compose.yml    # Docker configuration
├── requirements.txt      # Python dependencies
```

## Setup Instructions

### Prerequisites

- Docker and Docker Compose
- Git
- 8GB+ RAM allocated to Docker

### Installation

1. Clone the repository (if applicable):
   ```bash
   git clone <repository-url>
   cd fpt_airflow_course/final_exam
   ```

2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

3. Start the Airflow environment:
   ```bash
   docker-compose up -d
   ```

4. Wait for all services to start (may take a few minutes on first run)

5. Access the Airflow web UI:
   ```
   http://localhost:8080
   ```
   - Default username: `airflow`
   - Default password: `airflow`

### Initial Configuration

1. Ensure the Postgres connection is set up in Airflow:
   - Connection ID: `postgres_default`
   - Connection Type: `Postgres`
   - Host: `postgres`
   - Schema: `airflow`
   - Login: `airflow`
   - Password: `airflow`
   - Port: `5432`

2. Enable the DAGs in the Airflow UI:
   - `weather_data_collector`
   - `sales_weather_etl`
   - `sales_weather_insights`

## Usage

The pipeline runs automatically according to the following schedule:

1. **Weather Data Collector** runs daily at 1:00 AM
2. **Sales Weather ETL** runs daily at 1:30 AM
3. **Sales Weather Insights** runs daily at 2:00 AM

You can also trigger the DAGs manually from the Airflow UI for testing.

### Data Flow

1. Raw weather data is collected and stored in `data/raw/weather/{date}/`
2. Weather data is processed and saved to `data/processed/weather/{date}.csv`
3. Weather data and sales transactions are combined in a PostgreSQL fact table
4. Analysis reports are generated in `data/final/reports/`
5. Insights are created in `data/final/insights/`
6. A final comprehensive report is saved to `data/final/weather_sales_final_report.json` and `data/final/weather_sales_final_report.txt`

## Analytics Reports

The pipeline generates the following analytics:

1. **Regional Impact Analysis**: How weather affects sales in different regions
2. **Product Sensitivity Analysis**: Which products are most sensitive to weather changes
3. **Store Size Performance Analysis**: How different store sizes perform under various weather conditions

## Troubleshooting

- Check Airflow logs in the web UI or in the `logs/` directory
- If DAGs fail, ensure the data directories exist with proper permissions
- For database connection issues, verify the PostgreSQL service is running and the connection parameters are correct

## Extending the Project

- Add new cities to the `CITIES` dictionary in `weather_data_collector.py`
- Create new analysis tasks in `sales_weather_insights.py`
- Add more visualization options by modifying the existing Python functions

## License

This project is for educational purposes as part of the FPT Airflow Course.

