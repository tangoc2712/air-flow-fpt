#!/bin/bash
# Script to install Python dependencies in Airflow containers

echo "Installing Python dependencies in Airflow containers..."

# Copy requirements.txt to containers
echo "Copying requirements.txt to containers..."
docker cp /Users/quangngoc/sorceCode/training/fpt_airflow_course/final_exam/requirements.txt airflow-webserver:/opt/airflow/requirements.txt
docker cp /Users/quangngoc/sorceCode/training/fpt_airflow_course/final_exam/requirements.txt airflow-scheduler:/opt/airflow/requirements.txt

# Install dependencies in the webserver container
echo "Installing dependencies in webserver container..."
docker exec -it airflow-webserver pip install -r /opt/airflow/requirements.txt

# Install dependencies in the scheduler container
echo "Installing dependencies in scheduler container..."
docker exec -it airflow-scheduler pip install -r /opt/airflow/requirements.txt

echo "Dependencies installation completed!"