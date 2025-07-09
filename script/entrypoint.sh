#!/bin/bash

set -e

if [ -f "/opt/airflow/requirements.txt" ]; then
    echo "Installing Python dependencies..."
    pip install --no-cache-dir -r /opt/airflow/requirements.txt
fi

if [ ! -f "/opt/airflow/airflow.db" ]; then
    echo "Initializing Airflow DB..."
    airflow db init

    echo "Creating admin user..."
    airflow users create \
        --username admin \
        --firstname admin \
        --lastname admin \
        --role Admin \
        --email admin@example.com \
        --password admin
fi

# Upgrade DB and start webserver
airflow db upgrade
exec airflow webserver