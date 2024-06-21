#!/bin/bash

set -e

echo "Before requirements.txt check"
if [ -e "/opt/airflow/requirements.txt" ]; then
  $(command python) pip install --upgrade pip
  $(command -v pip) install --user -r requirements.txt

fi

echo "Before airflow.db check"
if [ ! -f "/opt/airflow/airflow.db" ]; then
  airflow db init && \
  airflow users create \
    --username admin \
    --firstname admin \
    --lastname admin \
    --role Admin \
    --email admin@example.com \
    --password admin
fi

$(command -v airflow) db upgrade

exec airflow webserver