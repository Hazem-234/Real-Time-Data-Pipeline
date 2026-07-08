#!/bin/bash
set -e

# 1. Install requirements safely
if [ -e "/opt/airflow/requirements.txt" ]; then
  python -m pip install --upgrade pip
  pip install -r /opt/airflow/requirements.txt
fi

# 2. Upgrade the Postgres database
airflow db upgrade

# 3. Create the user (the || true prevents crashes on future restarts)
airflow users create \
    --username admin \
    --firstname admin \
    --lastname admin \
    --role Admin \
    --email admin@example.com \
    --password admin || true

# 4. Start the webserver
exec airflow webserver