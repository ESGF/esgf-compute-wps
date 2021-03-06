#! /bin/bash

function check_postgres() {
  python -c \
    "import psycopg2; psycopg2.connect(host='${POSTGRES_HOST}', password='${POSTGRES_PASSWORD}', user='postgres')" 2>/dev/null

  echo $?
}

while [[ $(check_postgres) -ne 0 ]]; do
  sleep 2

  echo "Waiting for postgres"
done

export DJANGO_SETTINGS_MODULE=compute_wps.settings
export DJANGO_CONFIG_PATH=/etc/config/django.properties

python manage.py migrate
