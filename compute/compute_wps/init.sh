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

python manage.py migrate
python manage.py server --host default
python manage.py create_api_user ${API_USERNAME} ${API_PASSWORD}
