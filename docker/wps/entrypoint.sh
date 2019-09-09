#! /bin/bash

function check_postgres() {
  python -c "import psycopg2; psycopg2.connect(host='${POSTGRES_HOST}', password='${POSTGRES_PASSWORD}', user='postgres')" 2>/dev/null

  echo $?
}

if [ -z "${WPS_DEBUG}" ]
then
  while [[ $(check_postgres) -ne 0 ]]; do
    sleep 4

    echo "Waiting for postgres"
  done
fi

python manage.py migrate
python manage.py server --host default
python manage.py create_api_user ${API_USERNAME} ${API_PASSWORD}

if [ -z "${WPS_DEBUG}" ]
then
  python app.py "0.0.0.0" "8000" &

  child_pid=$!

  wait "$child_pid"
else
  gunicorn -b 0.0.0.0:8000 --reload compute.wsgi $@ &

  child_pid=$!

  wait "$child_pid"
fi
