#! /bin/bash

function cleanup() {
  kill -15 $child_pid

  wait "$child_pid"
}

trap cleanup SIGINT SIGTERM

function check_postgres() {
  python -c "import psycopg2; psycopg2.connect(host='${POSTGRES_HOST}', password='${POSTGRES_PASSWORD}', user='postgres')" 2>/dev/null

  echo $?
}

app_root=$CWT_BASE

if [ -z "${WPS_DEBUG}" ]
then
  while [[ $(check_postgres) -ne 0 ]]; do
    sleep 1 

    echo "Waiting for postgres"
  done
fi

python $app_root/manage.py migrate
python $app_root/manage.py server --host default
python $app_root/manage.py processes

if [ -z "${WPS_DEBUG}" ]
then
  python app.py "0.0.0.0" "8000" &

  child_pid=$!

  wait "$child_pid"
else
  gunicorn -b 0.0.0.0:8000 --reload --chdir $app_root/ compute.wsgi $@ &

  child_pid=$!

  wait "$child_pid"
fi
