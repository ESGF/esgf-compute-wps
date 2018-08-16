#! /bin/bash

source activate wps

pushd /var/www/compute/compute

metrics_path='/tmp/cwt_metrics'

rm -rf $metrics_path

mkdir $metrics_path

exec celery worker -A compute -b $CELERY_BROKER_URL $@ &

status=$?

if [[ $status -ne 0 ]]; then
  echo "Failed to start Celery worker"

  exit $status
fi

exec python wps/metrics.py &

status=$?

if [[ $status -ne 0 ]]; then
  echo "Failed to start metrics server"

  exit $status
fi

while sleep 60; do
  ps aux | grep metrics.py | grep -q -v grep
  status1=$?
  ps aux | grep celery | grep -q -v grep
  status2=$?

  if [[ $status1 -ne 0 ]] || [[ $status2 -ne 0 ]]; then
    echo "Something happend to a child process"

    exit 1
  fi
done
