#! /bin/bash

source activate wps

pushd /var/www/compute/compute

exec celery worker -A compute -b ${CELERY_BROKER_URL} ${@} &

celery_ret=${?}
celery_pid=${!}

if [[ ${celery_ret} -ne 0 ]]; then
  echo "Celery failed to start"

  return ${celery_ret}
fi

exec python wps/metrics.py &

metrics_ret=${?}
metrics_pid=${!}

if [[ ${metrics_ret} -ne 0 ]]; then
  echo "Metrics failed to start"

  return ${metrics_ret}
fi

while sleep 60; do
  if [[ $(kill -0 ${celery_pid}) -ne 0 ]] || [[ $(kill -0 ${metrics_pid}) -ne 0 ]]; then
    echo "Child process died"

    exit 1 
  fi
done
