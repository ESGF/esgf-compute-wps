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

if [[ -n "${CWT_METRICS}" ]]; then
  exec python wps/metrics.py &

  metrics_ret=${?}
  metrics_pid=${!}

  if [[ ${metrics_ret} -ne 0 ]]; then
    echo "Metrics failed to start"

    return ${metrics_ret}
  fi
fi

while sleep 60; do
  if [[ $(kill -0 ${celery_pid}) -ne 0 ]]; then
    echo "Celery process died"

    exit 1
  fi

  if [[ -n "${CWT_METRICS}" ]] && [[ $(kill -0 ${metrics_pid}) -ne 0 ]]; then
    echo "Metrics process died"

    exit 1
  fi
done
