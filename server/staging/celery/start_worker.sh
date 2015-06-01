#!/usr/bin/env bash
if [ $# -eq 0 ]; then
    echo "Usage: $0 <engine>"
else
  if [ $1 = 'spark' ]; then
    echo "Starting Celery worker as Spark master node"
    spark-submit  --master 'local[1]'  staging/celery/worker.py
  else
    echo "Starting Celery worker"
    celery -A staging.celery.manager worker --loglevel=debug  --concurrency 1 -n worker.manager &
  fi
fi
