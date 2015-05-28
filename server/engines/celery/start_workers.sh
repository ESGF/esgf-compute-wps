#!/usr/bin/env bash

spark-submit  --master local[1]  engines/celery/worker.py

# celery -A tasks worker --loglevel=debug --concurrency 1 -n worker1.%h
#celery -A tasks worker --loglevel=debug  --concurrency 1 -n worker2.%h &
#celery -A tasks worker --loglevel=debug  --concurrency 1 -n worker3.%h &
#celery -A tasks worker --loglevel=debug  --concurrency 1 -n worker4.%h &


