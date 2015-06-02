#!/usr/bin/env bash

celery -A engines.celery.tasks worker --loglevel=debug --concurrency 1 -n worker1.%h
#celery -A tasks worker --loglevel=debug  --concurrency 1 -n worker2.%h &
#celery -A tasks worker --loglevel=debug  --concurrency 1 -n worker3.%h &
#celery -A tasks worker --loglevel=debug  --concurrency 1 -n worker4.%h &


