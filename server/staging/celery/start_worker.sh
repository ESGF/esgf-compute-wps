#!/usr/bin/env bash

celery -A staging.celery.manager worker --loglevel=debug  --concurrency 1 -n worker.%h &

