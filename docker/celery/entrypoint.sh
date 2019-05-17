#! /bin/bash

[[ -e "/metrics" ]] && rm -rf /metrics

mkdir /metrics

compute-tasks-metrics &

celery worker -A compute_tasks ${@} 
