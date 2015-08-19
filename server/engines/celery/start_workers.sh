#!/usr/bin/env bash
echo "Started celery workers with PYTHONPATH: "
echo $PYTHONPATH

celery -A engines.celery.tasks worker --loglevel=debug --logfile=/usr/local/web/WPCDAS/server/logs/worker0.log --config=engines.celery.celeryconfig -c 1 -n worker0.%h &
celery -A engines.celery.tasks worker --loglevel=debug --logfile=/usr/local/web/WPCDAS/server/logs/worker1.log --config=engines.celery.celeryconfig -c 1 -n worker1.%h &
celery -A engines.celery.tasks worker --loglevel=debug --logfile=/usr/local/web/WPCDAS/server/logs/worker2.log --config=engines.celery.celeryconfig -c 1 -n worker2.%h &
celery -A engines.celery.tasks worker --loglevel=debug --logfile=/usr/local/web/WPCDAS/server/logs/worker3.log --config=engines.celery.celeryconfig -c 1 -n worker3.%h &

