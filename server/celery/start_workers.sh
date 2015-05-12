#!/usr/bin/env bash

source /Developer/Projects/EclipseWorkspace/uvcdat/master/build/install/bin/setup_runtime.sh

celery -A tasks worker --loglevel=debug -P eventlet --concurrency 4 -n worker1.%h
#celery -A tasks worker --loglevel=debug -P eventlet --concurrency 1 -n worker2.%h &
#celery -A tasks worker --loglevel=debug -P eventlet --concurrency 1 -n worker3.%h &
#celery -A tasks worker --loglevel=debug -P eventlet --concurrency 1 -n worker4.%h &


