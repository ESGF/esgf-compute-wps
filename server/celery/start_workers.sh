#!/usr/bin/env bash

source /Developer/Projects/EclipseWorkspace/uvcdat/master/build/install/bin/setup_runtime.sh

celery -A tasks worker --loglevel=debug -P eventlet --concurrency=1 &


