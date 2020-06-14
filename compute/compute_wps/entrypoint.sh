#! /bin/bash 

gunicorn -b 0.0.0.0:8000 \
  --worker-tmp-dir /dev/shm \
  --workers 2 \
  --threads 4 \
  --worker-class gthread \
  compute.wsgi
