#! /bin/bash

cd /var/www/compute/compute

celery -A compute -b $CELERY_BROKER worker $@
