#! /bin/bash

source activate wps

pushd /var/www/compute/compute

celery worker -A compute -b $CELERY_BROKER_URL $@
