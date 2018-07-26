#! /bin/bash

source activate wps

pushd /var/www/compute/compute

celery -A compute -b $CELERY_BROKER $@
