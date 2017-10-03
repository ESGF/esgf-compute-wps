#! /bin/bash

root_path="/var/www/compute/compute"

cd $root_path

celery worker -A compute -b $CELERY_BROKWER $@
