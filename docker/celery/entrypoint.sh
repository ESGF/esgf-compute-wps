#! /bin/bash

source activate wps

root_path="/var/www/compute/compute"

cd $root_path

exec celery worker -A compute -b $CELERY_BROKER $@
