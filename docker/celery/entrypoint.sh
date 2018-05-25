#! /bin/bash

source activate wps

pushd /var/www/compute/compute

exec celery $SUBCOMMAND -A compute -b $CELERY_BROKER -l INFO $@
