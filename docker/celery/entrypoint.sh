#! /bin/bash

source activate wps

base_dir=/var/www/compute/compute

celery $SUBCOMMAND -A compute -b $CELERY_BROKER --workdir $base_dir -l INFO $@
