#! /bin/bash

source activate wps

app_root="/var/www/compute/compute"

python $app_root/manage.py collectstatic --no-input

python $app_root/manage.py migrate

python $app_root/manage.py register_processes

gunicorn --env WPS_INIT=1 -b 0.0.0.0:8000 --reload --chdir $app_root/ compute.wsgi $@
