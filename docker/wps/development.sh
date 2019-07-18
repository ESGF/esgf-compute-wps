#! /bin/bash

export WPS_DEBUG=1
export PROVISIONER_FRONTEND=127.0.0.1:7777
export DJANGO_CONFIG_PATH=/etc/config/django.properties

python /compute/manage.py migrate
python /compute/manage.py runserver 0.0.0.0:80
