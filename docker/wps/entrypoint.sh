#! /bin/bash

python compute/manage.py collectstatic --no-input

python compute/manage.py migrate

gunicorn -b 0.0.0.0:8000 --reload --chdir compute/ compute.wsgi $@
