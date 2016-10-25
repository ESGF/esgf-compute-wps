#!/bin/bash

python compute/manage.py migrate

exec "$@"
