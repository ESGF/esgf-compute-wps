#! /bin/bash

pushd /var/www/webapp/compute

source activate wps

celery inspect ping -A compute -b $CELERY_BROKER_URL

if [[ $? -ne 0 ]]
then
  exit $?
fi

curl http://127.0.0.1:8080/metrics

if [[ $? -ne 0 ]]
then
  exit $?
fi

exit 0
