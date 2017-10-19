#! /bin/bash

cd /var/www/compute/compute

celery inspect ping -A compute -b $CELERY_BROKER -d celery@$HOSTNAME

if [[ $? -ge 1 ]]
then
  exit 1
fi

exit 0
