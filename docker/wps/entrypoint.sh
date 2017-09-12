#! /bin/bash

cd wps


cd ..

python manage.py collectstatic --no-input

if [ ! -z "$POSTGRES_HOST" ]
then
  while [$(ping -c1 postgres > /dev/null | echo $?) -eq 0]
  do
    sleep 1
  done

  python manage.py migrate
fi

exec $@
