#! /bin/bash

python manage.py collectstatic --no-input

if [ ! -z "$POSTGRES_HOST" ]
then
  until $(ping -c1 "$POSTGRES_HOST" 2> /dev/null && echo $?)
  do
    echo "Waiting for database at $POSTGRES_HOST"

    sleep 4
  done

  python manage.py migrate
fi

exec $@
