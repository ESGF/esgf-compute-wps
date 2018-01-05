#! /bin/bash

source activate wps

app_root="/var/www/compute/compute"

python $app_root/manage.py collectstatic --no-input

python $app_root/manage.py migrate

python $app_root/manage.py server --host default
python $app_root/manage.py processes --register
python $app_root/manage.py capabilities

if [[ -n $WPS_DEBUG ]]
then
  WPS_DEBUG=1
fi

if [[ "$WPS_DEBUG" -eq 0 ]]
then
  python app.py "0.0.0.0" "8000"
else
  gunicorn -b 0.0.0.0:8000 --reload --chdir $app_root/ compute.wsgi $@
fi
