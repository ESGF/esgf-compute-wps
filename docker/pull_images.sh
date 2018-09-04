#! /bin/bash

docker pull jasonb87/cwt_common:$1

docker pull jasonb87/cwt_wps:$1

docker pull jasonb87/cwt_celery:$1
