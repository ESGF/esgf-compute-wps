#! /bin/bash

sed -i.bak "s|\(FROM.*:\).*|\\1${1}|g" celery/Dockerfile

sed -i.bak "s|\(FROM.*:\).*|\\1${1}|g" wps/Dockerfile

sed -i.bak "s|\(.*image: jasonb87/cwt_celery:\).*|\\1${1}|g" docker-compose-template.yml

sed -i.bak "s|\(.*image: jasonb87/cwt_wps:\).*|\\1${1}|g" docker-compose-template.yml

sed -i.bak "s|\(.*image: jasonb87/cwt_celery:\).*|\\1${1}|g" kubernetes/*.yaml

sed -i.bak "s|\(.*image: jasonb87/cwt_wps:\).*|\\1${1}|g" kubernetes/*.yaml
