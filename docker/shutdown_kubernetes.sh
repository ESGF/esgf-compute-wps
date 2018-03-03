#! /bin/bash

kubectl delete -f kubernetes/ -l app=cwt,group=core -l "environment in (production, development)"

kubectl delete configmap app-config

kubectl delete configmap django-config

kubectl delete configmap traefik-config

kubectl delete secret ssl-secret

kubectl delete secret app-secret
