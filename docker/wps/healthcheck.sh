#! /bin/bash

curl --silent --fail http://0.0.0.0:8000/wps || exit 1 

echo "WPS endpoint success"

curl --silent --fail http://0.0.0.0:8000/wps/home || exit 1

echo "Web App endpoint success"
