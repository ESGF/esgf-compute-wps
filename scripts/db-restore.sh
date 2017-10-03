#! /bin/bash

cat $1 | docker-compose -f ../docker/docker-compose.yml psql -U postgres
