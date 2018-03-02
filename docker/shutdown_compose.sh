#! /bin/bash

export DEPLOY_DIR="./deploy_dir"

docker-compose down -v

sudo rm -rf ./_deploy/
