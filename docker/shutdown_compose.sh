#! /bin/bash

REMOVE_CONFIG=1

while [[ $# -gt 0 ]]
do
  arg=$1
  shift

  case $arg in
    --no-delete|-n)
      REMOVE_CONFIG=0
    ;;
  esac
done

export DEPLOY_DIR="./deploy_dir"

docker-compose -f docker-compose-new.yml down -v

[[ $REMOVE_CONFIG -eq 1 ]] && sudo rm -rf ./_deploy/
