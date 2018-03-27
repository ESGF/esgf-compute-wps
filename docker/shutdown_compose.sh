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

export DEPLOY_DIR="${PWD}/_deploy"

docker-compose -f docker-compose-new.yml down -v

if [[ $REMOVE_CONFIG -eq 1 ]]
then
  sudo rm -rf ${DEPLOY_DIR}

  sudo rm ${PWD}/docker-compose-new.yml*
fi
