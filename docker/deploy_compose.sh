#! /bin/bash

. ./parse_input.sh

export DEPLOY_DIR="./_deploy"

if [[ ! -e "$DEPLOY_DIR" ]]
then
  mkdir $DEPLOY_DIR

  cp common/app.properties $DEPLOY_DIR

  echo "POSTGRES_PASSWORD=$POSTGRES_PASSWORD" >> $DEPLOY_DIR/app.properties

  cat $DEPLOY_DIR/app.properties

  sed -i.bak "s/WPS_HOST=.*/WPS_HOST=$WPS_HOST/g" $DEPLOY_DIR/app.properties

  if [[ "$DEV" -eq "1" ]]
  then
    echo "WPS_DEBUG=1" >> $DEPLOY_DIR/app.properties

    mkdir -p $DEPLOY_DIR/data/public
    
    mkdir -p $DEPLOY_DIR/data/cache

    mkdir -p $DEPLOY_DIR/tmp

    mkdir -p $DEPLOY_DIR/user
  fi

  cp common/django.properties $DEPLOY_DIR
fi

docker-compose up -d

sleep 2

docker-compose ps
