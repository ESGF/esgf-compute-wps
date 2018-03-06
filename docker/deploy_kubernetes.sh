#! /bin/bash

. parse_input.sh

export DEPLOY_DIR="./_deploy"

[[ $(command -v kubectl 2>&1) ]] || (echo "Kubectl command cannot be found, please check that it is installed." && exit 1)

[[ ! -e "$DEPLOY_DIR" ]] && mkdir -p $DEPLOY_DIR

cp common/app.properties $DEPLOY_DIR/

sed -i.bak "s/WPS_HOST=.*/WPS_HOST=$WPS_HOST/g" $DEPLOY_DIR/app.properties

cp common/django.properties $DEPLOY_DIR/

cp kubernetes/traefik.toml $DEPLOY_DIR/

kubectl create configmap app-config --from-env-file=$DEPLOY_DIR/app.properties

kubectl create configmap django-config --from-file=$DEPLOY_DIR/django.properties

kubectl create configmap traefik-config --from-file=$DEPLOY_DIR/traefik.toml

kubectl create secret generic ssl-secret --from-file=$TLS_CRT --from-file=$TLS_KEY

kubectl create secret generic app-secret --from-literal=oauth_client=$OAUTH_CLIENT --from-literal=oauth_secret=$OAUTH_SECRET --from-literal=postgres_password=$POSTGRES_PASSWORD

[[ "$CONFIG_ONLY" -eq "1" ]] && exit 1

SELECTOR="app=cwt,group=core"

[[ "$DEV" -eq "1" ]] && SELECTOR="${SELECTOR},environment in (development, development-production)" || SELECTOR="${SELECTOR},environment in (production, development-production)"

kubectl apply -f kubernetes/ -l "$SELECTOR"
