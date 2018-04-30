#! /bin/bash

. deploy_compose.sh $@ --parse

function clean {
  kubectl delete configmap app-config
  kubectl delete configmap django-config
  kubectl delete configmap traefik-config
  kubectl delete secret ssl-secret
  kubectl delete secret app-secret

  sudo rm -rf $DEPLOY_DIR
}

function configuration {
  mkdir -p $DEPLOY_DIR{/data/public,/data/cache,/db,/tmp,/user,/conf}

  cp common/app.properties $DEPLOY_DIR/conf
  cp common/django.properties $DEPLOY_DIR/conf
  cp kubernetes/traefik.toml $DEPLOY_DIR/conf

  sed -i "s/WPS_HOST=.*/WPS_HOST=$HOST/g" $DEPLOY_DIR/conf/app.properties

  if [[ $DEVEL -eq 1 ]]
  then
    echo "WPS_DEBUG=1" >> $DEPLOY_DIR/conf/app.properties
  fi

  kubectl create configmap app-config --from-env-file=$DEPLOY_DIR/conf/app.properties

  kubectl create configmap django-config --from-file=$DEPLOY_DIR/conf/django.properties

  kubectl create configmap traefik-config --from-file=$DEPLOY_DIR/conf/traefik.toml

  if [[ $TLS_KEY -eq "" ]]
  then
    . generate_certs.sh

    TLS_KEY=$DEPLOY_DIR/ssl/tls.key

    TLS_CRT=$DEPLOY_DIR/ssl/tls.crt
  fi

  kubectl create secret generic ssl-secret --from-file=$TLS_CRT --from-file=$TLS_KEY

  kubectl create secret generic app-secret --from-literal=oauth_client=$OAUTH_CLIENT --from-literal=oauth_secret=$OAUTH_SECRET --from-literal=postgres_password=$POSTGRES_PASSWORD
}

[[ $(command -v kubectl 2>&1) ]] || (echo "Kubectl command cannot be found, please check that it is installed." && exit 1)

SELECTOR="app=cwt,group in (core, traefik)"

[[ "$DEVEL" -eq "1" ]] && SELECTOR="${SELECTOR},environment in (development, development-production)" || SELECTOR="${SELECTOR},environment in (production, development-production)"

case ${COMMAND,,} in
  start)
    if [[ $CLEAN -eq 1 ]]
    then
      clean
    fi

    if [[ ! -e $DEPLOY_DIR ]]
    then
      configuration
    fi

    kubectl apply -f kubernetes/ -l "$SELECTOR" 
    ;;
  stop)
    kubectl delete -f kubernetes/ -l "$SELECTOR"

    [[ $CLEAN -eq 1 ]] && clean
    ;;
  update)
    kubectl delete -f kubernetes/ -l "$SELECTOR"

    clean

    configuration

    kubectl apply -f kubernetes/ -l "$SELECTOR" 
    ;;
  config)
    [[ $CLEAN -eq 1 ]] && clean

    configuration
    ;;
esac
