#! /bin/bash

DEVEL=0
VERBOSE=0
CONFIG_ONLY=0
OAUTH_CLIENT=""
OAUTH_SECRET=""
POSTGRES_PASSWORD="abcd1234"
TLS_KEY=""
TLS_CRT=""
WPS_HOST="0.0.0.0"
CLEAN=0

export DEPLOY_DIR="${PWD}/_deploy"

function usage {
  echo -e "Usage: $0 OPTIONS COMMAND"
  echo -e ""
  echo -e "Options:"
  echo -e "  --dev:               Configure CWT WPS stack for developement"
  echo -e "  --config-only:       Only generate the configurations"
  echo -e "  --oauth-client-id:   OAuth2 client ID"
  echo -e "  --oauth-secret:      OAuth2 secret"
  echo -e "  --postgres-password: Postgres password"
  echo -e "  --tls-key:           Path to TLS key"
  echo -e "  --tls-crt:           Path to TLS certificate"
  echo -e "  --host:              A comma separated list of hostnames that Django will server (django ALLOWED_HOSTS)"
  echo -e "  --verbose:           Verbose output"
  echo -e "  --clean:             Stops the docker containers and deletes the configuration"
  echo -e "  --help:              Print usage statement"
}

[[ $# -eq 0 ]] && usage

while [[ $# -gt 0 ]]
do
  arg=$1
  shift

  case "$arg" in
    --dev) DEVEL=1;;
    --config-only) CONFIG_ONLY=1;;
    --oauth-client-id) OAUTH_CLIENT=$1 && shift;;
    --oauth-secret) OAUTH_SECRET=$1 && shift;;
    --postgres-password) POSTGRES_PASSWORD=$1 && shift;;
    --tls-key) TLS_KEY=$1 && shift;;
    --tls-crt) TLS_CRT=$1 && shift;;
    --host) WPS_HOST=$1 && shift;;
    --clean) CLEAN=1 && shift;;
    -v|--verbose) VERBOSE=1;;
    -h|--help) usage && exit 0;;
    *) echo -e "Unknown argument $arg" && usage && exit 0;;
  esac
done

if [[ $VERBOSE -eq 1 ]]
then
  echo -e "Environment:"
  echo -e "  DEVEL=${DEVEL}"
  echo -e "  CONFIG_ONLY=${CONFIG_ONLY}"
  echo -e "  OAUTH_CLIENT=${OAUTH_CLIENT}"
  echo -e "  OAUTH_SECRET=${OAUTH_SECRET}"
  echo -e "  POSTGRES_PASSWORD=${POSTGRES_PASSWORD}"
  echo -e "  TLS_KEY=${TLS_KEY}"
  echo -e "  TLS_CRT=${TLS_CRT}"
  echo -e "  WPS_HOST=${WPS_HOST}"
  echo -e "  CLEAN=${CLEAN}"
  echo -e ""
fi

export DEPLOY_DIR="${PWD}/_deploy"

[[ $(command -v kubectl 2>&1) ]] || (echo "Kubectl command cannot be found, please check that it is installed." && exit 1)

if [[ $CLEAN -eq 1 ]]
then
  kubectl delete -f kubernetes/

  kubectl delete configmap app-config
  kubectl delete configmap django-config
  kubectl delete configmap traefik-config
  kubectl delete secret ssl-secret
  kubectl delete secret app-secret

  sudo rm -rf $DEPLOY_DIR

  exit 1
fi

if [[ ! -e "$DEPLOY_DIR" ]]
then
  mkdir -p $DEPLOY_DIR{/data/public,/data/cache,/db,/tmp,/user,/conf}

  cp common/app.properties $DEPLOY_DIR/conf
  cp common/django.properties $DEPLOY_DIR/conf
  cp kubernetes/traefik.toml $DEPLOY_DIR/conf

  sed -i "s/WPS_HOST=.*/WPS_HOST=$WPS_HOST/g" $DEPLOY_DIR/conf/app.properties

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
fi

[[ "$CONFIG_ONLY" -eq "1" ]] && exit 1

SELECTOR="app=cwt,group in (core, traefik)"

[[ "$DEVEL" -eq "1" ]] && SELECTOR="${SELECTOR},environment in (development, development-production)" || SELECTOR="${SELECTOR},environment in (production, development-production)"

kubectl apply -f kubernetes/ -l "$SELECTOR"
