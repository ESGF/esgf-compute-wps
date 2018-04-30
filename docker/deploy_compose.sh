#! /bin/bash

COMMAND=""
DEVEL=0
OAUTH_ID=""
OAUTH_SECRET=""
POSTGRES_PASSWORD="abcd1234"
TLS_KEY=""
TLS_CRT=""
HOST="0.0.0.0"
CLEAN=0
PARSE=0

DEPLOY_DIR="${PWD}/_deploy"

function usage {
  echo -e "Usage: $0 [start|stop|update|config] OPTIONS"
  echo -e ""
  echo -e "Options:"
  echo -e "  --dev:               Configure CWT WPS stack for developement"
  echo -e "  --oauth-id:          OAuth2 client ID"
  echo -e "  --oauth-secret:      OAuth2 secret"
  echo -e "  --postgres-password: Postgres password"
  echo -e "  --tls-key:           Path to TLS key"
  echo -e "  --tls-crt:           Path to TLS certificate"
  echo -e "  --host:              A comma separated list of hostnames that Django will server (django ALLOWED_HOSTS)"
  echo -e "  --clean:             Remove the previous configuration"
  echo -e "  --help:              Print usage statement"
}

function parse_inputs {
  [[ $# -eq 0 ]] && usage && exit 1

  PRINT=0

  COMMAND=$1

  shift

  while [[ $# -gt 0 ]]
  do
    ARG=$1

    shift

    case $ARG in
      --dev) DEVEL=1 ;;
      --oauth-id) OAUTH_ID=$1 && shift ;;
      --oauth-secret) OAUTH_SECRET=$1 && shift ;;
      --postgres-password) POSTGRES_PASSWORD=$1 && shift ;;
      --tls-key) TLS_KEY=$1 && shift ;;
      --tls-crt) TLS_CRT=$1 && shift ;;
      --host) HOST=$1 && shift ;;
      --clean) CLEAN=1 ;;
      --parse) PARSE=1 ;;
      --help) usage && exit 1 ;;
      --print) PRINT=1 ;;
      *) usage && exit 1 ;;
    esac
  done  

  if [[ $PRINT -eq 1 ]]
  then
    echo "Command: $COMMAND"
    echo "Devel: $DEVEL"
    echo "Oauth id: $OAUTH_ID"
    echo "Oauth secret: $OAUTH_SECRET"
    echo "Postgres password: $POSTGRES_PASSWORD"
    echo "TLS key: $TLS_KEY"
    echo "TLS crt: $TLS_CRT"
    echo "Host: $HOST"
    echo "Clean: $CLEAN"
  fi
}

function clean {
  rm docker-compose.yml

  sudo rm -rf $DEPLOY_DIR
}

function configuration {
  mkdir -p $DEPLOY_DIR{/data/public,/data/cache,/db,/tmp,/user,/conf,/ssl}

  if [[ ! -z "$TLS_CRT" ]] && [[ ! -z "$TLS_KEY" ]]
  then
    cp $TLS_CRT $DEPLOY_DIR

    cp $TLS_KEY $DEPLOY_DIR
  else
    . generate_certs.sh

    TLS_CRT=$DEPLOY_DIR/ssl/tls.crt

    TLS_KEY=$DEPLOY_DIR/ssl/tls.key
  fi

  cp kubernetes/traefik.toml $DEPLOY_DIR/conf
  cp common/app.properties $DEPLOY_DIR/conf
  cp common/django.properties $DEPLOY_DIR/conf

  echo "POSTGRES_PASSWORD=${POSTGRES_PASSWORD}" >> $DEPLOY_DIR/conf/app.properties

  sed -i.bak "s/WPS_HOST=.*/WPS_HOST=$WPS_HOST/g" $DEPLOY_DIR/conf/app.properties

  if [[ $DEVEL -eq 1 ]]
  then
    echo "WPS_DEBUG=1" >> $DEPLOY_DIR/conf/app.properties
  fi

  cp docker-compose-template.yml $DEPLOY_DIR/docker-compose.yml

  sed -i.bak "s|#HOST|$HOST|g" $DEPLOY_DIR/docker-compose.yml
  sed -i.bak "s|\(.*\)# PATH_DB|\\1 $DEPLOY_DIR\/db|g" $DEPLOY_DIR/docker-compose.yml
  sed -i.bak "s|\(.*\)# PATH_CONF|\\1 $DEPLOY_DIR\/conf|g" $DEPLOY_DIR/docker-compose.yml
  sed -i.bak "s|\(.*\)# PATH_PUBLIC|\\1 $DEPLOY_DIR\/data/public|g" $DEPLOY_DIR/docker-compose.yml
  sed -i.bak "s|\(.*\)# PATH_CACHE|\\1 $DEPLOY_DIR\/data/cache|g" $DEPLOY_DIR/docker-compose.yml
  sed -i.bak "s|\(.*\)# PATH_TEMP|\\1 $DEPLOY_DIR\/tmp|g" $DEPLOY_DIR/docker-compose.yml
  sed -i.bak "s|\(.*\)# PATH_USER|\\1 $DEPLOY_DIR\/user|g" $DEPLOY_DIR/docker-compose.yml

  if [[ $DEVEL -eq 1 ]]
  then
    sed -i.bak "s/\(.*\)# DEBUG-wps-entrypoint /\\1/g" $DEPLOY_DIR/docker-compose.yml

    sed -i.bak "s/\(.*\)# DEBUG-celery-entrypoint /\\1/g" $DEPLOY_DIR/docker-compose.yml
  fi

  ln -sf $DEPLOY_DIR/docker-compose.yml docker-compose.yml
}

parse_inputs $@

if [[ $PARSE -eq 0 ]]
then
  case ${COMMAND,,} in
    start)
      [[ $CLEAN -eq 1 ]] && clean

      if [[ ! -e $DEPLOY_DIR ]]
      then
        configuration
      fi

      docker-compose up -d
      ;;  
    stop)
      docker-compose down -v

      if [[ $CLEAN -eq 1 ]]
      then
        clean
      fi
      ;;
    update)
      docker-compose down -v

      CLEAN=1

      configuration

      docker-compose up -d
      ;;
    config)
      CLEAN=1

      configuration
      ;;
  esac
fi
