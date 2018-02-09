#! /bin/bash

GREEN='\033[0;32m'
CLEAR='\033[0m'

function usage {
  echo "Usage: $0"
  echo ""
  echo "  --client: OAuth2 Client ID"
  echo "  --secret: OAuth2 Secret"
  echo "  --postgres: PostgreSQL password"
  echo "  --sslcert: SSL Certificate"
  echo "  --sslkey: SSL Key"
}

[[ $# -eq 0 ]] && usage && exit 1

while [[ $# -gt 0 ]]
do
  arg=$1
  shift

  case ${arg} in
    --client)
      oauth_client=$1
      shift
      ;;
    --secret)
      oauth_secret=$1
      shift
      ;;
    --postgres)
      postgres_pass=$1
      shift
      ;;
    --sslcert)
      ssl_cert=$1
      shift
      ;;
    --sslkey)
      ssl_key=$1
      shift
      ;;
    *)
      usage
      exit 1
      ;;
  esac
done

[[ $(command -v kubectl 2>&1) ]] || (echo "Kubectl command cannot be found, please check that it is installed." && exit 1)
[ -z "${oauth_client}" ] && echo "Missing required --client argument" && exit 1
[ -z "${oauth_secret}" ] && echo "Missing required --secret argument" && exit 1
[ -z "${postgres_pass}" ] && echo "Missing required --postgres argument" && exit 1
[ -z "${ssl_cert}" ] && echo "Missing required --sslcert argument" && exit 1
[ -z "${ssl_key}" ] && echo "Missing required --sslkey argument" && exit 1

echo -e "${GREEN}Generating kubectl configs and secrets${CLEAR}"

kubectl create configmap app-config --from-env-file=wps/app.properties
kubectl create configmap django-config --from-file=wps/django.properties
kubectl create configmap traefik-config --from-file=kubernetes/traefik.toml
kubectl create secret generic ssl-secret --from-file=${ssl_cert} --from-file=${ssl_key}
kubectl create secret generic app-secret --from-literal=oauth_client=${oauth_client} --from-literal=oauth_secret=${oauth_secret} --from-literal=postgres_password=${postgres_pass}

kubectl create --filename=kubernetes/
