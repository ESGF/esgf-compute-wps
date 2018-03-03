#! /bin/bash

DEV=0
OAUTH_CLIENT=''
OAUTH_SECRET=''
POSTGRES_PASSWORD='abcd'
TLS_KEY=''
TLS_CRT=''
WPS_HOST='0.0.0.0'

function usage {
  echo -e "Usage $0:"
  echo -e ""
  echo -e "  --dev\t\t\tEnable development mode"
  echo -e "  --client\t\tOAuth2 Client ID"
  echo -e "  --secret\t\tOAuth2 Secret"
  echo -e "  --postgres-pass\tPostgres password"
  echo -e "  --tls-key\t\tTLS Key"
  echo -e "  --tls-crt\t\tTLS Certificate"
  echo -e "  --host\t\tExternal host, for django ALLOW_HOSTS"
}

[[ $# -eq 0 ]] && usage && exit 0

while [[ $# -gt 0 ]]
do
  arg=$1
  shift

  case $arg in
  --dev)
    DEV=1
    ;;
  --client)
    OAUTH_CLIENT="$1"
    shift
    ;;
  --secret)
    OAUTH_SECRET="$1"
    shift
    ;;
  --postgres-pass)
    POSTGRES_PASSWORD="$1"
    shift
    ;;
  --tls-key)
    TLS_KEY="$1"
    shift
    ;;
  --tls-crt)
    TLS_CRT="$1"
    shift
    ;;
  --host)
    WPS_HOST="$1"
    shift
    ;;
  --help|-h|*)
    usage
    exit 0
  esac
done
