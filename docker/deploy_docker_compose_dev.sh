#! /bin/bash

GREEN='\033[0;32m'
CLEAR='\033[0m'

function usage {
  echo -e "Usage $0:"
  echo -e "  --id       OAuth2 Client ID"
  echo -e "  --secret   OAuth2 Client Secret"
}

while [[ $# -gt 0 ]]
do
  arg=$1
  shift

  case $arg in
    --id)
      CLIENT=$1
      shift
      ;;
    --secret)
      SECRET=$1
      shift
      ;;
    *)
      usage
      exit 1
      ;;
  esac
done

#[ -z "${CLIENT}" ] && usage && exit 1
#[ -z "${SECRET}" ] && usage && exit 1

echo -e "${GREEN}Installing docker local-persist${CLEAR}"
echo -e ""

curl -fsSL https://raw.githubusercontent.com/CWSpear/local-persist/master/scripts/install.sh | sudo bash

echo -e ""
echo -e "${GREEN}Setting up environment${CLEAR}"
echo -e ""

export OAUTH_CLIENT $CLIENT
export OATUH_SECRET $SECRET

echo -e "${GREEN}Deploying docker${CLEAR}"
echo -e ""

docker-compose -f docker-compose.dev.yml up -d

docker-compose ps

echo -e ""
echo -e "${GREEN}Cleaning up environment${CLEAR}"

unset OAUTH_CLIENT
unset OAUTH_SECRET
