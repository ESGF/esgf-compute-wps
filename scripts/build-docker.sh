#! /bin/bash

function usage {
  echo -e "Available arguments:"
  echo -e "\t--wps \t\tBuilds WPS image"
  echo -e "\t--celery: \tBuilds Celery image"
  echo -e "\t--thredds: \tBuilds Thredds image"
  echo -e "\t--cdas: \tBuilds CDAS2 image"
  echo -e "\t--push: \tPush built images"
  echo -e "\t--no-cache: \tBuild without using cached images"
}

if [[ $# -eq 0 ]]
then
  usage

  exit
fi

while [[ $# -gt 0 ]]
do
  key="$1"

  case $key in
    --wps)
      WPS=1
      shift
      ;;
    --celery)
      CELERY=1
      shift
      ;;
    --thredds)
      THREDDS=1
      shift
      ;;
    --cdas)
      CDAS=1
      shift
      ;;
    --push)
      PUSH=1
      shift
      ;;
    --no-cache)
      NOCACHE="--no-cache"
      shift
      ;;
  esac
done

repo="jasonb87"

echo "Building images and pushing to repo ${repo}"

docker -v

docker build ${NOCACHE} -t ${repo}/cwt_common:latest -f ../docker/common/Dockerfile ../docker/common/

if [[ -n $PUSH ]]
then
    docker push ${repo}/cwt_common:latest 
fi

if [[ -n $WPS ]]
then
  echo "Building WPS image"

  docker build ${NOCACHE} -t ${repo}/cwt_wps:latest -f ../docker/wps/Dockerfile ../docker/wps/

  if [[ -n $PUSH ]]
  then
      docker push ${repo}/cwt_wps:latest 
  fi
fi

if [[ -n $CELERY ]]
then
  echo "Building Celery image"

  docker build ${NOCACHE} -t ${repo}/cwt_celery:latest -f ../docker/celery/Dockerfile ../docker/celery/
  
  if [[ -n $PUSH ]]
  then
      docker push ${repo}/cwt_celery:latest 
  fi
fi

if [[ -n $THREDDS ]]
then
  echo "Building Thredds image"

  docker build ${NOCACHE} -t ${repo}/cwt_thredds:latest -f ../docker/thredds/Dockerfile ../docker/thredds/
  
  if [[ -n $PUSH ]]
  then
      docker push ${repo}/cwt_thredds:latest 
  fi
fi

if [[ -n $CDAS ]]
then
  echo "Building CDAS image"

  docker build ${NOCACHE} -t ${repo}/cwt_cdas:latest -f ../docker/cdas2/Dockerfile ../docker/cdas2/
  
  if [[ -n $PUSH ]]
  then
      docker push ${repo}/cwt_cdas:latest 
  fi
fi
