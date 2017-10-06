#! /bin/bash

function usage {
  echo -e "Available arguments:"
  echo -e "\t--wps \t\tBuilds WPS image"
  echo -e "\t--celery: \tBuilds Celery image"
  echo -e "\t--thredds: \tBuilds Thredds image"
  echo -e "\t--edas: \tBuilds EDAS2 image"
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
    --edas)
      EDAS=1
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

if [[ -n $EDAS ]]
then
  echo "Building EDAS image"

  docker build ${NOCACHE} -t ${repo}/cwt_edas:latest -f ../docker/edas/Dockerfile ../docker/edas/
  
  if [[ -n $PUSH ]]
  then
      docker push ${repo}/cwt_edas:latest 
  fi
fi
