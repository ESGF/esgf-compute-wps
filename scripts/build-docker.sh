#! /bin/bash

function usage {
  echo -e "Available arguments:"
  echo -e "\t--all \t\tBuild all images"
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
    --help|-h)
      usage
      exit
      ;;
    --all)
      WPS=1
      CELERY=1
      THREDDS=1
      EDAS=1
      shift
      ;;
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
      *)
      usage
      exit
      ;;
  esac
done

repo="jasonb87"

echo "Building images and pushing to repo ${repo}"

docker -v

function build {
  docker build ${NOCACHE} -t ${repo}/${1}:latest -f ${2}/Dockerfile ${2}

  if [[ -n $PUSH ]]
  then
    docker push ${repo}/${1}:latest
  fi
}

build cwt_common ../docker/common/

if [[ -n $WPS ]]
then
  echo "Building WPS image"

  build cwt_wps ../docker/wps
fi

if [[ -n $CELERY ]]
then
  echo "Building Celery image"

  build cwt_celery ../docker/celery
fi

if [[ -n $THREDDS ]]
then
  echo "Building Thredds image"

  build cwt_thredds ../docker/thredds
fi

if [[ -n $EDAS ]]
then
  echo "Building EDAS image"

  build cwt_edas ../docker/edas
fi
