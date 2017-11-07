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
  echo -e "\t--test: \tRuns E2E test"
}

if [[ $# -eq 0 ]]
then
  usage
  exit
fi

while [[ $# -gt 0 ]]
do
  key="$1"

  shift

  case $key in
    --help|-h)
      usage
      exit
      ;;
    --test)
      TEST=1
      ;;
    --all)
      WPS=1
      CELERY=1
      THREDDS=1
      EDAS=1
      ;;
    --wps)
      WPS=1
      ;;
    --celery)
      CELERY=1
      ;;
    --thredds)
      THREDDS=1
      ;;
    --edas)
      EDAS=1
      if [[ "$1" == "--tag" ]]
      then
        shift
        EDAS_TAG="--build-arg TAG=$1"
        shift
      else
        EDAS_TAG=""
      fi
      ;;
    --push)
      PUSH=1
      ;;
    --no-cache)
      NOCACHE="--no-cache"
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
  echo $*

  docker build ${NOCACHE} -t ${repo}/${1}:latest -f ${2}/Dockerfile ${3} ${2}

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
  echo "Building EDAS image $EDAS_TAG"

  build cwt_edas ../docker/edas "$EDAS_TAG"
fi

if [[ -n $TEST ]]
then
  echo "Testing images"

  docker-compose -f ../docker/docker-compose.yml down -v --remove-orphans

  docker-compose -f ../docker/docker-compose.yml up -d

  docker-compose -f ../docker/docker-compose.yml ps
fi
