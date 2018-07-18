#! /bin/bash

THREDDS=""

function usage {
  echo -e "Usage: $0 [GIT_TAG] [DOCKER_TAG] [--thredds VERSION]"
}

[[ $# -lt 2 ]] && usage && exit 1

GIT_TAG=$1 && shift

DOCKER_TAG=$1 && shift

while [[ $# -gt 0 ]]
do
  ARG=$1

  shift

  case $ARG in
    --thredds)
      THREDDS=$1 && shift
      ;;
    --help|-h)
      usage && exit 1
      ;;
    *)
      usage && exit 1
  esac
done

docker build -t jasonb87/cwt_common:$DOCKER_TAG -f docker/common/Dockerfile .

docker build -t jasonb87/cwt_wps:$DOCKER_TAG -f docker/wps/Dockerfile .

docker build -t jasonb87/cwt_celery:$DOCKER_TAG -f docker/celery/Dockerfile .

docker push jasonb87/cwt_common:$DOCKER_TAG

docker push jasonb87/cwt_wps:$DOCKER_TAG

docker push jasonb87/cwt_celery:$DOCKER_TAG

if [[ ! -z "$THREDDS" ]]
then
  docker build -t jasonb87/cwt_thredds:$THREDDS --build-arg TAG=$THREDDS thredds/

  docker push jasonb87/cwt_thredds:$THREDDS
fi
