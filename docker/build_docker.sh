#! /bin/bash


function usage {
  echo -e "Usage: $0 [DOCKER_TAG] [--push] [--thredds VERSION]"
}

[[ $# -lt 1 ]] && usage && exit 1

THREDDS=""
PUSH=1
DOCKER_TAG=$1 && shift

while [[ $# -gt 0 ]]
do
  ARG=$1

  shift

  case $ARG in
    --push)
      PUSH=0
      ;;
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

if [[ $PUSH -eq 0 ]]
then
  docker push jasonb87/cwt_common:$DOCKER_TAG

  docker push jasonb87/cwt_wps:$DOCKER_TAG

  docker push jasonb87/cwt_celery:$DOCKER_TAG
fi 

if [[ ! -z "$THREDDS" ]]
then
  docker build -t jasonb87/cwt_thredds:$THREDDS --build-arg TAG=$THREDDS thredds/

  docker push jasonb87/cwt_thredds:$THREDDS
fi
