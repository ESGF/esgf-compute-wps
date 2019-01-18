#! /bin/bash

function usage {
  echo "${0} --push --build-edask --build-thredds --build-cog"
  echo ""
  echo "Builds docker images for cwt_common, cwt_wps, cwt_celery, cwt_edask,"
  echo "cwt_thredds and cwt_cog."
  echo ""
  echo "Options:"
  echo "      --push              Push docker images"
  echo "      --build-edask       Build EDASK docker image"
  echo "      --build-thredds     Build Thredds docker image"
  echo "      --build-cog         Build COG docker image"
  echo "  -h, --help              Print usage"
}

PUSH=0
BUILD_EDASK=0
BUILD_THREDDS=0
BUILD_COG=0

while [[ ${#} -gt 0 ]]; do
  NAME=${1} && shift

  case "${NAME}" in
    --push)
      PUSH=1
      ;;
    --build-edask)
      BUILD_EDASK=1
      ;;
    --build-thredds)
      BUILD_THREDDS=1
      ;;
    --build-cog)
      BUILD_COG=1
      ;;
    -h|--help|*)
      usage

      exit 0
      ;;
  esac
done

BRANCH=$(git branch | grep \* | cut -d " " -f 2)

VERSION=${BRANCH##*/}

read -p "Build images for branch \"${BRANCH}\" [ENTER]: "

CONDA_PACKAGE=$(conda search -c cdat esgf-compute-api=${VERSION} | tail -n1 | tr -s " " | cut -d " " -f 1-3 | tr -s " " "=")

sed -i "s|\(.*\)esgf-compute-api.*|\1${CONDA_PACKAGE}|" docker/common/environment.yml

docker build -t jasonb87/cwt_common:${VERSION} -f docker/common/Dockerfile .

[[ ${PUSH} -eq 1 ]] && docker push jasonb87/cwt_common:${VERSION}

sed -i "s|\(FROM jasonb87/cwt_common:\).*|\1${VERSION}|" docker/wps/Dockerfile

docker build -t jasonb87/cwt_wps:${VERSION} -f docker/wps/Dockerfile .

[[ ${PUSH} -eq 1 ]] && docker push jasonb87/cwt_wps:${VERSION}

sed -i "s|\(FROM jasonb87/cwt_common:\).*|\1${VERSION}|" docker/celery/Dockerfile

docker build -t jasonb87/cwt_celery:${VERSION} -f docker/celery/Dockerfile .

[[ ${PUSH} -eq 1 ]] && docker push jasonb87/cwt_celery:${VERSION}

if [[ ${BUILD_EDASK} -eq 1 ]]; then
  docker build -t jasonb87/cwt_edask:latest -f docker/edas/Dockerfile .

  [[ ${PUSH} -eq 1 ]] && docker push jasonb87/cwt_edask:latest
fi

if [[ ${BUILD_THREDDS} -eq 1 ]]; then
  docker build -t jasonb87/cwt_thredds:latest -f docker/thredds/Dockerfile .

  [[ ${PUSH} -eq 1 ]] && docker push jasonb87/cwt_thredds:latest
fi

if [[ ${BUILD_COG} -eq 1 ]]; then
  docker build -t jasonb87/cwt_cog:latest -f docker/cog/Dockerfile .

  [[ ${PUSH} -eq 1 ]] && docker push jasonb87/cwt_cog:latest
fi
