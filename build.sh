#! /bin/sh

DOCKERFILE_PATH=${1}
TARGET=${2}
shift
shift
EXTRA=$@

buildctl-daemonless.sh \
  build \
  --frontend dockerfile.v0 \
  --local context=. \
  --local dockerfile=./${DOCKERFILE_PATH} \
  --opt target=${TARGET} \
  ${EXTRA}
