#! /bin/sh

set -x

DOCKERFILE_PATH=${1}
TARGET=${2}
shift
shift
EXTRA=$@

cd ${DOCKERFILE_PATH}

buildctl-daemonless.sh \
  build \
  --frontend dockerfile.v0 \
  --local context=${PWD} \
  --local dockerfile=${PWD} \
  --opt target=${TARGET} \
  ${EXTRA}
