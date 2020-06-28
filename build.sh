#! /bin/sh

set -x

DOCKERFILE_PATH="${1}"
TARGET="${2}"
shift
shift
EXTRA=$*

ls -la test_data/

tar c test_data/ | (cd "${DOCKERFILE_PATH}"; tar x)

cd "${DOCKERFILE_PATH}"

ls -la test_data/

buildctl-daemonless.sh \
  build \
  --frontend dockerfile.v0 \
  --local context="${PWD}" \
  --local dockerfile="${PWD}" \
  --opt target="${TARGET}" \
  "${EXTRA}"
