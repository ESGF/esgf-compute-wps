#! /bin/sh

set -x

WORKING_DIR="${1}"
shift

cd "${WORKING_DIR}"

buildctl-daemonless.sh \
  --debug \
  build ${@}
