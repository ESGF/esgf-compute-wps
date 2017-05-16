#! /bin/bash

set -e

echo 'spark.master=local' >> ${HOME}/.cdas/cache/cdas.properties

source bin/setup_runtime.sh

LD_LIBRARY_PATH=/opt/conda/lib sbt 'run bind 4356 4357'

exec "$@"
