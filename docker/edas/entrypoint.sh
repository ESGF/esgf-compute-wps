#! /bin/bash

cd /edas/

source activate edas

source bin/setup_runtime.sh

exec sbt "run 5670 5671 /root/.edas/cache/edas.properties"
