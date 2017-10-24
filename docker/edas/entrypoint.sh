#! /bin/bash

source activate edas

source edas/bin/setup_runtime.sh

cd edas

sed -ibak s/.*python.*//g ./bin/startup_edas_local.sh

exec ./bin/startup_edas_local.sh
