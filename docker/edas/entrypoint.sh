#! /bin/bash

source activate edas

source edas/bin/setup_runtime.sh

cd edas

exec ./bin/startup_edas_local.sh
