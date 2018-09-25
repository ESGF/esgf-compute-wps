#! /bin/bash

cd /edas/

source activate edas

source bin/setup_runtime.sh

exec bin/startup_edas_local.sh
