#! /bin/bash

set -e

source bin/setup_runtime.sh

export CDAS_JAR=${CDAS_HOME_DIR}/target/scala-2.10/cdas2_2.10-1.2.2-SNAPSHOT.jar
export CONDA_LIB=${CONDA_PREFIX}/lib
export APP_DEP_JARS=$(find ~/.ivy2 -name "*.jar" )
export APP_DEP_CP=$(echo $APP_DEP_JARS | sed -e "s/ /:/g" ):${CONDA_LIB}

java -cp $APP_DEP_CP:$CDAS_JAR nasa.nccs.cdas.portal.CDASApplication bind $CDAS_REQUEST_PORT $CDAS_RESPONSE_PORT ~/.cdas/cache/cdas.properties

exec "$@"
