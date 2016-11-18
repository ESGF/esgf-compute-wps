#!/bin/bash

base="/root/miniconda2"
passwd="abcd"

configure_mysql() {
  service mysqld start

  mysqladmin -u root password "$passwd"

  mysql -u root --password=abcd mysql < "$base/etc/create_func.sql"
  mysql -u root --password=abcd mysql -e "create database ophidiadb;"
  mysql -u root --password=abcd mysql -e "create database oph_dimensions;"
  mysql -u root --password=abcd ophidiadb < "$base/etc/ophidiadb.sql"
  mysql -u root --password=abcd ophidiadb -e "INSERT INTO host (hostname, cores, memory) VALUES ('127.0.0.1',1,1);"
  mysql -u root --password=abcd ophidiadb -e "INSERT INTO dbmsinstance (idhost, login, password, port) VALUES (1, 'root', '$passwd', 3306);"
  mysql -u root --password=abcd ophidiadb -e "INSERT INTO hostpartition (partitionname) VALUES ('test');"
  mysql -u root --password=abcd ophidiadb -e "INSERT INTO hashost (idhostpartition, idhost) VALUES (1,1);"
}

configure_framework() {
cat << EOF > "$base/etc/oph_configuration"
MAPPER_DB_NAME=ophidiadb
MAPPERDB_HOST=127.0.0.1
MAPPERDB_PORT=3306
MAPPERDB_LOGIN=root
MAPPERDB_PWD=$passwd
WEB_SERVER=http://127.0.0.1/site    ← Use *protocol://hostname/subfolders*
WEB_SERVER_LOCATION=/var/www/html/site
MEMORY=2048
BASE_SRC_PATH=/data/repository
EOF

cat << EOF > "$base/etc/oph_dim_configuration"
MAPPER_DB_NAME=oph_dimensions
MAPPERDB_HOST=127.0.0.1
MAPPERDB_PORT=3306
MAPPERDB_LOGIN=root
MAPPERDB_PWD=$passwd
EOF

cat << EOF > "$base/etc/oph_soap_configuration"
SOAP_HOST=127.0.0.1
SOAP_PORT=11372
SOAP_USERNAME=framework
SOAP_PASSWORD=$passwd
SOAP_TIMEOUT=60
SOAP_RECV_TIMEOUT=3600
EOF
}

configure_server() {
cat << EOF > "$base/etc/ophidiadb.conf"
MAPPER_DB_NAME=ophidiadb
MAPPERDB_HOST=127.0.0.1
MAPPERDB_PORT=3306
MAPPERDB_LOGIN=root
MAPPERDB_PWD=$passwd
EOF

cat << EOF > "$base/etc/rmanager"
NAME=Slurm
SUBM_CMD=srun
SUBM_ARGS=--mpi=pmi2 --input=none
SUBM_NCORES=-n
SUBM_INTERACT=
SUBM_BATCH=
SUBM_STDOUTPUT=-o
SUBM_STDERROR=-e
SUBM_POSTFIX=>/dev/null
SUBM_JOBNAME=-J
SUBM_CANCEL=scancel -n
SUBM_JOBCHECK=squeue -o "%j" | grep oph
EOF

cat << EOF > "$base/etc/server.conf"
TIMEOUT=3600
INACTIVITY_TIMEOUT=31536000
WORKFLOW_TIMEOUT=86400
LOGFILE=/root/miniconda2/log/server.log
CERT=/root/miniconda2/etc/cert/myserver.pem
CA=/root/miniconda2/etc/cert/cacert.pem
CERT_PASSWORD=$passwd
RMANAGER_CONF_FILE=/root/miniconda2/etc/rmanager.conf
AUTHZ_DIR=/root/miniconda2/authz
TXT_DIR=/root/miniconda2/txt
WEB_SERVER=http://server.hostname/ophidia
WEB_SERVER_LOCATION=/var/www/html/ophidia
OPERATOR_CLIENT=/root/miniconda2/bin/oph_analytics_framework
IP_TARGET_HOST=127.0.0.1
SUBM_USER=root
SUBM_USER_PUBLK=/root/.ssh/id_dsa.pub
SUBM_USER_PRIVK=/root/.ssh/id_dsa
OPH_XML_URL=http://127.0.0.1/ophidia/operators_xml
OPH_XML_DIR=/root/miniconda2/etc/operators_xml
NOTIFIER=framework
SERVER_FARM_SIZE=16
QUEUE_SIZE=0
HOST=127.0.0.1
PORT=11732
PROTOCOL=https
AUTO_RETRY=3
POLL_TIME=60
BASE_SRC_PATH=/data/repository
BASE_BACKOFF=1
EOF
}

configure_mysql

configure_framework

configure_server

exec "$@"
