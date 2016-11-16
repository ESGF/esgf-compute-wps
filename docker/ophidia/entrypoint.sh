#!/bin/bash

base="/root/miniconda2"

passwd="abcd"

hostname=$(hostname)

function mysql_setup {
  service mysqld start

  /usr/bin/mysqladmin -u root password "$passwd"

  mysql -u root --password=abcd mysql < "$base/etc/create_func.sql"
  mysql -u root --password=abcd mysql -e "create database ophidiadb;"
  mysql -u root --password=abcd mysql -e "create database oph_dimensions;"
  mysql -u root --password=abcd ophidiadb < "$base/etc/ophidiadb.sql"
  mysql -u root --password=abcd ophidiadb -e "INSERT INTO host (hostname, cores, memory) VALUES ('127.0.0.1', 4, 1);"
  mysql -u root --password=abcd ophidiadb -e "INSERT INTO dbmsinstance (idhost, login, password, port) VALUES (1, 'root', '$passwd', 3306);"
  mysql -u root --password=abcd ophidiadb -e "INSERT INTO hostpartition (partitionname) VALUES ('test');"
  mysql -u root --password=abcd ophidiadb -e "INSERT INTO hashost (idhostpartition, idhost) VALUES (1, 1);"

cat << EOF > "$base/etc/ophidiadb.conf"
MAPPER_DB_NAME=ophidiadb
MAPPERDB_HOST=$hostname
MAPPERDB_PORT=3306
MAPPERDB_LOGIN=root
MAPPERDB_PWD=$passwd
EOF
}

function create_dir {
  if [[ ! -e "$1" ]]
  then
    mkdir -p "$1"
  fi
}

function framework_configs {
cat << EOF > "$base/etc/oph_configuration"
MAPPER_DB_NAME=ophidiadb
MAPPERDB_HOST=$hostname
MAPPERDB_PORT=3306
MAPPERDB_LOGIN=root
MAPPERDB_PWD=$passwd
WEB_SERVER=http://$hostname/site    ← Use *protocol://hostname/subfolders*
WEB_SERVER_LOCATION=/var/www/html/site
MEMORY=2048
BASE_SRC_PATH=/data/repository
EOF

cat << EOF > "$base/etc/oph_dim_configuration"
MAPPER_DB_NAME=oph_dimensions
MAPPERDB_HOST=$hostname
MAPPERDB_PORT=3306
MAPPERDB_LOGIN=root
MAPPERDB_PWD=$passwd
EOF

cat << EOF > "$base/etc/oph_soap_configuration"
MAPPER_DB_NAME=oph_dimensions
MAPPERDB_HOST=$hostname
MAPPERDB_PORT=3306
MAPPERDB_LOGIN=root
MAPPERDB_PWD=$passwd
EOF
}

function server_configs {
  openssl req -newkey rsa:1024 -passout pass:$passwd -subj "/" -sha1 \
    -keyout rootkey.pem -out rootreq.pem

  openssl x509 -req -in rootreq.pem -passin pass:$passwd -sha1 \
    -extensions v3_ca -signkey rootkey.pem -out rootcert.pem

  cat rootcert.pem rootkey.pem > cacert.pem

  openssl req -newkey rsa:1024 -passout pass:$passwd -subj "/" -sha1 \
    -keyout serverkey.pem -out serverreq.pem

  openssl x509 -req -in serverreq.pem -passin pass:$passwd -sha1 \
    -extensions usr_cert -CA cacert.pem -CAkey cacert.pem -CAcreateserial \
    -out servercert.pem

  cat servercert.pem serverkey.pem rootcert.pem > myserver.pem

  create_dir "$base/etc/cert"

  cp myserver.pem cacert.pem "$base/etc/cert"

cat << EOF > "$base/etc/server.conf"
TIMEOUT=3600
INACTIVITY_TIMEOUT=31536000
WORKFLOW_TIMEOUT=86400
LOGFILE=$base/log/server.log
CERT=$base/etc/cert/myserver.pem
CA=$base/etc/cert/cacert.pem
CERT_PASSWORD=abcd
RMANAGER_CONF_FILE=$base/etc/rmanager.conf
AUTHZ_DIR=$base/authz
TXT_DIR=$base/txt
WEB_SERVER=http://$hostname/ophidia
WEB_SERVER_LOCATION=/var/www/html/ophidia
OPERATOR_CLIENT=$base/bin/oph_analytics_framework
IP_TARGET_HOST=$hostname
SUBM_USER=ophidia
SUBM_USER_PUBLK=/root/.ssh/id_dsa.pub
SUBM_USER_PRIVK=/root/.ssh/id_dsa
OPH_XML_URL=http://$hostname/ophidia/operators_xml
OPH_XML_DIR=$base/etc/operators_xml
NOTIFIER=framework
SERVER_FARM_SIZE=16
QUEUE_SIZE=0
HOST=$hostname
PORT=11732
PROTOCOL=https
AUTO_RETRY=3
POLL_TIME=60
BASE_SRC_PATH=/data/repository
BASE_BACKOFF=1
EOF

  srv_conf=$(oph_server 2>&1 | grep -e "configuration" | cut -d' ' -f8 | tr -d "'")

  mkdir -p $(dirname $srv_conf)

  ln -sf "$base/etc/server.conf" "$srv_conf"
}

create_dir "$base/log"
create_dir "$base/txt"
create_dir "$base/authz/sessions"

framework_configs

server_configs

mysql_setup

exec "$@"
