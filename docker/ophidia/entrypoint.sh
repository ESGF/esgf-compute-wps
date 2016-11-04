#!/bin/bash

pass="abcd"

base="/root/miniconda2"
cert="."

root_key="$cert/rootkey.pem"
root_req="$cert/rootreq.pem"
root_cert="$cert/rootcert.pem"
ca_cert="$cert/cacert.pem"

srv_key="$cert/serverkey.pem"
srv_req="$cert/serverreq.pem"
srv_cert="$cert/servercert.pem"
my_srv="$cert/myserver.pem"

ssh-keygen -t dsa -f ~/.ssh/id_dsa -N ""

cat ~/.ssh/id_dsa.pub >> ~/.ssh/authorized_keys

chmod 0600 ~/.ssh/authorized_keys

openssl req -newkey rsa:1024 -passout pass:$pass -subj "/" -sha1 -keyout \
  $root_key -out $root_req

openssl x509 -req -in $root_req -passin pass:$pass -sha1 -extensions v3_ca \
  -signkey $root_key -out $root_cert

cat $root_cert $root_key > $ca_cert

openssl req -newkey rsa:1024 -passout pass:$pass -subj "/" -sha1 -keyout \
  $srv_key -out $srv_req

openssl x509 -req -in $srv_req -passin pass:$pass -sha1 -extensions usr_cert \
  -CA $ca_cert -CAkey $ca_cert -CAcreateserial -out $srv_cert

cat $srv_cert $srv_key $root_cert > $my_srv

cert_base="$base/etc/cert"

mkdir -p $cert_base

cp $ca_cert $my_srv $cert_base

sed -ibak "s/\(MAPPERDB_PWD\)=.*/\1=abcd/g" /root/miniconda2/etc/oph_configuration
sed -ibak "s/\(MAPPERDB_PWD\)=.*/\1=abcd/g" /root/miniconda2/etc/oph_dim_configuration
sed -ibak "s/\(SUBM_USER_PUBLK\)=.*/\1=\/root\/\.ssh\/id_dsa\.pub/g" /root/miniconda2/etc/server.conf
sed -ibak "s/\(SUBM_USER_PRIVK\)=.*/\1=\/root\/\.ssh\/id_dsa/g" /root/miniconda2/etc/server.conf

service mysqld start

mysqladmin -h 127.0.0.1 -u root password $pass

mysql -h 127.0.0.1 -u root --password=abcd mysql < "$base/etc/create_func.nomatheval.sql"
#mysql -h 127.0.0.1 -u root --password=abcd
mysql -h 127.0.0.1 -u root --password=abcd mysql -e "create database ophidiadb;"
mysql -h 127.0.0.1 -u root --password=abcd mysql -e "create database oph_dimensions;"
mysql -h 127.0.0.1 -u root --password=abcd ophidiadb < "$base/etc/ophidiadb.sql"
mysql -h 127.0.0.1 -u root --password=abcd ophidiadb -e "INSERT INTO host (hostname, cores, memory) VALUES ('127.0.0.1', 4, 1);"
mysql -h 127.0.0.1 -u root --password=abcd ophidiadb -e "INSERT INTO dbmsinstance (idhost, login, password, port) VALUES (1, 'root', 'abcd', 3306);"
mysql -h 127.0.0.1 -u root --password=abcd ophidiadb -e "INSERT INTO hostpartition (partitionname) VALUES ('test');"
mysql -h 127.0.0.1 -u root --password=abcd ophidiadb -e "INSERT INTO hashost VALUES (1, 1);"

service sshd start

exec "$@"
