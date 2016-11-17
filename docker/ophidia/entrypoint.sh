#!/bin/bash

passwd="abcd"
base="/root/miniconda2"

configure_mysql() {
  service mysqld start

  mysqladmin -u root password "$passwd"

  #mysql -u root --password=abcd mysql
  mysql -u root --password=abcd mysql < "$base/etc/create_func.sql"
  mysql -u root --password=abcd mysql -e "create database ophidiadb;"
  mysql -u root --password=abcd mysql -e "create database oph_dimensions;"
}

exec "$@"
