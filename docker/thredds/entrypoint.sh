#!/bin/bash

data=/data
public=${data}/public
cache=${data}/cache
thredds="content/thredds"

if [[ ! -e "$public" ]]
then
  mkdir -p $public
fi

if [[ ! -e "$cache" ]]
then
  mkdir -p $cache
fi

if [[ ! -e "$thredds" ]]
then
  mkdir -p $thredds

  cp -f catalog.xml $thredds
fi

if [[ ! -e "./webapps/threddsCWT" ]]
then
  ./bin/catalina.sh start && sleep 10 && ./bin/catalina.sh stop

  sed -ibak s/\<param\-value\>thredds/\<param\-value\>threddsCWT/ ./webapps/threddsCWT/WEB-INF/web.xml
fi

exec sh -c "./bin/catalina.sh run $@"
