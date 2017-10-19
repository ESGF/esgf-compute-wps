#!/bin/bash

while [[ $# -gt 0 ]]
do
  case $1 in
  --proxy_name|-h)
    shift
    proxy_name=$1
    shift
    ;;
  --proxy_port|-p)
    shift
    proxy_port=$1
    shift
    ;;
  esac
done

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
  ./bin/catalina.sh start && sleep 10 && ./bin/catalina.sh stop && sleep 10

  sed -ibak s/\<param\-value\>thredds/\<param\-value\>threddsCWT/ ./webapps/threddsCWT/WEB-INF/web.xml
fi

if [[ -n $proxy_name ]]
then
  sed -ibak s/proxyName=\"0\.0\.0\.0\"/proxyName=\"${proxy_name}\"/g conf/server.xml
fi

if [[ -n $proxy_port ]]
then
  sed -ibak s/proxyPort=\"80\"/proxyPort=\"${proxy_port}\"/g conf/server.xml
fi

exec sh -c "./bin/catalina.sh run $@"
