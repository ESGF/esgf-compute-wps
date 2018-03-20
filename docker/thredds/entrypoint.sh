#!/bin/bash

[[ ! -e "/data/public" ]] && mkdir -p /data/public

catalina.sh start

while [[ -z "$(grep -E 'threddsCWT.war] has finished' ./logs/catalina.out)" ]]
do
  sleep 1
done

sed -i.bak "s/<param-value>thredds/<param-value>threddsCWT/g" ./webapps/threddsCWT/WEB-INF/web.xml

cp catalog.xml ./content/thredds/catalog.xml

cp threddsConfig.xml ./content/thredds/threddsConfig.xml

tail -f logs/catalina.out
