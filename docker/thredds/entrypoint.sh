#!/bin/bash

[[ ! -e "/data/public" ]] && mkdir -p /data/public

catalina.sh start

while [[ ! -e "./webapps/threddsCWT/WEB-INF/web.xml" ]]
do
  sleep 1
done

sleep 20

catalina.sh stop

sleep 20

sed -i.bak "s/<param-value>thredds/<param-value>threddsCWT/g" ./webapps/threddsCWT/WEB-INF/web.xml

exec catalina.sh run
