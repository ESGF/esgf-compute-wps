#!/bin/bash

data="/data"
thredds="content/thredds"
catalog="$thredds/catalog.xml"

if [[ ! -e "$data" ]]
then
  mkdir -p "$data"
fi

if [[ ! -e "$catalog" ]]
then
  mkdir -p "$thredds" 

cat << EOF > content/thredds/catalog.xml
<?xml version="1.0" ?>
<catalog xmlns="http://www.unidata.ucar.edu/namespaces/thredds/InvCatalog/v1.0" >
  <service name="odap" serviceType="OpenDAP" base="/thredds/dodsC/" />
  <datasetScan name="Default" path="test" location="/data/" >
    <serviceName>odap</serviceName>
  </datasetScan >
</catalog>
EOF
fi

exec "$@"
