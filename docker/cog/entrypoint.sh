#! /bin/bash

pushd /usr/local/cog/cog_install

git fetch

git checkout cwt

popd

pushd /usr/local/cog/cog_config

sed -i 's|WPS_ENDPOINT.*|WPS_ENDPOINT = '"$4"'|g' cog_settings.cfg
sed -i 's|WPS_DATACART.*|WPS_DATACART = '"$5"'|g' cog_settings.cfg

popd

exec /usr/local/bin/docker-entrypoint.sh $1 $2 $3
