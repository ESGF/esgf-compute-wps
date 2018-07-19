#! /bin/bash

source activate wps

pushd /var/www/compute/compute

exec $@
