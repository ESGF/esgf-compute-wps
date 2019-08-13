#!/bin/bash

# Make directory incase it is not already present
[[ ! -e "/data/public" ]] && mkdir -p /data/public

tini -- $@
