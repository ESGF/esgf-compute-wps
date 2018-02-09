#! /bin/bash

if [[ $# -eq 0 ]]
then
  echo "Usage: $0 <master_ip>"

  exit 1
fi

openssl genrsa -out ca.key 2048

openssl req -x509 -new -nodes -key ca.key -subj "/CN=$1" -days 10000 -out ca.crt
