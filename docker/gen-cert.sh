#! /bin/bash

if [[ $# -eq 0 ]]
then
  echo "Usage: $0 <identifier> <masterip> <masterclusterip> <dnsname>"

  exit 1
fi

cp csr.conf ${1}-csr.conf

sed -i.bak s/\<masterip\>/$2/ ${1}-csr.conf
sed -i.bak s/\<masterclusterip\>/$3/ ${1}-csr.conf
sed -i.bak s/\<dnsname\>/$4/ ${1}-csr.conf

openssl genrsa -out ${1}.key 2048

openssl req -new -key ${1}.key -out ${1}.csr -config ${1}-csr.conf

openssl x509 -req -in ${1}.csr -CA ca.crt -CAkey ca.key -CAcreateserial -out ${1}.crt -days 10000 -extensions v3_ext -extfile ${1}-csr.conf

openssl rsa -in ${1}.key -pubout -out ${1}Pub.key
