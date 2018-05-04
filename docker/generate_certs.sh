#! /bin/bash

mkdir -p $DEPLOY_DIR/ssl

openssl genrsa -out $DEPLOY_DIR/ssl/ca.key 4096

openssl req -x509 -new -nodes -key $DEPLOY_DIR/ssl/ca.key -subj "/C=US/ST=CA/L=Livermore/O=CWT/CN=CWT Root CA" -sha256 -days 1024 -out $DEPLOY_DIR/ssl/ca.crt

openssl genrsa -out $DEPLOY_DIR/ssl/tls.key 2048

openssl req -new -key $DEPLOY_DIR/ssl/tls.key -subj "/C=US/ST=CA/L=Livermore/O=CWT/CN=$HOST" -out $DEPLOY_DIR/ssl/tls.csr

openssl x509 -req -in $DEPLOY_DIR/ssl/tls.csr -CA $DEPLOY_DIR/ssl/ca.crt -CAkey $DEPLOY_DIR/ssl/ca.key -CAcreateserial -out $DEPLOY_DIR/ssl/tls.crt -days 500 -sha256
