#! /bin/bash

openssl req -newkey rsa:2048 -nodes -keyout tls.key -subj "/CN=ca" -x509 -days 365 -out tls.crt
