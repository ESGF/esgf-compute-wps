FROM jasonb87/cwt_common:latest

WORKDIR /

COPY entrypoint.sh entrypoint.sh

WORKDIR /var/www/compute/compute

EXPOSE 8786

ENTRYPOINT ["/entrypoint.sh"]
