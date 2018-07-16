FROM jasonb87/cwt_common:2.1.0

ENV SUBCOMMAND 'worker'

RUN wget https://github.com/ESGF/esgf-dist/raw/master/installer/certs/esg_trusted_certificates.tar && \
	mkdir /tmp/certs && \
	tar -xvf esg_trusted_certificates.tar -C /tmp/certs --strip 1 && \
	rm esg_trusted_certificates.tar 

WORKDIR /var/www/compute/docker/celery

EXPOSE 4356 4357

ENTRYPOINT ["./entrypoint.sh"]
