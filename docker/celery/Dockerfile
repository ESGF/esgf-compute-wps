FROM jasonb87/cwt_wps:latest

ENV C_FORCE_ROOT 'true'
ENV CELERY_BROKER redis://redis/wps_celery
ENV CELERY_BACKED redis://redis/wps_celery

RUN wget https://github.com/ESGF/esgf-dist/raw/master/installer/certs/esg_trusted_certificates.tar && \
	mkdir /tmp/certs && \
	tar -xvf esg_trusted_certificates.tar -C /tmp/certs --strip 1 && \
	rm esg_trusted_certificates.tar 

WORKDIR /var/www/compute/compute

COPY entrypoint.sh entrypoint.sh

EXPOSE 4356 4357

ENTRYPOINT ["./entrypoint.sh"]

CMD ["celery", "-A", "compute", "-b", "$CELERY_BROKER", "worker", "-l", "info"]
