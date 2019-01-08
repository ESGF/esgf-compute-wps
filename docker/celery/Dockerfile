FROM jasonb87/cwt_common:2.1.0

RUN mkdir -p /tmp/certs && \
	curl -sL https://github.com/ESGF/esgf-dist/raw/master/installer/certs/esg_trusted_certificates.tar | tar xvf - -C /tmp/certs --strip 1

WORKDIR /

COPY docker/celery/entrypoint.sh .

EXPOSE 4356 4357 8000

ENTRYPOINT ["./entrypoint.sh"]

CMD ["-l", "INFO"]
