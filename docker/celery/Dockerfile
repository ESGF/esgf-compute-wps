FROM jasonb87/cwt_common:2.0.1

ENV C_FORCE_ROOT 'true'

RUN wget https://github.com/ESGF/esgf-dist/raw/master/installer/certs/esg_trusted_certificates.tar && \
	mkdir /tmp/certs && \
	tar -xvf esg_trusted_certificates.tar -C /tmp/certs --strip 1 && \
	rm esg_trusted_certificates.tar 

WORKDIR /

COPY entrypoint.sh entrypoint.sh

COPY healthcheck.sh healthcheck.sh

EXPOSE 4356 4357

HEALTHCHECK CMD ./healthcheck.sh

ENTRYPOINT ["./entrypoint.sh"]

CMD ["-l", "info"]
