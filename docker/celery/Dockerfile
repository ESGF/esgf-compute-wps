FROM jasonb87/cwt_common:latest

ENV C_FORCE_ROOT 'true'
ENV CELERY_BROKER redis://redis/0
ENV CELERY_BACKED redis://redis/0

RUN conda install -c conda-forge celery dask django && \
	conda clean --all -y

RUN wget https://github.com/ESGF/esgf-dist/raw/master/installer/certs/esg_trusted_certificates.tar && \
	mkdir /tmp/certs && \
	tar -xvf esg_trusted_certificates.tar -C /tmp/certs --strip 1 && \
	rm esg_trusted_certificates.tar 

RUN pip install django-webpack-loader

WORKDIR /

COPY entrypoint.sh entrypoint.sh

COPY healthcheck.sh healthcheck.sh

EXPOSE 4356 4357

HEALTHCHECK CMD ./healthcheck.sh

ENTRYPOINT ["./entrypoint.sh"]

CMD ["-l", "info"]
