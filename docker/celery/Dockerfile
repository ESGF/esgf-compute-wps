FROM jasonb87/cwt_wps:devel

RUN mkdir -p /tmp/certs && \
	curl -sL https://github.com/ESGF/esgf-dist/raw/master/installer/certs/esg_trusted_certificates.tar | tar xvf - -C /tmp/certs --strip 1 && \
  conda install -c cdat/label/unstable -c cdat/label/nightly -c conda-forge \
  cdms2=3.1.2.2019.03.30.00.05.g9b0dfed \
  libcdms=3.1.2.2019.02.09.03.57.g8f1cefe \
  libdrs=3.1.2.2019.02.09.01.20.g0c04b0c \
  libdrs_f=3.1.2.2019.02.09.01.20.g0c04b0c \
  cdtime=3.1.2.2019.02.09.12.03.gba9e0cb && \
  conda install -c conda-forge dask distributed python-kubernetes xarray netcdf4

WORKDIR /

COPY docker/celery/entrypoint.sh .

COPY docker/celery/healthcheck.sh .

EXPOSE 4356 4357 8000

ENTRYPOINT ["./entrypoint.sh"]

CMD ["-l", "INFO"]
