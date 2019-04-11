FROM jasonb87/cwt_wps:devel

RUN mkdir -p /tmp/certs && \
	curl -sL https://github.com/ESGF/esgf-dist/raw/master/installer/certs/esg_trusted_certificates.tar | tar xvf - -C /tmp/certs --strip 1 && \
  conda install -c cdat/label/unstable -c cdat/label/nightly -c conda-forge \
  cdms2 \
  libcdms \
  libdrs \
  libdrs_f \
  cdtime \
  dask distributed python-kubernetes xarray netcdf4

WORKDIR /

COPY docker/celery/entrypoint.sh .

COPY docker/celery/healthcheck.sh .

EXPOSE 4356 4357 8000

ENTRYPOINT ["./entrypoint.sh"]

CMD ["-l", "INFO"]
