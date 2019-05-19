FROM continuumio/miniconda3:4.5.12 as builder

WORKDIR /src

COPY . .

WORKDIR /src/compute

RUN conda install -c conda-forge -y conda-build && \
      conda build -c conda-forge -c cdat compute_kubernetes/ && \
      conda build -c cdat/label/nightly -c cdat -c conda-forge --use-local compute_tasks/

FROM continuumio/miniconda3:4.5.12

ENV prometheus_multiproc_dir /metrics

COPY --from=builder /opt/conda/conda-bld/noarch/* /opt/conda/conda-bld/noarch/

RUN mkdir -p /tmp/certs && \
      curl -sL https://github.com/ESGF/esgf-dist/raw/master/installer/certs/esg_trusted_certificates.tar | tar xvf - -C /tmp/certs --strip 1

RUN conda install -c cdat/label/nightly -c cdat -c conda-forge --use-local compute-tasks

WORKDIR /

COPY docker/celery/entrypoint.sh .

COPY docker/celery/healthcheck.sh .

EXPOSE 8000

ENTRYPOINT ["./entrypoint.sh"]

CMD ["-c", "1", "-Q", "default", "-n", "default", "-l", "INFO"]
