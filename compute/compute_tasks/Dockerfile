FROM continuumio/miniconda3:4.7.10 as builder

RUN conda update -n base conda && \
      conda install -c defaults -c conda-forge -y conda-build conda-forge-pinning

WORKDIR /src

COPY compute/compute_tasks compute_tasks/

RUN export
RUN conda build -c conda-forge -c cdat -m /opt/conda/conda_build_config.yaml --python 3.7 compute_tasks/

FROM continuumio/miniconda3:4.7.10 as production

COPY --from=builder /opt/conda/pkgs/ /opt/conda/pkgs/
COPY --from=builder /opt/conda/conda-bld/noarch/ /opt/conda/conda-bld/noarch/

RUN conda install --yes -c conda-forge -c cdat --use-local compute-tasks curl tini && \
      conda clean -afy

RUN mkdir -p /tmp/certs && \
      curl -sL https://github.com/ESGF/esgf-dist/raw/master/installer/certs/esg_trusted_certificates.tar | tar xvf - -C /tmp/certs --strip 1

RUN apt-get update && \
      apt-get install -y --no-install-recommends dnsutils && \
      rm -rf /var/lib/apt/lists/*

WORKDIR /

COPY compute/compute_tasks/entrypoint.sh .
COPY compute/compute_tasks/healthcheck.sh .

EXPOSE 8000

ENTRYPOINT ["tini", "--"]

CMD ["/bin/bash", "./entrypoint.sh", "-c", "1", "-Q", "default", "-n", "default", "-l", "INFO"]

ARG GIT_SHORT_COMMIT

ENV CONTAINER_VER ${GIT_SHORT_COMMIT}
ENV prometheus_multiproc_dir /metrics

FROM scratch as testresult

COPY --from=builder /output/unittest.xml .
COPY --from=builder /output/coverage.xml .
