ARG CONDA_VERSION=4.8.2

FROM continuumio/miniconda3:${CONDA_VERSION} as builder

RUN conda update -n base conda && \
      conda install -c defaults -c conda-forge -y conda-build conda-forge-pinning curl

RUN mkdir -p /tmp/certs && \
      curl -sL https://github.com/ESGF/esgf-dist/raw/master/installer/certs/esg_trusted_certificates.tar | tar xvf - -C /tmp/certs --strip 1

WORKDIR /build

COPY compute/compute_tasks compute_tasks/

RUN conda build -c conda-forge -c cdat -m /opt/conda/conda_build_config.yaml --python 3.7 compute_tasks/

FROM continuumio/miniconda3:${CONDA_VERSION} as production

RUN mkdir /metrics

RUN apt-get update && \
      apt-get install -y --no-install-recommends dnsutils && \
      rm -rf /var/lib/apt/lists/*

COPY --from=builder /tmp/certs /tmp/certs
COPY --from=builder /opt/conda/pkgs/ /opt/conda/pkgs/
COPY --from=builder /opt/conda/conda-bld/noarch/ /opt/conda/conda-bld/noarch/

RUN conda install -c conda-forge -c cdat --use-local compute-tasks && \
      conda clean -afy

WORKDIR /

COPY compute/compute_tasks/entrypoint.sh .
COPY compute/compute_tasks/healthcheck.sh .

ENV TINI_VERSION v0.18.0
ADD https://github.com/krallin/tini/releases/download/${TINI_VERSION}/tini /tini
RUN chmod +x /tini

EXPOSE 8000

ENV prometheus_multiproc_dir /metrics

ARG CONTAINER_IMAGE
ENV CONTAINER_IMAGE $CONTAINER_IMAGE

ENTRYPOINT ["/tini", "--"]

CMD ["/bin/bash", "./entrypoint.sh", "-c", "1", "-Q", "default", "-n", "default", "-l", "INFO"]

FROM scratch as testresult

COPY --from=builder /output/unittest.xml .
COPY --from=builder /output/coverage.xml .
