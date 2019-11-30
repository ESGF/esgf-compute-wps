FROM continuumio/miniconda3:4.7.10 as builder

RUN conda install -y conda-build

WORKDIR /src

COPY compute/compute_provisioner compute_provisioner/
COPY compute/compute_tasks compute_tasks/

ARG COMPUTE_API_VER

ENV COMPUTE_API_VER ${COMPUTE_API_VER}

RUN conda build -c conda-forge compute_provisioner/ && \
      conda build -c conda-forge -c cdat --use-local compute_tasks/

FROM continuumio/miniconda3:4.7.10 as production

RUN conda install -c conda-forge curl && \
      mkdir -p /tmp/certs && \
      curl -sL https://github.com/ESGF/esgf-dist/raw/master/installer/certs/esg_trusted_certificates.tar | tar xvf - -C /tmp/certs --strip 1

COPY --from=builder /opt/conda/pkgs/ /opt/conda/pkgs/
COPY --from=builder /opt/conda/conda-bld/noarch/ /opt/conda/conda-bld/noarch/

RUN conda install -c conda-forge -c cdat --use-local compute-tasks && \
      conda clean -a -y

ENV TINI_VERSION v0.18.0

RUN curl -fSLk https://github.com/krallin/tini/releases/download/${TINI_VERSION}/tini -o /usr/local/bin/tini && \
      chmod +x /usr/local/bin/tini

RUN apt-get update && apt-get install -y --no-install-recommends dnsutils && \
      rm -rf /var/lib/apt/lists/*

WORKDIR /

COPY compute/compute_tasks/entrypoint.sh .
COPY compute/compute_tasks/healthcheck.sh .

ARG GIT_SHORT_COMMIT
ARG COMPUTE_API_VER

ENV CONTAINER_VER ${GIT_SHORT_COMMIT}
ENV COMPUTE_API_VER ${COMPUTE_API_VER}
ENV prometheus_multiproc_dir /metrics

EXPOSE 8000

ENTRYPOINT ["/usr/local/bin/tini", "--"]

CMD ["/bin/bash", "./entrypoint.sh", "-c", "1", "-Q", "default", "-n", "default", "-l", "INFO"]

FROM production as testing

WORKDIR /testing

RUN conda install -c conda-forge pytest pytest-mock pytest-cov myproxyclient pydap && \
      mkdir /metrics

COPY compute/compute_tasks .

RUN pytest -vvv --junit-xml=unittesting.xml --cov=compute_tasks --cov-report=html:coverage/ || exit 0
