FROM continuumio/miniconda3:4.5.12 as base

ARG COMPUTE_API_VER

ENV COMPUTE_API_VER ${COMPUTE_API_VER}

WORKDIR /src

COPY . .

WORKDIR /src/compute

RUN GIT_REV=$(git rev-parse --short HEAD) && \
      conda install -c conda-forge -y conda-build=3.18.7 && \
      echo "__version__ = '${GIT_REV}'" > compute_kubernetes/compute_kubernetes/_version.py && \
      echo "__version__ = '${GIT_REV}'" > compute_provisioner/compute_provisioner/_version.py && \
      echo "__version__ = '${GIT_REV}'" > compute_tasks/compute_tasks/_version.py

FROM base as builder

RUN conda build -c conda-forge compute_kubernetes/ && \
      conda build -c conda-forge compute_provisioner/ && \
      conda build -c cdat/label/nightly -c cdat -c conda-forge --use-local compute_tasks/

FROM builder as dev

RUN apt-get update && apt-get install -y --no-recommends vim && \
      conda install -c cdat/label/nightly -c cdat -c conda-forge --use-local --only-deps compute-tasks flake8 && \
      pip install -e compute_kubernetes/ && \
      pip install -e compute_provisioner/ && \
      pip install -e compute_tasks/

FROM continuumio/miniconda3:4.5.12

ENV COMPUTE_API_VER ${COMPUTE_API_VER}

ENV prometheus_multiproc_dir /metrics

COPY --from=builder /opt/conda/conda-bld/noarch/* /opt/conda/conda-bld/noarch/

RUN conda install -c cdat/label/nightly -c cdat -c conda-forge --use-local compute-tasks && \
      conda clean -a -y

RUN mkdir -p /tmp/certs && \
      curl -sL https://github.com/ESGF/esgf-dist/raw/master/installer/certs/esg_trusted_certificates.tar | tar xvf - -C /tmp/certs --strip 1

WORKDIR /

COPY docker/celery/entrypoint.sh .

COPY docker/celery/healthcheck.sh .

EXPOSE 8000

ENTRYPOINT ["tini", "--"]

CMD ["/bin/bash", "./entrypoint.sh", "-c", "1", "-Q", "default", "-n", "default", "-l", "INFO"]
