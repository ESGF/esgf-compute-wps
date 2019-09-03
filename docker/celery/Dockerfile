FROM continuumio/miniconda3:4.6.14 as builder

RUN conda install -y conda-build

WORKDIR /src

COPY compute/compute_kubernetes compute_kubernetes/
COPY compute/compute_provisioner compute_provisioner/
COPY compute/compute_tasks compute_tasks/

ARG COMPUTE_API_VER
ARG GIT_SHORT_COMMIT

ENV COMPUTE_API_VER ${COMPUTE_API_VER}
ENV GIT_SHORT_COMMIT ${GIT_SHORT_COMMIT}

RUN echo "__version__ = '${GIT_SHORT_COMMIT}'" > compute_kubernetes/compute_kubernetes/_version.py && \
      echo "__version__ = '${GIT_SHORT_COMMIT}'" > compute_provisioner/compute_provisioner/_version.py && \
      echo "__version__ = '${GIT_SHORT_COMMIT}'" > compute_tasks/compute_tasks/_version.py

RUN conda build -c conda-forge compute_kubernetes/ && \
      conda build -c conda-forge compute_provisioner/ && \
      conda build -c cdat/label/nightly -c cdat -c conda-forge --use-local compute_tasks/

FROM builder as dev

RUN apt-get update && apt-get install -y --no-install-recommends vim && \
      # Use conda to install package for dependencies
      conda install -c conda-forge -c cdat  --use-local --only-deps compute-tasks &&\
      conda install -c conda-forge flake8 pytest pytest-mock pytest-cov && \
      pip install -e compute_kubernetes/ && \
      pip install -e compute_provisioner/ && \
      pip install -e compute_tasks/

FROM continuumio/miniconda3:4.6.14

RUN mkdir -p /tmp/certs && \
      curl -sL https://github.com/ESGF/esgf-dist/raw/master/installer/certs/esg_trusted_certificates.tar | tar xvf - -C /tmp/certs --strip 1

COPY --from=builder /opt/conda/pkgs/ /opt/conda/pkgs/
COPY --from=builder /opt/conda/conda-bld/noarch/ /opt/conda/conda-bld/noarch/

RUN conda install -c conda-forge -c cdat --use-local compute-tasks && \
      conda clean -a -y

WORKDIR /

COPY docker/celery/entrypoint.sh .
COPY docker/celery/healthcheck.sh .

ARG GIT_SHORT_COMMIT
ARG COMPUTE_API_VER

ENV CONTAINER_VER ${GIT_SHORT_COMMIT}
ENV COMPUTE_API_VER ${COMPUTE_API_VER}
ENV prometheus_multiproc_dir /metrics

EXPOSE 8000

ENTRYPOINT ["tini", "--"]

CMD ["/bin/bash", "./entrypoint.sh", "-c", "1", "-Q", "default", "-n", "default", "-l", "INFO"]
