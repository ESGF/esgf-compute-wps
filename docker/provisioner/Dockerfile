FROM continuumio/miniconda3:4.6.14 as builder

ARG GIT_SHORT_COMMIT

ENV GIT_SHORT_COMMIT ${GIT_SHORT_COMMIT}

WORKDIR /src

COPY compute/compute_provisioner/ compute_provisioner/

RUN echo "__version__ = '${GIT_SHORT_COMMIT}'" > compute_provisioner/compute_provisioner/_version.py

RUN conda install conda-build && \
      conda build -c conda-forge compute_provisioner/

FROM builder as dev

RUN apt-get update && \
      apt-get install -y --no-install-recommends vim && \
      rm -rf /var/lib/apt/lists/* && \
      conda install -c conda-forge --use-local --only-deps compute-provisioner flake8 pytest && \
      pip install -e compute_provisioner/

FROM continuumio/miniconda3:4.6.14

COPY --from=builder /opt/conda/pkgs/ /opt/conda/pkgs/
COPY --from=builder /opt/conda/conda-bld/noarch/ /opt/conda/conda-bld/noarch/

RUN conda install -c conda-forge --use-local compute-provisioner

EXPOSE 7777 7778

ENV FRONTEND_PORT 7777
ENV BACKEND_PORT 7778

ENTRYPOINT ["tini", "--"]

CMD ["python", "-m", "compute_provisioner.load_balancer"]
