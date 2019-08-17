FROM continuumio/miniconda3:4.6.14 as builder

ARG GIT_SHORT_COMMIT

ENV GIT_SHORT_COMMIT ${GIT_SHORT_COMMIT}

WORKDIR /src

COPY compute/compute_kubernetes/ compute_kubernetes/

RUN echo "__version__ = '${GIT_SHORT_COMMIT}'" > compute_kubernetes/compute_kubernetes/_version.py

RUN conda install -y conda-build && \
      conda build -c conda-forge -c cdat compute_kubernetes/

FROM builder as dev

RUN apt-get update && \
      apt-get install -y --no-install-recommends vim && \
      rm -rf /var/lib/apt/lists/* && \
      conda install -c conda-forge -c cdat --use-local --only-deps compute-kubernetes flake8 pytest && \
      pip install -e compute_kubernetes/

FROM continuumio/miniconda3:4.6.14

COPY --from=builder /opt/conda/pkgs/ /opt/conda/pkgs/
COPY --from=builder /opt/conda/conda-bld/noarch/ /opt/conda/conda-bld/noarch/

RUN conda install -c conda-forge -c cdat --use-local compute-kubernetes && \
      conda clean -a -y

ENTRYPOINT ["tini", "--"]

CMD ["compute-kube-monitor"]
