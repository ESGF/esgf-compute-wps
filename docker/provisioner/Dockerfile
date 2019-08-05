FROM continuumio/miniconda3:4.5.12 as base

WORKDIR /src

COPY . .

WORKDIR /src/compute

RUN GIT_REV=$(git rev-parse --short HEAD) && \
      conda update -n base -c defaults conda && \
      echo "__version__ = '${GIT_REV}'" > compute_provisioner/compute_provisioner/_version.py

FROM base as builder

RUN conda install -c conda-forge conda-build=3.17.8 && \
      conda build -c conda-forge compute_provisioner/

FROM builder as dev

RUN apt-get update && \
      apt-get install -y --no-install-recommends vim && \
      rm -rf /var/lib/apt/lists/* && \
      conda install -c conda-forge --use-local --only-deps compute-provisioner flake8 pytest && \
      pip install -e compute_provisioner/

FROM continuumio/miniconda3:4.5.12

ENV FRONTEND_PORT 7777

ENV BACKEND_PORT 7778

COPY --from=builder /opt/conda/conda-bld/noarch/* /opt/conda/conda-bld/noarch/

RUN conda update -n base -c defaults conda && \
      conda install -c conda-forge --use-local compute-provisioner

EXPOSE 7777 7778

ENTRYPOINT ["tini", "--"]

CMD ["python", "-m", "compute_provisioner.load_balancer"]
