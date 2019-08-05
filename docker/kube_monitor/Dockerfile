FROM continuumio/miniconda3:4.5.12 as base

WORKDIR /src

COPY . .

WORKDIR /src/compute

RUN GIT_REV=$(git rev-parse --short HEAD) && \
      conda update -n base -c defaults conda && \
      echo "__version__ = '${GIT_REV}'" > compute_kubernetes/compute_kubernetes/_version.py

FROM base as builder

RUN conda install -c conda-forge conda-build=3.17.8 && \
      conda build -c conda-forge -c cdat compute_kubernetes/

FROM builder as dev

RUN apt-get update && \
      apt-get install -y --no-install-recommends vim && \
      rm -rf /var/lib/apt/lists/* && \
      conda install -c conda-forge -c cdat --use-local --only-deps compute-kubernetes flake8 pytest && \
      pip install -e compute_kubernetes/

FROM continuumio/miniconda3:4.5.12

COPY --from=builder /opt/conda/conda-bld/noarch/* /opt/conda/conda-bld/noarch/

RUN conda update -n base -c defaults conda && \
      conda install -c conda-forge -c cdat --use-local compute-kubernetes && \
      conda clean -a -y

ENTRYPOINT ["compute-kube-monitor"]
