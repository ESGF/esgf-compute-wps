FROM continuumio/miniconda3:4.5.12 as builder

WORKDIR /src

COPY . .

WORKDIR /src/compute

RUN conda install -c conda-forge -y conda-build && \
      conda build -c conda-forge -c cdat compute_kubernetes/

FROM continuumio/miniconda3:4.5.12

COPY --from=builder /opt/conda/conda-bld/noarch/* /opt/conda/conda-bld/noarch/

RUN conda install -c conda-forge -c cdat --use-local compute-kubernetes

ENTRYPOINT ["compute-kube-monitor"]
