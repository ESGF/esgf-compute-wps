FROM continuumio/miniconda3:4.5.12 as builder

WORKDIR /src

COPY . .

WORKDIR /src/compute

RUN conda install -c conda-forge -y conda-build && \
      conda build -c conda-forge compute_provisioner

FROM continuumio/miniconda3:4.5.12

ENV TINI_VERSION v0.18.0

ENV FRONTEND_PORT 7777

ENV BACKEND_PORT 7778

RUN curl https://github.com/krallin/tini/releases/download/${TINI_VERSION}/tini -o /tini && \
      chmod +x /tini 

COPY --from=builder /opt/conda/conda-bld/noarch/* /opt/conda/conda-bld/noarch/

RUN conda install -c conda-forge --use-local compute-provisioner

EXPOSE 7777 7778

ENTRYPOINT ["tini", "--"]

CMD ["python", "-m", "compute_provisioner.load_balancer"]
