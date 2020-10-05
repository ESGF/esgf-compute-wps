ARG BASE_IMAGE

FROM ${BASE_IMAGE} as builder

RUN conda update -n base conda && \
      conda install -c conda-forge -y conda-build conda-forge-pinning

WORKDIR /build

COPY compute_provisioner/ compute_provisioner/
COPY meta.yaml meta.yaml
COPY setup.cfg setup.cfg
COPY setup.py setup.py

RUN conda build -c conda-forge -c cdat -m /opt/conda/conda_build_config.yaml --python 3.7 . \
      mkdir -p /compute-channel/noarch && \
      cp /opt/conda/pkgs/compute-provisioner*.tar.bz2 /compute-channel/noarch && \
      conda index /compute-channel

FROM ${BASE_IMAGE} as production

COPY --from=builder /compute-channel /compute-channel

RUN conda install -c file:///compute-channel -c conda-forge compute-provisioner && \
      conda clean -afy && \
      rm -rf /opt/conda/pkgs

ENV TINI_VERSION v0.18.0
ADD https://github.com/krallin/tini/releases/download/${TINI_VERSION}/tini /tini
RUN chmod +x /tini

EXPOSE 7777 7778

ENV FRONTEND_PORT 7777
ENV BACKEND_PORT 7778

ARG CONTAINER_IMAGE
ENV CONTAINER_IMAGE $CONTAINER_IMAGE

ENTRYPOINT ["/tini", "--"]

CMD ["python", "-m", "compute_provisioner.provisioner"]
