ARG BASE_IMAGE
FROM ${BASE_IMAGE} as builder

WORKDIR /build

COPY compute_provisioner/ compute_provisioner/
COPY meta.yaml .
COPY setup.cfg .
COPY setup.py .

RUN mamba mambabuild \
      -c conda-forge \
      --output-folder channel/ . && \
      mamba index channel/

FROM ${BASE_IMAGE} as production

WORKDIR /home/esgf

COPY --from=builder /build/channel /channel

RUN mamba install -y \
      -c file:///channel -c conda-forge \
      compute-provisioner tini python=3.7 && \
      mamba clean -afy && \
      rm -rf /opt/conda/pkgs

ARG CONTAINER_IMAGE
ENV CONTAINER_IMAGE $CONTAINER_IMAGE

ENTRYPOINT ["tini", "--"]

CMD ["python", "-m", "compute_provisioner.provisioner"]
