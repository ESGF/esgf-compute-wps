ARG BASE_IMAGE
FROM ${BASE_IMAGE} as builder

WORKDIR /build

RUN conda update -n base conda && \
      conda install -c conda-forge -y conda-build conda-smithy && \
      conda smithy ci-skeleton compute_provisioner

WORKDIR /build/recipe

COPY compute_provisioner/ compute_provisioner/
COPY meta.yaml .
COPY setup.cfg .
COPY setup.py .

WORKDIR /build

RUN conda smithy rerender && \
      conda build -c conda-forge -m .ci_support/linux_64_.yaml --output-folder channel/ . && \
      conda index channel/

FROM ${BASE_IMAGE} as production

WORKDIR /

COPY --from=builder /build/channel /channel

RUN conda install -c file:///channel -c conda-forge compute-provisioner && \
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
