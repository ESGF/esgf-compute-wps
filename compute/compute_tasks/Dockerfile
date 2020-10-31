ARG BASE_IMAGE
FROM ${BASE_IMAGE} as builder

WORKDIR /build

RUN conda update -n base -c defaults conda && \
      conda install -c conda-forge -y conda-build conda-smithy curl && \
      conda smithy ci-skeleton compute_tasks

RUN mkdir -p /tmp/certs && \
      curl -sL https://github.com/ESGF/esgf-dist/raw/master/installer/certs/esg_trusted_certificates.tar | tar xvf - -C /tmp/certs --strip 1

WORKDIR /build/recipe

COPY compute_tasks/ compute_tasks/
COPY conftest.py .
COPY meta.yaml .
COPY setup.cfg .
COPY setup.py .
COPY env .

WORKDIR /build

RUN conda smithy rerender && \
      conda build -c conda-forge -c cdat -m .ci_support/linux_64_.yaml --output-folder channel/ . && \
      conda index channel/

FROM ${BASE_IMAGE} as production

RUN mkdir /metrics

WORKDIR /

COPY --from=builder /tmp/certs /tmp/certs
COPY --from=builder /build/channel /channel 

RUN apt update && \
      apt install -y --no-install-recommends dnsutils && \
      rm -rf /var/lib/apt/lists/*

RUN conda install -c file:///channel -c conda-forge -c cdat compute-tasks && \
      conda clean -afy && \
      rm -rf /opt/conda/pkgs

COPY entrypoint.sh .
COPY healthcheck.sh .

ENV TINI_VERSION v0.18.0
ADD https://github.com/krallin/tini/releases/download/${TINI_VERSION}/tini /tini
RUN chmod +x /tini

EXPOSE 8000

ENV prometheus_multiproc_dir /metrics

ARG CONTAINER_IMAGE
ENV CONTAINER_IMAGE $CONTAINER_IMAGE

ENTRYPOINT ["/tini", "--"]

CMD ["/bin/bash", "./entrypoint.sh", "-c", "1", "-Q", "default", "-n", "default", "-l", "INFO"]

FROM scratch as testresult

COPY --from=builder /output/unittest.xml .
COPY --from=builder /output/coverage.xml .
