ARG BASE_IMAGE

FROM ${BASE_IMAGE} as builder

ARG TEST_DATA=/test_data
ENV TEST_DATA ${TEST_DATA}

RUN conda update -n base conda && \
      conda install -c defaults -c conda-forge -y conda-build conda-forge-pinning curl

RUN mkdir -p /tmp/certs && \
      curl -sL https://github.com/ESGF/esgf-dist/raw/master/installer/certs/esg_trusted_certificates.tar | tar xvf - -C /tmp/certs --strip 1

WORKDIR /build

# COPY test_data /test_data
COPY compute_tasks/ compute_tasks/
COPY conftest.py conftest.py
COPY meta.yaml meta.yaml
COPY setup.cfg setup.cfg
COPY setup.py setup.py

RUN conda build -c conda-forge -c cdat -m /opt/conda/conda_build_config.yaml --python 3.7 . && \
      mkdir -p /compute-channel/noarch && \
      cp /opt/conda/pkgs/compute-tasks*.tar.bz2 /compute-channel/noarch && \
      conda index /compute-channel

FROM ${BASE_IMAGE} as production

RUN mkdir /metrics

RUN apt-get update && \
      apt-get install -y --no-install-recommends dnsutils && \
      rm -rf /var/lib/apt/lists/*

COPY --from=builder /tmp/certs /tmp/certs
COPY --from=builder /compute-channel /compute-channel

RUN conda install -c file:///compute-channel -c conda-forge -c cdat compute-tasks && \
      conda clean -afy && \
      rm -rf /opt/conda/pkgs

WORKDIR /

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

# FROM scratch as testdata

# COPY --from=builder /test_data/ .
