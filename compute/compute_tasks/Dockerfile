ARG BASE_IMAGE
FROM ${BASE_IMAGE} as builder

WORKDIR /build

COPY compute_tasks/ compute_tasks/
COPY conftest.py .
COPY meta.yaml .
COPY setup.cfg .
COPY setup.py .
COPY env .

RUN mamba mambabuild \
      -c conda-forge -c cdat \
      --output-folder channel/ . && \
      conda index channel

FROM ${BASE_IMAGE} as production

WORKDIR /home/esgf

COPY --from=builder /build/channel channel 

RUN mamba install -y \
      -c file:///home/esgf/channel -c conda-forge -c cdat \
      compute-tasks python=3.7 tini && \
      conda clean -afy && \
      rm -rf /opt/conda/pkgs && \
      mkdir /home/esgf/metrics

COPY entrypoint.sh .
COPY healthcheck.sh .

USER root

RUN chown -R esgf:conda /home/esgf && \
      chmod -R 775 /home/esgf

USER $USER

ENV prometheus_multiproc_dir /home/esgf/metrics

ARG CONTAINER_IMAGE
ENV CONTAINER_IMAGE $CONTAINER_IMAGE

ENTRYPOINT ["tini", "--"]

CMD ["/bin/bash", "./entrypoint.sh", "-c", "1", "-Q", "default", "-n", "default", "-l", "INFO"]
