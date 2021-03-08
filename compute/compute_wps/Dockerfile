ARG BASE_IMAGE
FROM $BASE_IMAGE as builder

WORKDIR /build

COPY compute_wps/ compute_wps/
COPY setup.py .
COPY setup.cfg .
COPY meta.yaml .
COPY pytest.ini .
COPY env .

RUN mamba mambabuild \
      -c conda-forge -c cdat \
      --output-folder channel/ . && \
      conda index channel/

FROM $BASE_IMAGE as production

WORKDIR /home/esgf

COPY --from=builder /build/channel channel

RUN mamba install -y \
      -c file:///home/esgf/channel -c conda-forge -c cdat \
      compute-wps python=3.7 tini && \
      conda clean -afy && \
      rm -rf /opt/conda/pkgs && \
      django-admin startproject compute

WORKDIR /home/esgf/compute

COPY init.sh .
COPY entrypoint.sh .

ARG CONTAINER_IMAGE
ENV CONTAINER_IMAGE $CONTAINER_IMAGE

ENTRYPOINT ["tini", "--"]

CMD ["./entrypoint.sh"]
