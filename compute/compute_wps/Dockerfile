ARG BASE_IMAGE

FROM ${BASE_IMAGE} as builder

RUN conda update -n base conda && \
      conda install -c conda-forge -y conda-build conda-forge-pinning

WORKDIR /build

COPY compute_wps/ compute_wps/
COPY setup.cfg .
COPY django.properties .
COPY setup.py .
COPY settings.py .
COPY meta.yaml .
COPY urls.py .
COPY wsgi.py .
COPY pytest.ini .

RUN conda build -c conda-forge -c cdat -m /opt/conda/conda_build_config.yaml --python 3.7 . && \
      mkdir -p /compute-channel/noarch && \
      cp /opt/conda/pkgs/compute-wps*.tar.bz2 /compute-channel/noarch && \
      conda index /compute-channel

FROM ${BASE_IMAGE} as production

COPY --from=builder /compute-channel /compute-channel

WORKDIR /

RUN conda install -c file:///compute-channel -c conda-forge -c cdat compute-wps && \
      conda clean -afy && \
      rm -rf /opt/conda/pkgs && \
      django-admin startproject compute 

COPY django.properties /etc/config/

WORKDIR /compute

COPY settings.py compute/
COPY urls.py compute/
COPY wsgi.py compute/
COPY init.sh .
COPY entrypoint.sh .

ENV TINI_VERSION v0.18.0
ADD https://github.com/krallin/tini/releases/download/${TINI_VERSION}/tini /tini
RUN chmod +x /tini

EXPOSE 8000

ARG CONTAINER_IMAGE
ENV CONTAINER_IMAGE $CONTAINER_IMAGE

ENTRYPOINT ["/tini", "--"]

CMD ["./entrypoint.sh"]

FROM scratch as testresult

COPY --from=builder /output/unittest.xml .
COPY --from=builder /output/coverage.xml .
