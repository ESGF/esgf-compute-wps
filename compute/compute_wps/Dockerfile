ARG CONDA_VERSION=4.8.2

FROM continuumio/miniconda3:${CONDA_VERSION} as builder

RUN conda update -n base conda && \
      conda install -c conda-forge -y conda-build conda-forge-pinning

WORKDIR /build

COPY compute/compute_wps/ compute_wps/

RUN conda build -c conda-forge -c cdat -m /opt/conda/conda_build_config.yaml --python 3.7 compute_wps/

FROM continuumio/miniconda3:${CONDA_VERSION} as production

COPY --from=builder /opt/conda/pkgs/ /opt/conda/pkgs/
COPY --from=builder /opt/conda/conda-bld/noarch/ /opt/conda/conda-bld/noarch/

WORKDIR /

RUN conda install -c conda-forge -c cdat --use-local compute-wps curl && \
      django-admin startproject compute 

COPY compute/compute_wps/django.properties /etc/config/

WORKDIR /compute

COPY compute/compute_wps/settings.py compute/
COPY compute/compute_wps/urls.py compute/
COPY compute/compute_wps/wsgi.py compute/
COPY compute/compute_wps/init.sh .
COPY compute/compute_wps/entrypoint.sh .

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
