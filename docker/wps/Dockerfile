FROM continuumio/miniconda3:4.6.14 as base

ARG COMPUTE_API_VER

ENV COMPUTE_API_VER ${COMPUTE_API_VER}

WORKDIR /src

COPY . .

WORKDIR /src/compute

RUN GIT_REV=$(git rev-parse --short HEAD) && \
      echo "__version__ = '${GIT_REV}'" > compute_settings/compute_settings/_version.py && \
      echo "__version__ = '${GIT_REV}'" > compute_wps/compute_wps/_version.py 

FROM base as builder

RUN conda install -c conda-forge conda-build && \
      conda build -c conda-forge -c cdat compute_settings/ && \
      conda build -c cdat/label/nightly -c cdat -c conda-forge --use-local compute_wps/

FROM builder as dev

RUN apt-get update && \
      apt-get install -y --no-install-recommends vim && \
      rm -rf /var/lib/apt/lists/* && \
      conda install -c cdat/label/nightly -c cdat -c conda-forge --use-local --only-deps compute-wps flake8 pytest && \
      pip install -e compute_settings/ && \
      pip install -e compute_wps/

COPY docker/wps/django.properties /etc/config/

WORKDIR /

RUN django-admin startproject compute && \
      mkdir -p /var/log/cwt  

WORKDIR /compute/compute

COPY docker/wps/settings.py .
COPY docker/wps/urls.py .
COPY docker/wps/wsgi.py .

WORKDIR /src/compute/compute_wps

COPY docker/wps/development.sh .

ENTRYPOINT ["tini", "--"]

CMD ["/bin/bash"]

FROM continuumio/miniconda3:4.6.14

COPY --from=builder /opt/conda/conda-bld/noarch/* /opt/conda/conda-bld/noarch/

RUN conda install -c cdat/label/nightly -c cdat -c conda-forge --use-local compute-wps && \
      django-admin startproject compute && \
      mkdir -p /var/log/cwt

COPY docker/wps/django.properties /etc/config/

WORKDIR /compute/compute

COPY docker/wps/settings.py .
COPY docker/wps/urls.py .
COPY docker/wps/wsgi.py .

WORKDIR /

COPY docker/wps/app.py .
COPY docker/wps/entrypoint.sh .

ENV CWT_BASE=/compute

EXPOSE 8000

ENTRYPOINT ["./entrypoint.sh"]
