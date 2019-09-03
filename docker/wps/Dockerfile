FROM continuumio/miniconda3:4.7.10 as builder

ARG GIT_SHORT_COMMIT
ARG COMPUTE_API_VER

ENV GIT_SHORT_COMMIT ${GIT_SHORT_COMMIT}
ENV COMPUTE_API_VER ${COMPUTE_API_VER}

RUN conda install -y conda-build

WORKDIR /src

COPY compute/compute_settings/ compute_settings/
COPY compute/compute_wps/ compute_wps/

RUN echo "__version__ = '${GIT_SHORT_COMMIT}'" > compute_settings/compute_settings/_version.py && \
      echo "__version__ = '${GIT_SHORT_COMMIT}'" > compute_wps/compute_wps/_version.py 

RUN conda build -c conda-forge -c cdat compute_settings/ && \
      conda build -c cdat/label/nightly -c cdat -c conda-forge --use-local compute_wps/

FROM builder as dev

COPY docker/wps/django.properties /etc/config/django.properties

RUN apt-get update && \
      apt-get install -y --no-install-recommends vim && \
      rm -rf /var/lib/apt/lists/* && \
      conda install -c cdat/label/nightly -c cdat -c conda-forge --use-local --only-deps compute-wps flake8 pytest && \
      pip install -e compute_settings/ && \
      pip install -e compute_wps/

WORKDIR /

RUN django-admin startproject compute

WORKDIR /compute/compute

COPY docker/wps/settings.py .
COPY docker/wps/urls.py .
COPY docker/wps/wsgi.py .

WORKDIR /src/compute/compute_wps

COPY docker/wps/development.sh .

ENTRYPOINT ["tini", "--"]

CMD ["/bin/bash"]

FROM continuumio/miniconda3:4.7.10

COPY --from=builder /opt/conda/pkgs/ /opt/conda/pkgs/
COPY --from=builder /opt/conda/conda-bld/noarch/ /opt/conda/conda-bld/noarch/

WORKDIR /

RUN conda install -c cdat/label/nightly -c cdat -c conda-forge --use-local compute-wps && \
      django-admin startproject compute

COPY docker/wps/django.properties /etc/config/

WORKDIR /compute

COPY docker/wps/settings.py compute/
COPY docker/wps/urls.py compute/
COPY docker/wps/wsgi.py compute/
COPY docker/wps/app.py .
COPY docker/wps/entrypoint.sh .

EXPOSE 8000

ENTRYPOINT ["tini", "--"]

CMD ["./entrypoint.sh"]
