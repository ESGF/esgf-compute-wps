FROM continuumio/miniconda:4.5.12 as builder

WORKDIR /src

COPY . .

WORKDIR /src/compute/wps

RUN conda install -c conda-forge -y conda-build && \
      conda build -c conda-forge -c cdat .

FROM continuumio/miniconda:4.5.12

COPY --from=builder /opt/conda/conda-bld/noarch/* /opt/conda/conda-bld/noarch/

COPY docker/wps/environment.yml environment.yml

RUN conda env update -n base -f environment.yml && \
      conda install -c conda-forge -c cdat --use-local esgf-compute-wps && \
      django-admin startproject compute && \
      rm environment.yml && \
      mkdir -p /var/log/cwt

WORKDIR /etc/config

COPY docker/wps/django.properties .

WORKDIR /compute/compute

COPY docker/wps/celery.py .
COPY docker/wps/settings.py .
COPY docker/wps/urls.py .
COPY docker/wps/wsgi.py .

WORKDIR /

COPY docker/wps/app.py .
COPY docker/wps/entrypoint.sh .

ENV CWT_BASE=/compute

EXPOSE 8000

ENTRYPOINT ['./entrypoint.sh']
