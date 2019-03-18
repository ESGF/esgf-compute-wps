FROM continuumio/miniconda:4.5.12 as builder

WORKDIR /wps

COPY . .

RUN conda install -c conda-forge -y conda-build && \
      conda build -c conda-forge -c cdat compute/wps

FROM continuumio/miniconda:4.5.12

COPY --from=builder /opt/conda/conda-bld/noarch/* /opt/conda/conda-bld/noarch/

COPY docker/wps/environment.yml environment.yml

RUN conda env update -n base -f environment.yml && \
      conda install -c conda-forge -c cdat --use-local esgf-compute-wps && \
      django-admin startproject compute && \
      rm environment.yml

COPY docker/wps/django.properties /etc/config
COPY docker/wps/celery.py /compute/compute
COPY docker/wps/settings.py /compute/compute
COPY docker/wps/urls.py /compute/compute
COPY docker/wps/wsgi.py /compute/compute
