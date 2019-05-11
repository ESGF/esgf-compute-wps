FROM continuumio/miniconda3:4.5.12 as builder

WORKDIR /src

COPY . .

WORKDIR /src/compute

RUN conda install -c conda-forge -y conda-build && \
      conda build -c conda-forge -c cdat cwt_settings/ && \
      conda build -c conda-forge -c cdat cwt_kubernetes/ && \
      conda build -c conda-forge -c cdat --use-local wps/

FROM continuumio/miniconda3:4.5.12

COPY --from=builder /opt/conda/conda-bld/noarch/* /opt/conda/conda-bld/noarch/

RUN conda install -c conda-forge -c cdat --use-local \
      esgf-compute-wps xarray && \
      django-admin startproject compute && \
      mkdir -p /var/log/cwt

WORKDIR /etc/config

COPY docker/wps/django.properties .

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
