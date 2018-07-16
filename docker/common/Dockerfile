FROM continuumio/miniconda:4.3.27

LABEL maintainer="boutte3@llnl.gov"

ENV CELERY_BROKER=redis://redis/0

ENV CELERY_BACKEND=redis://redis/0

ENV POSTGRES_HOST=postgres

ENV WPS_HOST=0.0.0.0

RUN mkdir -p /var/log/cwt

COPY . /var/www/compute

WORKDIR /var/www

RUN conda env create --name wps --file compute/docker/common/environment.yml && \
	conda clean -y --all

RUN mkdir -p /etc/config && \
	cp compute/docker/common/django.properties /etc/config/django.properties
