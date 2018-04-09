FROM continuumio/miniconda:4.3.27

LABEL maintainer="boutte3@llnl.gov"

ARG TAG=master

ENV CELERY_BROKER=redis://redis/0

ENV CELERY_BACKEND=redis://redis/0

ENV POSTGRES_HOST=postgres

ENV WPS_HOST=0.0.0.0

COPY environment.yml environment.yml

RUN conda env create --name wps --file environment.yml && \
	PATH=/opt/conda/envs/wps/bin:$PATH pip install django-webpack-loader && \
	conda clean -y --all 

WORKDIR /var/www

RUN git clone --depth=1 --branch=${TAG} https://github.com/ESGF/esgf-compute-wps compute && \
	mkdir -p /var/log/cwt

COPY django.properties /etc/config/django.properties
