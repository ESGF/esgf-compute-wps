FROM continuumio/miniconda:4.3.27

ARG TAG=master

LABEL maintainer="boutte3@llnl.gov"

COPY environment.yml environment.yml

RUN conda env create --name wps --file environment.yml && \
	conda clean -y --all

WORKDIR /var/www

RUN git clone --depth=1 --branch=${TAG} https://github.com/ESGF/esgf-compute-wps compute
