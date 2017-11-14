FROM continuumio/miniconda:4.3.27

ARG TAG=master

LABEL maintainer="boutte3@llnl.gov"

COPY environment.yml environment.yml

RUN conda env create --file environment.yml

#RUN conda create -n wps -c conda-forge -c uvcdat esgf-compute-api cdms2 cdutil genutil \
#	pyzmq lxml redis-py psycopg2 requests-oauthlib myproxyclient python-openid \
#       	django celery dask && \
#	conda clean --all -y

WORKDIR /var/www

RUN git clone --depth=1 --branch=${TAG} https://github.com/ESGF/esgf-compute-wps compute
