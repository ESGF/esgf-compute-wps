FROM continuumio/miniconda:4.3.14

ARG TAG=master

LABEL maintainer="boutte3@llnl.gov"

RUN conda install -c conda-forge -c uvcdat esgf-compute-api cdms2 cdutil genutil \
	pyzmq lxml redis-py psycopg2 requests-oauthlib myproxyclient python-openid && \
	conda clean --all -y

WORKDIR /var/www

RUN git clone --depth=1 https://github.com/ESGF/esgf-compute-wps compute && \
	cd compute && \
	git checkout ${tag}
