FROM continuumio/miniconda:latest

RUN echo "deb http://ppa.launchpad.net/webupd8team/java/ubuntu xenial main" | tee /etc/apt/sources.list.d/webupd8team-java.list && \
	echo "deb http://dl.bintray.com/sbt/debian /" | tee -a /etc/apt/sources.list.d/sbt.list && \
	apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv-keys EEA14886 && \
	apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv 2EE0EA64E40A89B84B2DF73499E82A75642AC823 && \
	echo debconf shared/accepted-oracle-license-v1-1 select true | debconf-set-selections && \
	echo debconf shared/accepted-oracle-license-v1-1 seen true | debconf-set-selections && \
	apt-get update && \
	apt-get install -y --no-install-recommends oracle-java8-installer scala sbt &&\
	apt-get clean

ENV CONDA_PREFIX /opt/conda

RUN conda install -c conda-forge cdms2 cdutil genutil nco pyzmq psutil lxml

RUN git clone --depth=1 https://github.com/nasa-nccs-cds/CDAS2.git && \
	cd CDAS2 && \
	sbt package; exit 0

RUN cd CDAS2 && \
	sbt package && \
	python setup.py install

WORKDIR /CDAS2

COPY entrypoint.sh entrypoint.sh

ENV CDAS_REQUEST_PORT 4356
ENV CDAS_RESPONSE_PORT 4357

ENTRYPOINT ["./entrypoint.sh"]
