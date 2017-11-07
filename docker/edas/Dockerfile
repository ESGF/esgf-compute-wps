FROM continuumio/miniconda:4.3.11

ARG TAG="master"

RUN git clone --branch $TAG https://github.com/nasa-nccs-cds/EDAS.git edas

RUN apt-get update && \
	apt-get install --no-install-recommends -y apt-transport-https && \
       	echo "deb https://dl.bintray.com/sbt/debian /" |  tee -a /etc/apt/sources.list.d/sbt.list && \
	apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv 2EE0EA64E40A89B84B2DF73499E82A75642AC823 && \
	apt-get update && \
	apt-get install --no-install-recommends -y default-jdk build-essential sbt scala bc && \
	conda create -n edas -c conda-forge -c uvcdat uvcdat pyzmq psutil lxml && \
	conda clean --yes --all && \
	apt-get remove -y build-essential && \
	apt-get autoremove -y && \
	apt-get autoclean -y

WORKDIR /edas

RUN sbt package; exit 0

RUN sbt package

WORKDIR /

RUN curl -SL https://d3kbcqa49mib13.cloudfront.net/spark-1.6.2-bin-hadoop2.6.tgz | tar xvzf - && \
	mv spark-1.6.2-bin-hadoop2.6 spark 

ENV PATH /spark/bin:/opt/conda/envs/edas/bin:$PATH

RUN cd /edas && \
	python setup.py install && \
	mkdir -p ~/.edas/sbin && \
	cp -rf /edas/bin/* ~/.edas/sbin

COPY edas.properties /root/.edas/cache/

COPY entrypoint.sh entrypoint.sh

COPY healthcheck.py healthcheck.py

EXPOSE 5670 5671

#HEALTHCHECK CMD python healthcheck.py

ENTRYPOINT './entrypoint.sh'
