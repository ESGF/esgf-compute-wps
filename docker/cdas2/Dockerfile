FROM continuumio/miniconda:4.2.12

RUN echo "deb http://dl.bintray.com/sbt/debian /" | tee -a /etc/apt/sources.list.d/sbt.list && \
	echo "deb http://ftp.debian.org/debian jessie-backports main" | tee -a /etc/apt/sources.list.d/jessie-backports.list && \
	apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv 2EE0EA64E40A89B84B2DF73499E82A75642AC823 && \
	apt-get update --fix-missing && \
	apt-get install -y --no-install-recommends -t jessie-backports openjdk-8-jre-headless ca-certificates-java && \
	apt-get install -y --no-install-recommends default-jdk sbt linux-headers-amd64 build-essential openjdk-8-jdk && \
	apt-get clean && \
	update-java-alternatives -s java-1.8.0-openjdk-amd64

RUN conda install -c conda-forge -c uvcdat/label/nightly -c uvcdat uvcdat-nox nco pyzmq

RUN git clone --depth=1 https://github.com/nasa-nccs-cds/CDAS2.git && \
	cd CDAS2 && \
	git checkout master

WORKDIR /CDAS2

RUN sed -i 's/System.out.println.*//g' src/main/java/nasa/nccs/cdas/workers/test/floatClient.java && \
	sed -i 's/System.out.println.*//g' src/main/java/nasa/nccs/cdas/workers/test/helloClient.java

ENV CONDA_PREFIX=/opt/conda

RUN sbt compile; exit 0

RUN sbt compile && \
	sbt stage

RUN python setup.py install

ENV CDAS_REQUEST_PORT 4356
ENV CDAS_RESPONSE_PORT 4357
ENV UVCDAT_ANONYMOUS_LOG=no

EXPOSE $CDAS_REQUEST_PORT $CDAS_RESPONSE_PORT

CMD ["sh", "-c", "target/universal/stage/bin/cdas2 bind $CDAS_REQUEST_PORT $CDAS_RESPONSE_PORT -J-Xmx2000M -J-Xms512M -J-Xss1M -J-XX:+CMSClassUnloadingEnabled -J-XX:+UseConcMarkSweepGC"]
