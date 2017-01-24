FROM ubuntu:latest

RUN apt-get update --fix-missing && \
	apt-get install -y git wget default-jdk scala

RUN echo "deb http://dl.bintray.com/sbt/debian /" | tee -a /etc/apt/sources.list.d/sbt.list && \
	apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv 2EE0EA64E40A89B84B2DF73499E82A75642AC823 && \
	apt-get update && \
	apt-get install sbt

RUN apt-get install -y bzip2

RUN wget --quiet https://repo.continuum.io/miniconda/Miniconda2-latest-Linux-x86_64.sh -O conda.sh && \
	/bin/bash conda.sh -bp /opt/conda && \
	rm conda.sh

ENV PATH /opt/conda/bin:$PATH

RUN git clone https://github.com/nasa-nccs-cds/CDAS2.git

WORKDIR /CDAS2

ENV CONDA_PREFIX /opt/conda

RUN sed -i 's/System.out.println.*//g' src/main/java/nasa/nccs/cdas/workers/test/floatClient.java && \
	sed -i 's/System.out.println.*//g' src/main/java/nasa/nccs/cdas/workers/test/helloClient.java

RUN sbt compile; exit 0 

RUN sbt compile && \
	sbt stage

ENV CDAS_REQUEST_PORT 4356
ENV CDAS_RESPONSE_PORT 4357

CMD ["sh", "-c", "target/universal/stage/bin/cdas2 bind $CDAS_REQUEST_PORT $CDAS_RESPONSE_PORT"]
