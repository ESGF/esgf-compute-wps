FROM continuumio/miniconda:4.3.27

ARG TAG="1.2.3-RC2"

RUN git clone --branch $TAG https://github.com/nasa-nccs-cds/EDAS.git edas

RUN echo "deb http://ppa.launchpad.net/webupd8team/java/ubuntu xenial main" | \
  tee /etc/apt/sources.list.d/webupd8team-java.list && \
  echo "deb-src http://ppa.launchpad.net/webupd8team/java/ubuntu xenial main" | \
  tee -a /etc/apt/sources.list.d/webupd8team-java.list && \
  apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv-keys EEA14886 && \
  apt-get update && \
  echo "oracle-java8-installer shared/accepted-oracle-license-v1-1 select true" | debconf-set-selections && \
  echo "oracle-java8-installer shared/accepted-oracle-license-v1-1 seen true" | debconf-set-selections && \
  apt-get install -y --no-install-recommends oracle-java8-installer

RUN echo "deb https://dl.bintray.com/sbt/debian /" | tee -a /etc/apt/sources.list.d/sbt.list && \
      apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv 2EE0EA64E40A89B84B2DF73499E82A75642AC823 && \
      apt-get install -y apt-transport-https && \
      apt-get update && \
      apt-get install -y --no-install-recommends scala sbt

RUN conda create -n edas -c conda-forge cdutil cdms2 pyzmq psutil lxml requests urllib3 six && \
      cd edas/ && \
      PATH=/opt/conda/envs/edas/bin:$PATH python setup.py install

COPY build_edas.sh build_edas.sh

RUN bash build_edas.sh 

RUN curl http://us.mirrors.quenda.co/apache/spark/spark-2.3.1/spark-2.3.1-bin-hadoop2.7.tgz -o spark.tgz && \
      mkdir -p /spark && \
      tar -xf spark.tgz -C /spark --strip-components=1

ENV PATH=/spark/bin:$PATH

ENV USER=root

COPY edas.properties /root/.edas/cache

RUN cp -rf /edas/bin/* /root/.edas/sbin && \
      mkdir -p /root/.edas/cache/collections/agg && \
      touch /spark/conf/slaves

COPY entrypoint.sh entrypoint.sh

ENTRYPOINT ["./entrypoint.sh"] 
