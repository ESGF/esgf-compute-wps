FROM tomcat:8.5

ENV THREDDS_VERSION 4.6.10

ENV TINI_VERSION v0.18.0

ENV JAVA_HEAP_INIT 512m
ENV JAVA_HEAP_MAX 1g

RUN wget http://artifacts.unidata.ucar.edu/content/repositories/unidata-releases/edu/ucar/tds/${THREDDS_VERSION}/tds-${THREDDS_VERSION}.war \
	-O webapps/threddsCWT.war

RUN wget https://github.com/krallin/tini/releases/download/${TINI_VERSION}/tini -O /tini && \
  chmod +x /tini

COPY setenv.sh bin/
COPY entrypoint.sh entrypoint.sh
COPY catalog.xml catalog.xml
COPY threddsConfig.xml threddsConfig.xml
COPY server.xml conf/

ENTRYPOINT ["/tini", "--"]

CMD ["/bin/bash", "./entrypoint.sh"]
