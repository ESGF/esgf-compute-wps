FROM tomcat:8.5

ARG THREDDS_VER
ARG JAVA_HEAP_INIT=512m
ARG JAVA_HEAP_MAX=1g
ARG TINI_VERSION=v0.18.0

ENV THREDDS_VER ${THREDDS_VER}
ENV TINI_VERSION ${TINI_VERSION}
ENV JAVA_HEAP_INIT ${JAVA_HEAP_INIT}
ENV JAVA_HEAP_MAX ${JAVA_HEAP_MAX}

RUN curl -k http://artifacts.unidata.ucar.edu/content/repositories/unidata-releases/edu/ucar/tds/${THREDDS_VER}/tds-${THREDDS_VER}.war -o webapps/threddsCWT.war && \
      wget https://github.com/krallin/tini/releases/download/${TINI_VERSION}/tini -O /tini && \
      chmod +x /tini

COPY docker/thredds/setenv.sh bin/
COPY docker/thredds/entrypoint.sh entrypoint.sh
COPY docker/thredds/catalog.xml catalog.xml
COPY docker/thredds/threddsConfig.xml threddsConfig.xml
COPY docker/thredds/server.xml conf/

ENTRYPOINT ["/tini", "--"]

CMD ["/bin/bash", "./entrypoint.sh"]
