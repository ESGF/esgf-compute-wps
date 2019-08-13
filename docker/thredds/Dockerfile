FROM tomcat:8.5

ARG THREDDS_VER
ARG JAVA_HEAP_INIT=512m
ARG JAVA_HEAP_MAX=1g
ARG TINI_VERSION=v0.18.0

ENV THREDDS_VER ${THREDDS_VER}
ENV TINI_VERSION ${TINI_VERSION}
ENV JAVA_HEAP_INIT ${JAVA_HEAP_INIT}
ENV JAVA_HEAP_MAX ${JAVA_HEAP_MAX}

ENV THREDDS_WAR_URL http://artifacts.unidata.ucar.edu/content/repositories/unidata-releases/edu/ucar/tds/${THREDDS_VER}/tds-${THREDDS_VER}.war

RUN curl -fSLk "${THREDDS_WAR_URL}" -o thredds.war && \
      unzip thredds.war -d ${CATALINA_HOME}/webapps/threddsCWT/ && \
      rm -rf thredds.war && \
      /bin/bash -c "cp -rf ${CATALINA_HOME}/webapps/threddsCWT/*.{gif,jpg,css} ${CATALINA_HOME}/webapps/ROOT/" && \
      sed -i.bak "s/<param-value>thredds/<param-value>threddsCWT/g" ${CATALINA_HOME}/webapps/threddsCWT/WEB-INF/web.xml
  
RUN curl -fSLk https://github.com/krallin/tini/releases/download/${TINI_VERSION}/tini -o /usr/local/bin/tini && \
      chmod +x /usr/local/bin/tini

RUN mkdir -p ${CATALINA_HOME}/content/thredds

COPY docker/thredds/threddsConfig.xml ${CATALINA_HOME}/content/thredds/threddsConfig.xml
COPY docker/thredds/catalog.xml ${CATALINA_HOME}/content/thredds/catalog.xml

COPY docker/thredds/setenv.sh ${CATALINA_HOME}/bin/setenv.sh

COPY docker/thredds/server.xml ${CATALINA_HOME}/conf/server.xml

COPY docker/thredds/entrypoint.sh entrypoint.sh

ENTRYPOINT ["tini", "--"]

CMD ["/bin/bash", "./entrypoint.sh"]
