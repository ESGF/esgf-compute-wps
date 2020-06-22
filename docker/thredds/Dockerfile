FROM tomcat:8.5 as production

ENV THREDDS_VER 4.6.14
ENV TINI_VERSION v0.18.0
ENV JAVA_HEAP_INIT 512m
ENV JAVA_HEAP_MAX 1g

ENV THREDDS_WAR_URL http://artifacts.unidata.ucar.edu/content/repositories/unidata-releases/edu/ucar/tds/${THREDDS_VER}/tds-${THREDDS_VER}.war

RUN curl -fSLk "${THREDDS_WAR_URL}" -o thredds.war && \
      unzip thredds.war -d ${CATALINA_HOME}/webapps/threddsCWT/ && \
      rm -rf thredds.war && \
      mkdir -p ${CATALINA_HOME}/webapps/ROOT && \
      /bin/bash -c "cp -rf ${CATALINA_HOME}/webapps/threddsCWT/*.{gif,jpg,css} ${CATALINA_HOME}/webapps/ROOT/" && \
      sed -i.bak "s/<param-value>thredds/<param-value>threddsCWT/g" ${CATALINA_HOME}/webapps/threddsCWT/WEB-INF/web.xml
  
RUN curl -fSLk https://github.com/krallin/tini/releases/download/${TINI_VERSION}/tini -o /usr/local/bin/tini && \
      chmod +x /usr/local/bin/tini

RUN mkdir -p ${CATALINA_HOME}/content/thredds

# Thredds configuration
COPY threddsConfig.xml ${CATALINA_HOME}/content/thredds/threddsConfig.xml
COPY catalog.xml ${CATALINA_HOME}/content/thredds/catalog.xml
# Tomcat configuration
COPY setenv.sh ${CATALINA_HOME}/bin/setenv.sh
COPY server.xml ${CATALINA_HOME}/conf/server.xml
# Entrypoint
COPY entrypoint.sh entrypoint.sh

ENTRYPOINT ["/usr/local/bin/tini", "--"]

CMD ["/bin/bash", "-c", "./entrypoint.sh ${CATALINA_HOME}/bin/catalina.sh run"]
