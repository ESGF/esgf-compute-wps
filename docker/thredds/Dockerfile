ARG BASE_IMAGE
FROM ${BASE_IMAGE} as builder

WORKDIR /build

ARG TINI_VERSION=v0.19.0
ARG TINI_SHA256=93dcc18adc78c65a028a84799ecf8ad40c936fdfc5f2a57b1acda5a8117fa82c
ARG TINI_URL=https://github.com/krallin/tini/releases/download/${TINI_VERSION}/tini

RUN curl -fSLk ${TINI_URL} -o tini && \
      echo "${TINI_SHA256} tini" | sha256sum -c --strict && \
      chmod +x tini

ARG TDS_VERSION=5.0.0-beta8
ARG TDS_SHA1=fa1d6e4c2f82bab406ca9da7b7bb20693ae013ef
ARG TDS_URL=http://artifacts.unidata.ucar.edu/content/repositories/unidata-releases/edu/ucar/tds/${TDS_VERSION}/tds-${TDS_VERSION}.war

RUN curl -fSLk ${TDS_URL} -o thredds.war && \
      echo "${TDS_SHA1} thredds.war" | sha1sum -c --strict && \
      unzip thredds.war -d thredds/

FROM ${BASE_IMAGE} as production

COPY --from=builder /build/tini /usr/local/bin/tini
COPY --from=builder /build/thredds ${CATALINA_HOME}/webapps/threddsCWT

COPY threddsConfig.xml ${CATALINA_HOME}/content/thredds/
COPY catalog.xml ${CATALINA_HOME}/content/thredds/

RUN sed -i.bak "s/<param-value>thredds/<param-value>threddsCWT/g" ${CATALINA_HOME}/webapps/threddsCWT/WEB-INF/web.xml && \
      mkdir -p ${CATALINA_HOME}/content/thredds/logs && \
      ln -sf /dev/stdout ${CATALINA_HOME}/content/thredds/logs/cache.log && \
      ln -sf /dev/stdout ${CATALINA_HOME}/content/thredds/logs/catalogInit.log && \
      ln -sf /dev/stdout ${CATALINA_HOME}/content/thredds/logs/featureCollectionScan.log && \
      ln -sf /dev/stdout ${CATALINA_HOME}/content/thredds/logs/fmrc.log && \
      ln -sf /dev/stdout ${CATALINA_HOME}/content/thredds/logs/httpout.log && \
      ln -sf /dev/stdout ${CATALINA_HOME}/content/thredds/logs/serverStartup.log && \
      ln -sf /dev/stdout ${CATALINA_HOME}/content/thredds/logs/threddsServlet.log


COPY setenv.sh ${CATALINA_HOME}/bin/
COPY server.xml ${CATALINA_HOME}/conf/

VOLUME /data/public

ENTRYPOINT ["/usr/local/bin/tini", "--"]

CMD ["/bin/bash", "-c", "${CATALINA_HOME}/bin/catalina.sh run"]
