FROM tomcat:8.5

ARG TAG=4.6.10

RUN wget http://artifacts.unidata.ucar.edu/content/repositories/unidata-releases/edu/ucar/tds/$TAG/tds-$TAG.war \
	-O webapps/threddsCWT.war

COPY setenv.sh bin/
COPY entrypoint.sh entrypoint.sh
COPY catalog.xml catalog.xml
COPY threddsConfig.xml threddsConfig.xml

HEALTHCHECK CMD curl --silent --location --fail http://0.0.0.0:8080/threddsCWT || exit 1

ENTRYPOINT ["./entrypoint.sh"]
