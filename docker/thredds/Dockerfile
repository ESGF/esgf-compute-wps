FROM tomcat:8.5

RUN wget http://artifacts.unidata.ucar.edu/content/repositories/unidata-releases/edu/ucar/tds/4.6.10/tds-4.6.10.war \
	-O webapps/threddsCWT.war

COPY setenv.sh bin/

COPY entrypoint.sh entrypoint.sh

COPY catalog.xml catalog.xml

COPY server.xml conf/server.xml

EXPOSE 8080

HEALTHCHECK CMD curl --silent --location --fail http://0.0.0.0:8080/threddsCWT || exit 1

ENTRYPOINT ["./entrypoint.sh"]
