FROM tomcat:8.5

RUN wget http://artifacts.unidata.ucar.edu/content/repositories/unidata-releases/edu/ucar/tds/4.6.10/tds-4.6.10.war \
	-O webapps/threddsCWT.war

COPY setenv.sh bin/

COPY entrypoint.sh entrypoint.sh

COPY catalog.xml catalog.xml

EXPOSE 8080

ENTRYPOINT ["./entrypoint.sh"]
