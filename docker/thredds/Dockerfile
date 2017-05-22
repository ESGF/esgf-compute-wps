FROM tomcat:latest

RUN wget http://artifacts.unidata.ucar.edu/content/repositories/unidata-releases/edu/ucar/tds/4.6.10/tds-4.6.10.war \
	-O webapps/thredds.war

COPY setenv.sh bin/

COPY entrypoint.sh entrypoint.sh

RUN chmod +x entrypoint.sh

EXPOSE 8080

ENTRYPOINT ["./entrypoint.sh"]

CMD ["catalina.sh", "run"]
