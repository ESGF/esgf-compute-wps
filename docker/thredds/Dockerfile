FROM tomcat:7.0

RUN apt-get update
RUN apt-get install -y curl

RUN curl -o webapps/thredds.war http://artifacts.unidata.ucar.edu/content/repositories/unidata-releases/edu/ucar/tds/4.6.6/tds-4.6.6.war 

COPY setenv.sh bin/
