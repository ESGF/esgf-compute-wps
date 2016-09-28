FROM tomcat:7.0

RUN apt-get update
RUN apt-get install -y curl

RUN curl -o webapps/thredds.war http://artifacts.unidata.ucar.edu/content/repositories/unidata-releases/edu/ucar/tds/4.6.6/tds-4.6.6.war 

COPY setenv.sh bin/

RUN mkdir -p content/thredds && echo "<?xml version=\"1.0\"?>\n\
<catalog xmlns=\"http://www.unidata.ucar.edu/namespaces/thredds/InvCatalog/v1.0\" >\n\
	<service name=\"odap\" serviceType=\"OpenDAP\" base=\"/thredds/dodsC/\" />\n\
	<datasetScan name=\"Default\" path=\"test\" location=\"/data/\" >\n\
		<serviceName>odap</serviceName>\n\
	</datasetScan >\n\
</catalog>" >> content/thredds/catalog.xml
