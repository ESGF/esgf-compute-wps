  WPCDAS:  Web Processing Climate Data Analysis Services

WPS API leveraging UVCDAT for server-side climate data analytics with clients for web, python, etc.
The inital version of the WPS server was based on code developed by Charles Doutriaux, LLNL.

Package installation:

  >> sudo mkdir /usr/local/web
  >> sudo chown <username> /usr/local/web
  >> cd /usr/local/web
  >> git checkout git@github.com:ThomasMaxwell/CDS-WPS-AaaS.git WPCDAS
  >> chmod -R a+rX ./WPCDAS
  >> chmod -R a+w ./WPCDAS/server/logs

WPS Server Requirements:

    UVCDAT:  http://uvcdat.llnl.gov/

    Once UVCDAT is installed the following packages should be installed using UVCDAT's python:

    lxml:    http://lxml.de/
    Django:  https://www.djangoproject.com/
    django-corsheaders:  https://github.com/OttoYiu/django-cors-headers


Client Requirements:

     The web client is executed using an HTTP server such as Apache or CherryPy.
     For Apache, edit the /usr/local/apache2/conf/httpd.conf file:
       Set Listen to 8001
       Set DocumentRoot to "/usr/local/web/WPCDAS/clients/web/htdocs"

Service Initialization:

  Setup the UVCDAT environment:

  >> source .../install/bin/setup_runtime.sh

  Start the WPS service with the command:

  >> cd /usr/local/web/WPCDAS/server; python manage.py runserver

  Start the apache server with the command:

  >> sudo /usr/local/apache2/bin/apachectl -k start

Testing:

  WPS service test URLs can be found at: http://localhost:8001/

Web Client:

     A test plot can be accessed at http://localhost:8001/cdas/.
     Clicking on the map initiates a WPS call which retrieves a timeseries at the chosen location and then plots it.

