################
ESGF Compute WPS
################

ESGF Compute WPS is a Django application providing access to computational
resources through 
`Web Processing Service <http://www.opengeospatial.org/standards/wps>`_ (WPS) 
interface standard. The application provides a WPS 1.0.0 interface through
`PyWPS <https://github.com/geopython/pywps>`_.

Intallation
###########
::

    pip install -r requirements.txt

Quickstart
##########
::

    python compute/manage.py runserver 0.0.0.0:8000

Docker
######

Provided are several docker files and docker-compose files.

****************************
Standalone Django WPS server
****************************

You can access the demo page at http://0.0.0.0:8000

::

    docker pull jasonb87/esgf_wps
    
    docker run -d -p 8000:8000 esgf_wps

***************************
Django WPS server w/THREDDS
***************************

Django WPS and THREDDS will share a volume at /data which points to your home
directory.

Django WPS can be accessed at http://0.0.0.0:8000
THREDDS can be accessed at http://0.0.0.0:8080/thredds

::

    cd docker/thredds

    docker-compose up -d

*************************************
Django WPS server w/THREDDS & Ophidia
*************************************

Django WPS, THREDDS and Ophidia server. They all share the volume /data.

Django WPS can be accessed at http://0.0.0.0:8000
THREDDS can be accessed at http://0.0.0.0:8080/thredds

::

    cd docker/ophidia

    docker-compose up -d

Ophidia Terminal can be accessed by

::
    
    cd docker/ophidia

    docker-compose exec ophidia oph_term -H 127.0.0.1 -P 11732 -u oph-test -p abcd
