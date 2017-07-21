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

Requirements:

- Conda
- Celery Worker w/Redis
- PostgreSQL database

::

    git clone https://github.com/ESGF/esgf-compute-wps

    cd esgf-compute-wps/

    conda create -n esgf_wps -c conda-forge -c uvcdat esgf-compute-api cdms2 cdutil genutil pyzmq lxm

    pip install -r requirements.txt

    python compute/manage.py migrate

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
