# ESGF Compute WPS

ESGF Compute is a Django application capable of providing compute resources to
the ESGF data stack. It leverages the 1.0.0 version of the [Web Processing Service](http://www.opengeospatial.org/standards/wps>) (WPS)
standard to process compute requests.

# Intallation

Both the Kubernetes and Docker Compose scripts will create a "_deploy" folder
in the current working directory where all the configuration files are stored.
See [django.properties](docker/common/django.properties) for configuration.

### Kubernetes

To deploy on Kubernetes there is a script to assist in the process.

```
git clone https://github.com/ESGF/esgf-compute-wps

cd esgf-compute-wps/docker

./deploy_kubernetes.sh --help
```

### Docker Compose

To deploy on docker there is a script to assist in the process.

```
git clone https://github.com/ESGF/esgf-compute-wps

cd esgf-compute-wps/docker

./deploy_compose.sh --help
```

### Bare Metal

Requirements:

* [Conda](https://conda.io/miniconda.html)
* [Yarn](https://yarnpkg.com/lang/en/docs/install/)
* [Celery Worker](http://docs.celeryproject.org/en/latest/userguide/workers.html)
* [PostgreSQL](https://www.postgresql.org/download/)

Evironment variables:

* OAUTH_CLIENT: 	OAuth2.0 Client value
* OAUTH_SECRET: 	OAuth2.0 Secret value
* CELERY_BROKER: 	Celery Broker URI
* CELERY_BACKEND: 	Celery Backend URI 
* POSTGRES_HOST: 	PostgreSQL server address
* POSTGRES_PASSWORD: 	PostgresSQL password
* REDIS_HOST: 		Redis Host URI

```
git clone https://github.com/ESGF/esgf-compute-wps

pushd esgf-compute-wps/compute/wps/webapp/

yarn install

./node_modules/.bin/webpack --config config/webpack.prod

popd

pushd esgf-compute-wps/compute

// Define required environment variables

export DJANGO_CONFIG_PATH="${PWD}/../docker/common/django.properties"

python manage.py collectstatic

python manage.py migrate

python manage.py server --host default

python manage.py processes --register

python manage.py capabilities

gunicorn -b 0.0.0.0:8000 --reload compute.wsgi 
```
