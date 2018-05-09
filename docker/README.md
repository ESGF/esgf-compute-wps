# Docker

### Docker Compose

1. Install [docker](https://docs.docker.com/install/) and [docker compose](https://docs.docker.com/compose/install/).
2. Execute `./deploy_compose.sh` in a terminal.
 * `./deploy_compose.sh --help` to display help.

Production | Development
-----------|------------
`./deploy_compose.sh start --host $(hostname -i)` | `./deploy_compose.sh start --host $(hostname -i) --dev`

**Note**: If you run compose in development mode you will need to execute the following in two separate shells, `docker-compose exec wps bash entrypoint.sh` and `docker-compose exec celery bash entrypoint.sh -l info`. This is done to handle cases where Django stops serving files and needs to be restarted. Normally you'd have to relaunch the container and lose current work.

### Helm

* Install [kubernetes](https://kubernetes.io/docs/setup/).
* Install [helm](https://github.com/kubernetes/helm/docs/install.md).
  * Install [tiller](https://github.com/kubernetes/helm/docs/install.md#installing-tiller)
* Deploy the helm chart with the following:
  * Check values.yaml for configuration options.
  * ```bash
    cd docker/helm/esgf-compute-wps
    helm install .
    ```
    
### Kubernetes

* Install [kubernetes](https://kubernetes.io/docs/setup/).
  * The preferred way to launch a single node cluster is using [minikube](https://kubernetes.io/docs/tasks/tools/install-minikube/), for production look into [kubeadm](https://kubernetes.io/docs/setup/independent/install-kubeadm/).
* Execute `./deploy_kubernetes.sh start --host $(hostname -i)` in a terminal.
  * `./deploy_kubernetes.sh --help` to display help.

< *note*: When deploying at LLNL must edit the trafik-configmap and add the minimum tls version, allowed cipher suites and a redirect rule for the https enpoint from / to /wps/home.

### Bare Metal

##### Requirements:

* [Conda](https://conda.io/miniconda.html)
* [Yarn](https://yarnpkg.com/lang/en/docs/install/)
* [Celery Worker](http://docs.celeryproject.org/en/latest/userguide/workers.html)
* [PostgreSQL](https://www.postgresql.org/download/)
* [Redis](https://redis.io/topics/quickstart)
* [NGINX](https://www.nginx.com/resources/wiki/start/topics/tutorials/install/) or [Apache](https://httpd.apache.org/docs/trunk/install.html)

##### Static files:

These are collected in /var/www/static which you will need to point NGINX or Apache
to serve.

##### Celery worker:

```
cd esgf-compute-wps/compute

// Launch a Celery worker
celery worker -A compute -b $CELERY_BROKER -l info

// Launch a Celery beat worker
celery worker -A compute -b $CELERY_BROKER -l info -B
```

##### Evironment variables:

* OAUTH_CLIENT: 	OAuth2.0 Client value
* OAUTH_SECRET: 	OAuth2.0 Secret value
* CELERY_BROKER: 	Celery Broker URI
* CELERY_BACKEND: 	Celery Backend URI
* POSTGRES_HOST: 	PostgreSQL server address
* POSTGRES_PASSWORD: 	PostgresSQL password
* REDIS_HOST: 		Redis Host URI

##### Install:

```
git clone https://github.com/ESGF/esgf-compute-wps

pushd esgf-compute-wps/compute/wps/webapp/

yarn install

./node_modules/.bin/webpack --config config/webpack.prod

popd

pushd esgf-compute-wps/

export DJANGO_CONFIG_PATH="${PWD}/docker/common/django.properties"

conda env create --name wps --file docker/common/environment.yml

source activate wps

popd

// Define required environment variables

pushd esgf-compute-wps/compute

python manage.py collectstatic

python manage.py migrate

python manage.py server --host default

python manage.py processes --register

python manage.py capabilities

// Launch using gUnicorn
gunicorn -b 0.0.0.0:8000 --reload compute.wsgi
// or
// Launch using bjoern
python app.py "0.0.0.0" "8000"
```
