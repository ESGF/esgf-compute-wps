# ESGF Compute
ESGF Compute is a WPS application capable of providing remote compute resources. 

The end goal is to provide a federated service that brings the computation to the data.

### Services
* [Compute WPS](compute/compute-wps) This is a Django based implmenetation of the [Web Processing Service](http://www.opengeospatial.org/standards/wps)(WPS) standard. It currently only supports 1.0.0.

# Contribute
We welcome contributions to the project, before moving ahead please review the following documents:

* [Contributing Guide](CONTRIBUTING.md)
* [Developers Guide](DEVELOPER.md)

# Intallation
We support deployment by [Helm](https://helm.sh/), the use of Tiller is optional. The compute service is designed to run behind [Traefik](https://docs.traefik.io/) though it is possible to run it behind other reverse proxy/load balancers. Instructions to deploy a default Traefik installation will be described below. The service also uses a [Prometheus](https://prometheus.io/) server to collect metrics for the compute service. This can be installed by Helm or a bare metal install, instructions for Helm will be provided otherwise see the Prometheus documentation for installation instructions. Refer to the configuration section below for instructions on configuring the compute service to use the Prometheus server.

### Requirements
* [Kubernetes Cluster](https://kubernetes.io/docs/setup/pick-right-solution/)
* [Helm Installation](https://helm.sh/docs/using_helm/#install-helm)
* [Prometheus Installation](https://prometheus.io/docs/prometheus/latest/installation/)
* [Tiller (optional)](https://helm.sh/docs/using_helm/#installing-tiller)
* [Traefik (optional)](https://docs.traefik.io/)

### Configuration
Refer to comments in [values.yaml](docker/helm/compute/values.yaml) located in docker/helm/compute for chart defaults.

**NOTE** This helm chart will automatically create PersistentVolume (PV) and PersistentVolumeClaims (PVC) for shared storage. These PVs will use HostPath for the storage type. If dyanmic provisioning is preferred then the path for each volume will need to be removed and the correct StorageClass set. By removing the path variable the chart will not create the PVs and only create the PVCs allowing Kubernetes to create the volumes.

Change the following
```yaml
persistence:
  public:
    storageClassName: slow
    capacity: 100Gi
    path: /p/cscratch/nimbus/wps/data/public
```
to
```yaml
persistence:
  public:
    storageClassName: cephfs-default # Whatever storage class you plan to use
    capacity: 100Gi
    path: null
```

### Deployment

#### Production
1. Install the [Helm](https://helm.sh/docs/using_helm/#installing-helm) client for your chosen OS.
2. *Optional* Install [Tiller](https://helm.sh/docs/using_helm/#installing-tiller) server.
3. `git clone https://github.com/ESGF/esgf-compute-wps/`
4. `cd docker/helm/compute`
5. Create a production.yaml file, refer to values.yaml to customize the install.

**Note** There are some required values that must be set. Helm will raise an error when installing the chart or templating the files.

6. `helm dependency update` will pull all of the dependency charts.

##### Tiller
1. `helm install . -f production.yaml` will install the chart in the current namespace.
2. `helm list` should contain an entry for the compute chart.
```
NAME                    REVISION        UPDATED                         STATUS          CHART           NAMESPACE
laughing-condor         1               Thu May  9 21:49:10 2019        DEPLOYED        traefik-1.68.1  default
precise-alligator       1               Tue Jun 18 00:10:49 2019        DEPLOYED        compute-1.0.0   default
```

##### No Tiller
1. `helm template . -f production.yaml --output-dir rendered/` will render the templates and create files for Kubernetes.
2. `kubectl apply -k rendered/` will apply all of the files to the current namespace.
3. `kubectl get pods` should contain entries similar to the following.
```
NAME                                                        READY   STATUS    RESTARTS   AGE
laughing-condor-traefik-b9447cc8f-lhrwr                     1/1     Running   12         39d
precise-alligator-compute-celery-ingress-6f65c48f8c-fgxr9   3/3     Running   0          2m34s
precise-alligator-compute-kube-monitor-8458dd94d5-hxrrw     1/1     Running   0          2m34s
precise-alligator-compute-nginx-7967b9666d-jdnr9            1/1     Running   0          2m34s
precise-alligator-compute-provisioner-7b5c6fdc68-qwlmk      1/1     Running   0          2m34s
precise-alligator-compute-thredds-854864c74d-94zd9          1/1     Running   0          2m34s
precise-alligator-compute-wps-6d7b5dcc66-qbbvc              1/1     Running   0          2m34s
precise-alligator-postgresql-0                              1/1     Running   0          2m34s
precise-alligator-redis-master-0                            1/1     Running   0          2m33s
precise-alligator-traefik-857576cd87-djfhb                  1/1     Running   0          2m34s
```

#### Development
See [development.yaml](docker/helm/compute/development.yaml) for an example Helm chart configuration file.
This will launch the service in a development environment, essentially preventing services from running
normally. The WPS and Celery containers will be run with `/bin/sleep infinity` and a shared volume will be
mounted between the two pods. The shared volume can be used to clone the Github repo and install the python
packages in development mode, allowing for live code editing. Some of the pods will fail to start because 
their dependent pods will not have automatically started. Next let's get the pods running.

##### WPS Pod
1. `kubectl get pods --selector app.kubernetes.io/component=wps` use the value in the "Name" column.
2. `kubectl exec -it <NAME> /bin/bash`
3. `bash /entrypoint.sh`

##### Celery Pod
1. `kubectl get pods --selector app.kubernetes.io/component=celery` use the value in the "Name" column.
2. `kubectl exec -it <NAME> -c compute-celery-ingress /bin/bash`
3. `bash /entrypoint.sh -c 2 -Q ingress -n ingress -l INFO` this will start the Celery working using the ingress queue.

### Prometheus
The default [prometheus.yaml](docker/helm/compute/prometheus.yaml) will install a Prometheus server along with the default exporters e.g. Node Exporter, Kubernetes Export, etc. The services will be scheduled on Kubernetes nodes which have the label `tier=backend`.
1. `cd docker/helm/compute`
2. Edit [prometheus.yaml](docker/helm/compute/prometheus.yaml) to configure the chart. See the [chart repo](https://github.com/helm/charts/tree/master/stable/prometheus) for information on configuring.
3. `helm install stable/prometheus -f prometheus.yaml`

or

3. `helm template stable/prometheus -f prometheus.yaml --output-dir rendered-prometheus/`
4. `kubectl apply -k rendered-prometheus/`

### Traefik **(Optional)**
The default [traefik.yaml](docker/helm/compute/traefik.yaml) will install a Traefik server that will be scheduled on a Kubernetes node which has a label `tier=frontend`, it will bind HostPort 80 and 443 on the node and will automatically create self-signed certificates. See step 2 for further customization of the reverse proxy/load balancer.
1. `cd docker/helm/compute`
2. Edit [traefik.yaml](docker/helm/compute/traefik.yaml) to configure the chart. See the [chart repo](https://github.com/helm/charts/tree/master/stable/traefik) for information on configuring.
3. `helm install stable/traefik -f traefik.yaml`

or

3. `helm template stable/traefik -f traefik.yaml --output-dir rendered-traefik/`
4. `kubectl apply -k rendered-traefik/`

# Design documents
Coming soon.
