# ESGF Compute
The ESGF Compute Service is a containerized application capable of providing compute resources through a web service, using the [Web Processing Service (WPS)](http://www.opengeospatial.org/standards/wps) standard as an interface between the user and the compute resources. Currently version 1.0.0 of the WPS standard is supported, with 2.0.0 in the near future.

The end goal is to provide a federated service for ESGF that brings the computation to the data.

Table of Contents
=================

* [Contribute](#contribute)
* [Question?](#question)
* [Installation](#installation)

# Contribute
We welcome contributions to the project, before moving ahead please review the following documents:

* [Contributing Guide](CONTRIBUTING.md)
* [Developers Guide](DEVELOPER.md)

# Question?
Please review the [FAQ](FAQ.md), if you do not find an answer to your question open an issue on [GitHub](https://github.com/ESGF/esgf-compute-wps/issues/new).

# Installation

## Requirements
* [Kubernetes](#kubernetes)
* [Helm](#helm)
* [Traefik](https://docs.traefik.io/)
* [Prometheus](https://prometheus.io/docs/prometheus/latest/installation/)

### Kubernetes

Kubernetes is required to run the ESGF Compute service, installation instructions can be found on the [Kubernetes](https://kubernetes.io/docs/setup/pick-right-solution/).

### Helm

[Helm](https://helm.sh/) is the method of deployment for Kubernetes, installation instructions can be found on their [website](https://helm.sh/docs/using_helm/#install-helm).

* [Configuration](#configuration)
* [Deployment with Tiller](#deployment-with-tiller)
* [Deployment without Tiller](#deployment-without-tiller)

#### Configuration
There are some pre-configured environment files available. Review the [storage](#storage) section before deploying.

**NOTE:** The following pre-configured environments may be missing some required values. If you experience an error similar to ```Error: render error in "compute/templates/celery-deployment.yaml": template: compute/templates/celery-deployment.yaml:167:20: executing "compute/templates/celery-deployment.yaml" at <required "Set celery...>: error calling required: Set celery.prometheusUrl``` then a required value is missing. The last portion of the error ```required: Set celery.prometheusUrl``` will have which value is missing. In this example the following value needs to be set.
```yaml
celery:
  prometheusUrl:
```

* [production.yaml](docker/helm/compute/production.yaml) environment has defined the container resource requirements. Using this on a single node or small cluster may have adverse effects as the resource requirements may be larger than available resources. Persistent storage is enabled.
* [development.yaml](docker/helm/compute/development.yaml) environment does not define any container resource requirements and disables pod health and readiness checks. This environment is prefered for single node or small clusters. Persistent storage is disabled. You can find further information about this environment [here](#development)
* [development-resources.yaml](docker/helm/compute/development-resources.yaml) environment is the same as development.yaml but has defined the container resource requirements. **NOTE:** This may be renamed in the near future.

All of the base configuration values for the helm chart can be found in [values.yaml](docker/helm/compute/values.yaml).

##### Storage
The Helm chart will automatically create all required [PersistentVolumes (PV)](https://kubernetes.io/docs/concepts/storage/persistent-volumes/#persistent-volumes) and [PersistentVolumeClaims (PVC)](https://kubernetes.io/docs/concepts/storage/persistent-volumes/#persistentvolumeclaims). The PVs that are created using HostPath as the storage type. If deploying on a multi-node Kubernetes cluster, the usage of [nodeSelector](https://kubernetes.io/docs/concepts/configuration/assign-pod-node/#nodeselector) and labeling nodes will be required to ensure persistent storage. The following services use persistent storage in a production environment; Postgres and Redis.

Example:
```yaml
postgresql:
  master:
    nodeSelector:
      storage/postgresql: persist
      
redis:
  master:
    nodeSelector:
      storage/redis: persist
```

To disable the creation of the PVs, the path value must be deleted by setting it to ```null```.

Example:
```yaml
persistence:
  dataClaimName: data-pvc

  volumes:
    data:
      storageClassName: slow
      capacity: 100Gi
      path: /p/cscratch/nimbus/wps/data
```
to
```yaml
persistence:
  dataClaimName: data-pvc

  volumes:
    data:
      storageClassName: slow
      capacity: 100Gi
      path: null
```

#### Deployment with Tiller
Either choose an existing [environment](#configuration) or create a custom one.

1. `git clone https://github.com/ESGF/esgf-compute-wps/`
2. `cd docker/helm/compute`
3. `helm dependency update`
4. `helm install . -f <environement>.yaml` will install the chart in the current Kubernetes namespace.
2. Verify install with `helm list` and `kubectl get pods`, see [sample](#sample-output).

##### Deployment without Tiller
Either choose an existing [environment](#configuration) or create a custom one.

1. `git clone https://github.com/ESGF/esgf-compute-wps/`
2. `cd docker/helm/compute`
3. `helm dependency update`
4. `helm template . -f production.yaml --output-dir rendered/` will render the templates and create files for Kubernetes.
5. `kubectl apply -k rendered/` will apply all of the files to the current namespace.
6. Verify install with `helm list` and `kubectl get pods`, see [sample](#sample-output).

#### Sample output 
The following is sample out from "helm list" and "kubectl get pods"
##### Helm list output
```
NAME                    REVISION        UPDATED                         STATUS          CHART           NAMESPACE
laughing-condor         1               Thu May  9 21:49:10 2019        DEPLOYED        traefik-1.68.1  default
precise-alligator       1               Tue Jun 18 00:10:49 2019        DEPLOYED        compute-1.0.0   default
```

##### Kubectl get pods
```
NAME                                                        READY   STATUS    RESTARTS   AGE
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
In addition to disabling health/readiness checks and persistent storage, some containers can be set to a development mode where their default commands are replaced with `/bin/sleep infinity`. This allows for live development within the pods container. A directory /devel is created and shared between containers as well, so code may be shared between the containers.

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
2. Edit [prometheus-storage.yaml](docker/helm/compute/prometheus-storage.yaml) to configure the storage for Prometheus. Two PersistentVolumes are required; one for the server and a second for the alert-manager.

**Note** If dynamic provisioning of storage is required please refer to the Prometheus [chart documentation](https://github.com/helm/charts/tree/master/stable/prometheus)

3. `kubectl apply -f prometheus-storage.yaml`
3. Edit [prometheus.yaml](docker/helm/compute/prometheus.yaml) to configure the chart. See the [chart documentation](https://github.com/helm/charts/tree/master/stable/prometheus) for information on configuring the chart.
4. `helm install stable/prometheus -f prometheus.yaml`

or

4. `helm template stable/prometheus -f prometheus.yaml --output-dir rendered-prometheus/`
5. `kubectl apply -k rendered-prometheus/`

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
