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
In addition to disabling health/readiness checks and persistent storage, some containers can be set to a development mode where their default commands are replaced with `/bin/sleep infinity`. This allows for live development within the pods container. A directory `/devel` is created and shared between containers as well. See [development.yaml](docker/helm/compute/development.yaml) for more information.

### Prometheus
* [External](#external)
* [Prometheus Helm Chart](#prometheus-helm-chart)

#### External
In a production environment you may already have an existing Prometheus server. This server will need to be configured to scrap the service endpoints in the Kubernetes cluster.

If you wish to deploy a Prometheus server with Helm there are some included files to assist in this; [prometheus.yaml](docker/helm/compute/prometheus.yaml) and [prometheus-storage.yaml](docker/helm/compute/prometheus-storage.yaml).

`celery.prometheusUrl` will need to be set to the base address of the prometheus server e.g. `https://internal.prometheus.com/`.
#### Prometheus Helm Chart
If a development environment is being deployed, the Helm chart has an included Prometheus server. The following command can be used when deploying; `helm install . -f development.yaml -f prometheus-dev.yaml`. This will deploy the Prometheus chart without any persistent storage. This can be combined with the development Traefik configuration; `helm install . -f development.yaml -f traefik-dev.yaml -f prometheus-dev.yaml`.

### Traefik
The ESGF Compute service utilizes two instances of Traefik; an external and internal instance. In a production environment [traefik.yaml](docker/helm/compute/traefik.yaml) can be used to deploy the external instance. The Helm chart will take care of deploying the internal instance.

In a development environment [traefik-dev.yaml](docker/helm/compute/traefik-dev.yaml) can be used to have the internal instance act as both the external and internal instances. When deploying the chart you can using the following command to deploy `helm install . -f development.yaml -f traefik-dev.yaml`, this will configure the Traefik instance to handle both external and internal Traefik. This can be combined with the development Prometheus server as well; `helm install . -f development.yaml -f traefik-dev.yaml -f prometheus-dev.yaml`.


# Design documents
Coming soon.
