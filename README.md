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
We support deployment by [Helm](https://helm.sh/), Tiller is optional.

### Requirements
* [Kubernetes Cluster](https://kubernetes.io/docs/setup/pick-right-solution/)
* [Helm Installation](https://helm.sh/docs/using_helm/#install-helm)
* [Tiller (optional)](https://helm.sh/docs/using_helm/#installing-tiller)

### Configuration
Refer to [values.yaml](docker/helm/compute/values.yaml) for chart defaults.

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
#### Helm with Tiller
1. `cd docker/helm/compute`
2. Create a production.yaml with any custom settings. Refer to the [values.yaml](docker/helm/compute/values.yaml) for possible values. 
3. `helm install . -f production.yaml`
4. Verify the installion with `helm list` and `kubectl get pods`.

#### Helm standalone
1. `cd docker/helm/compute`
2. Create a production.yaml with any custom settings. Refer to the values.yaml for possible values. 
3. `helm template . -f production.yaml -n UNIQUE_NAME --output-dir ${PWD}/output`
4. `kubectl apply -k ${PWD}/output`
5. Verify the installion with `helm list` and `kubectl get pods`.

### Development
If you set `development: true` in your config.yaml then the WPS and Celery containers will be
started with `sleep infinity` rather than their default commands. This provides the ability to
develop in these containers. See the following comments on how to run the default commands for
each container.

##### WPS

The default command is `bash /entrypoint.sh`.

##### Celery

The default command is `bash /entrypoint.sh -c 2 -Q ingress -n ingress -l DEBUG`.
