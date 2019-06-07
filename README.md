# ESGF Compute

ESGF Compute is a WPS application capable of providing remote compute resources. 

The end goal is to provide a federated service that brings the computation to the data.

### Services

1. [Compute WPS](compute/wps) This is a Django based implmenetation of the [Web Processing Service](http://www.opengeospatial.org/standards/wps)(WPS) standard. It currently only supports 1.0.0.

# Contribute

We welcome contributions to the project, before moving ahead please review the following documents:

* [Contributing Guide](CONTRIBUTING.md)
* [Developers Guide](DEVELOPER.md)

# Intallation

We only support deployment by [Helm](https://helm.sh/).

### Requirements

* [Kubernetes Cluster](https://kubernetes.io/docs/setup/pick-right-solution/)
* [Helm Installation](https://helm.sh/docs/using_helm/)

### Deployment

1. `cd docker/helm/esgf-compute-wps`
2. Create a config.yaml with any custom settings. Refer to the values.yaml for all defaults.
3. `helm install . -f config.yaml`
4. Verify the installion with `helm list` and `kubectl get pods`.

### Development

If you set `development: true` in your config.yaml then the WPS and Celery containers will be
started with `sleep infinity` rather than their default commands. This provides the ability to
develop in these containers. See the following comments on how to run the default commands for
each container.

##### WPS

The default command is `bash /entrypoint.sh`.

##### Celery

The default command is `bash /entrypoint.sh -c 2 -Q ingress -n ingress -l DEBUG`.
