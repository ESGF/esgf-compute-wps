# Docker

### Docker Compose

1. Install [docker](https://docs.docker.com/install/) and [docker compose](https://docs.docker.com/compose/install/).
2. Execute `./deploy_compose.sh` in a terminal.

Production | Development
-----------|------------
`./deploy_compose.sh --host $(hostname -i)` | `./deploy_compose.sh --host $(hostname -i) --dev`

**Note**: If you run compose in development mode you will need to execute the following in two separate shells, `docker-compose exec wps bash entrypoint.sh` and `docker-compose exec celery bash entrypoint.sh -l info`. This is done to handle cases where Django stops serving files and needs to be restarted. Normally you'd have to relaunch the container and lose current work.

### Kubernetes

1. Install [kubernetes](https://kubernetes.io/docs/setup/).
  * The preferred way to launch a single node cluster is using [minikube](https://kubernetes.io/docs/tasks/tools/install-minikube/), for production look into [kubeadm](https://kubernetes.io/docs/setup/independent/install-kubeadm/).
2. Execute `./deploy_kubernetes.sh` in a terminal.
