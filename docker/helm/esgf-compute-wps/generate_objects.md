# Generating Kubernetes object files

1. Install Helm
   1. https://docs.helm.sh/using_helm/#installing-helm
2. `cd esgf-compute-wps/docker/helm/esgf-compute-wps`
3. Edit [values.yaml](docker/helm/esgf-compute-wps/values.yaml)
4. `mkdir kubernetes`
5. `helm template --name compute --namespace default --output-dir kubernetes -f values.yaml .`
6. `kubectl apply -f kubernetes/*/*.yaml`
