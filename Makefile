REGISTRY := aims2.llnl.gov
GIT_COMMIT := $(shell git log -n1 --pretty=%h)

.PHONY: provisioner
provisioner: PROJECT_DIR = compute_provisioner
provisioner: IMAGE_NAME = compute-provisioner
provisioner: buildkit

.PHONY: provisioner-docker
provisioner-docker: PROJECT_DIR = compute_provisioner
provisioner-docker: IMAGE_NAME = compute-provisioner
provisioner-docker: docker

.PHONY: tasks
tasks: PROJECT_DIR = compute_tasks
tasks: IMAGE_NAME = compute-tasks
tasks: buildkit

.PHONY: tasks-docker
tasks-docker: PROJECT_DIR = compute_tasks
tasks-docker: IMAGE_NAME = compute-tasks
tasks-docker: docker

.PHONY: wps
wps: PROJECT_DIR = compute_wps
wps: IMAGE_NAME = compute-wps
wps: buildkit
	
.PHONY: wps-docker
wps-docker: PROJECT_DIR = compute_wps
wps-docker: IMAGE_NAME = compute-wps
wps-docker: docker

.PHONY: thredds
thredds:
	buildctl-daemonless.sh build \
		--frontend dockerfile.v0 \
		--local context=. \
		--local dockerfile=docker/thredds \
		--output type=image,name=$(REGISTRY)/compute-thredds:$(GIT_COMMIT),push=true \
		--export-cache type=registry,ref=$(REGISTRY)/compute_thredds:cache \
		--import-cache type=registry,ref=$(REGISTRY)/compute_thredds:cache 

.PHONY: thredds-docker
thredds-docker:
	docker run \
		-it --privileged --rm \
		-v ${PWD}:/data -v ${HOME}/.docker/config.json:/root/.docker/config.json \
		-w /data \
		--entrypoint buildctl-daemonless.sh \
		moby/buildkit:master \
		build \
		--frontend dockerfile.v0 \
		--local context=. \
		--local dockerfile=docker/thredds \
		--output type=image,name=$(REGISTRY)/compute-thredds:$(GIT_COMMIT),push=true \
		--export-cache type=registry,ref=$(REGISTRY)/compute_thredds:cache \
		--import-cache type=registry,ref=$(REGISTRY)/compute_thredds:cache 

.PHONY: buildkit
buildkit:
	buildctl-daemonless.sh build \
		--frontend dockerfile.v0 \
		--local context=. \
		--local dockerfile=compute/$(PROJECT_DIR) \
		--opt build-arg:GIT_SHORT_COMMIT=$(GIT_COMMIT) \
		--output type=image,name=$(REGISTRY)/$(IMAGE_NAME):$(GIT_COMMIT),push=true \
		--export-cache type=registry,ref=$(REGISTRY)/$(IMAGE_NAME):cache \
		--import-cache type=registry,ref=$(REGISTRY)/$(IMAGE_NAME):cache 

.PHONY: docker
docker:
	docker run \
		-it --privileged --rm \
		-v ${PWD}:/data -v ${HOME}/.docker/config.json:/root/.docker/config.json \
		-w /data \
		--entrypoint buildctl-daemonless.sh \
		moby/buildkit:master \
		build \
		--frontend dockerfile.v0 \
		--local context=. \
		--local dockerfile=compute/$(PROJECT_DIR) \
		--opt build-arg:GIT_SHORT_COMMIT=$(GIT_COMMIT) \
		--output type=image,name=$(REGISTRY)/$(IMAGE_NAME):$(GIT_COMMIT),push=true \
		--export-cache type=registry,ref=$(REGISTRY)/$(IMAGE_NAME):cache \
		--import-cache type=registry,ref=$(REGISTRY)/$(IMAGE_NAME):cache 
