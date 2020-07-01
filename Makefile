.PHONY: provisioner tasks wps thredds build integration-tests docker-buildctl prune-cache

GIT_BRANCH := $(shell git rev-parse --abbrev-ref HEAD)
GIT_COMMIT := $(shell git rev-parse --short HEAD)

IMAGE = $(if $(REGISTRY),$(REGISTRY)/)$(NAME)
VERSION = $(shell cat $(DOCKERFILE)/VERSION)
TAG = $(or $(if $(findstring $(GIT_BRANCH),master),$(VERSION)),$(VERSION)_$(GIT_BRANCH)_$(GIT_COMMIT))

CONDA_VERSION ?= 4.8.2
TARGET ?= production
OUTPUT_PATH ?= $(PWD)/output
CACHE ?= local
CACHE_PATH ?= $(PWD)/cache
TEST_DATA_PATH ?= $(PWD)/test_data

ifeq ($(CACHE),local)
CACHE_ARG := --import-cache type=local,src=$(CACHE_PATH) \
	--export-cache type=local,dest=$(CACHE_PATH),mode=max
else ifeq ($(CACHE),remote)
# Evaluate when used to populate $(IMAGE)
CACHE_ARG = --import-cache type=registry,ref=$(IMAGE):cache \
	--export-cache type=registry,ref=$(IMAGE):cache,mode=max
endif

DOCKER_BUILDCTL := docker run -it --rm \
	--privileged \
	--group-add $(shell id -g) \
	-v $(PWD):$(PWD) \
	-w $(PWD) \
	--entrypoint=/bin/sh \
	moby/buildkit:master

# Is buildctl-daemonless.sh available?
ifeq ($(shell which buildctl-daemonless.sh 2>/dev/null),)
BUILD := $(DOCKER_BUILDCTL)
else
BUILD := $(SHELL)
endif

ifneq ($(or $(findstring $(TARGET),testresult),$(findstring $(TARGET),testdata)),)
OUTPUT_ARG := --output type=local,dest=$(OUTPUT_PATH)
else
ifeq ($(shell which buildctl-daemonless.sh 2>/dev/null),)
OUTPUT_ARG = --output type=docker,name=$(IMAGE):$(TAG),dest=$(OUTPUT_PATH)/image.tar
POST_BUILD := cat $(OUTPUT_PATH)/image.tar | docker load
else
OUTPUT_ARG = --output type=image,name=$(IMAGE):$(TAG),push=true
endif
endif

EXTRA = --opt build-arg:CONTAINER_IMAGE=$(IMAGE):$(TAG) \
	--opt build-arg:CONDA_VERSION=$(CONDA_VERSION) \
	--opt build-arg:TEST_DATA_PATH=$(TEST_DATA_PATH)

# Default 2 days
BUILDCTL_KEEP_DURATION ?= 172800s
BUILDCTL_KEEP_STORAGE ?= 10240

docker-buildctl:
	$(DOCKER_BUILDCTL)

prune-cache:
	$(BUILD) /usr/bin/buildctl-daemonless.sh du

	$(BUILD) /usr/bin/buildctl-daemonless.sh prune \
		--all --keep-storage $(BUILDCTL_KEEP_STORAGE)
		# --keep-duration $(BUILDCTL_KEEP_DURATION)

integration-tests: CONDA := $(patsubst %/bin/conda,%,$(shell find /opt/**/bin $(HOME)/**/bin -type f -iname 'conda' 2>/dev/null))
integration-tests: CONDA_ENV := wps-integration-tests
integration-tests: CONDA_PKGS := esgf-compute-api pytest nbconvert xarray netcdf4 jupyter_client ipykernel dask distributed
integration-tests: CONDA_CHANNELS := -c conda-forge -c cdat
integration-tests: CONDA_ENV_EXISTS := $(shell conda env list | grep $(CONDA_ENV))
integration-tests:
	source $(CONDA)/bin/activate base; \
		conda env remove -n $(CONDA_ENV) || exit 0; \
		conda create -n $(CONDA_ENV) -y $(CONDA_CHANNELS) $(CONDA_PKGS)
	
	mkdir notebooks || exit 0

	source $(CONDA)/bin/activate $(CONDA_ENV); \
		python -m ipykernel install --user --name $(CONDA_ENV); \
		pytest compute/tests/test_runner.py --wps_kernel $(CONDA_ENV) --wps-url $(WPS_URL) --wps-token $(WPS_TOKEN)

provisioner: NAME	:= compute-provisioner
provisioner: DOCKERFILE := compute/compute_provisioner
provisioner: build
	echo -e "provisioner:\n  imageTag: $(TAG)" > update_provisioner.yaml

tasks: NAME := compute-tasks
tasks: DOCKERFILE := compute/compute_tasks
tasks: build
	echo -e "celery:\n  imageTag: $(TAG)" > update_tasks.yaml

wps: NAME := compute-wps
wps: DOCKERFILE := compute/compute_wps
wps: build
	echo -e "wps:\n  imageTag: $(TAG)" > update_wps.yaml

thredds: NAME := compute-thredds
thredds: DOCKERFILE := docker/thredds
thredds: build
	echo -e "thredds:\n  imageTag: $(TAG)" > update_thredds.yaml

build:
	$(SHELL) -c "if [[ ! -e '$(TEST_DATA_PATH)' ]]; then mkdir -p $(TEST_DATA_PATH); fi"
	$(SHELL) -c "if [[ ! -e '$(CACHE_PATH)' ]]; then mkdir -p $(CACHE_PATH); fi"
	$(SHELL) -c "if [[ ! -e '$(OUTPUT_PATH)' ]]; then mkdir -p $(OUTPUT_PATH); fi"

	$(BUILD) build.sh $(DOCKERFILE) $(TARGET) $(EXTRA) $(CACHE_ARG) $(OUTPUT_ARG)
	
	$(POST_BUILD)
