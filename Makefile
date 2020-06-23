.PHONY: provisioner tasks wps thredds build integration-tests

export

SHELL = /bin/bash

IMAGE := $(if $(REGISTRY),$(REGISTRY)/)$(IMAGE)
TAG ?= $(or $(if $(shell git rev-parse --abbrev-ref HEAD | grep master),$(shell cat $(DOCKERFILE)/VERSION)),$(shell git rev-parse --short HEAD))
TARGET ?= production
CONDA_VERSION ?= 4.8.2
CACHE ?= local
CACHE_PATH ?= /cache
CACHE_REGISTRY == $(REGISTRY)
OUTPUT_PATH ?= output

CONDA_TEST_ENV = wps-integration-tests
CONDA = $(patsubst %/bin/conda,%,$(shell find /opt/**/bin ${HOME}/**/bin -type f -iname 'conda' 2>/dev/null))

ifeq ($(CACHE),local)
CACHE_ARG = --import-cache type=local,src=$(CACHE_PATH) \
	--export-cache type=local,dest=$(CACHE_PATH),mode=max
else ifeq ($(CACHE),remote)
CACHE_ARG = --import-cache type=registry,ref=$(IMAGE):cache \
	--export-cache type=registry,ref=$(IMAGE):cache,mode=max
endif

ifeq ($(shell which buildctl-daemonless.sh),)
OUTPUT = --output type=docker,name=$(IMAGE):$(TAG),dest=/output/image.tar
POST_BUILD = cat output/image.tar | docker load
BUILD =  docker run \
				 -it --rm \
				 --privileged \
				 -v $(PWD)/test_data:/test_data \
				 -v $(PWD)/cache:$(CACHE_PATH) \
				 -v $(PWD)/output:/output \
				 -v $(PWD):/build \
				 -w /build \
				 --entrypoint=/bin/sh \
				 moby/buildkit:master 
else
OUTPUT = --output type=image,name=$(IMAGE):$(TAG),push=true
BUILD = $(SHELL)
endif

ifneq ($(or $(findstring testresult,$(TARGET)),$(findstring testdata,$(TARGET))),)
ifeq ($(shell which buildctl-daemonless.sh),)
OUTPUT = --output type=local,dest=/output
else
OUTPUT = --output type=local,dest=$(OUTPUT_PATH)
endif
endif

EXTRA = --opt build-arg:CONTAINER_IMAGE=$(IMAGE):$(TAG) \
	--opt build-arg:CONDA_VERSION=$(CONDA_VERSION) 

integration-tests:
	source $(CONDA)/bin/activate base; \
		[[ "$(conda env list | grep $(CONDA_TEST_ENV))" ]] || \
		conda create -n $(CONDA_TEST_ENV) -y -c conda-forge -c cdat \
		esgf-compute-api pytest nbconvert nbformat xarray netcdf4 jupyter_client ipykernel ; \
		mkdir notebooks/ || exit 0

	source $(CONDA)/bin/activate $(CONDA_TEST_ENV); \
		python -m ipykernel install --user --name $(CONDA_TEST_ENV); \
		pytest compute/tests/test_runner.py --wps-kernel $(CONDA_TEST_ENV) \
		--wps-url $(WPS_URL) --wps-token $(WPS_TOKEN)

provisioner: IMAGE := compute-provisioner
provisioner: DOCKERFILE := compute/compute_provisioner
provisioner:
	$(MAKE) build	

tasks: IMAGE := compute-tasks
tasks: DOCKERFILE := compute/compute_tasks
tasks:
	mkdir -p compute/compute_tasks/test_data

	$(MAKE) build	

wps: IMAGE := compute-wps
wps: DOCKERFILE := compute/compute_wps
wps:
	$(MAKE) build	

thredds: IMAGE := compute-thredds
thredds: DOCKERFILE := docker/thredds
thredds:
	$(MAKE) build	

build:
	$(BUILD) build.sh $(DOCKERFILE) $(TARGET) $(EXTRA) $(CACHE_ARG) $(OUTPUT)
	
	$(POST_BUILD)
