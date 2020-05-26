.PHONY: provisioner tasks wps thredds build test-env

export

SHELL = /bin/bash

IMAGE := $(if $(REGISTRY),$(REGISTRY)/)$(IMAGE)
TAG := $(or $(if $(shell git rev-parse --abbrev-ref HEAD | grep master),$(shell cat $(DOCKERFILE)/VERSION)),$(shell git rev-parse --short HEAD))
CACHE := local
TARGET = production
CONDA_VERSION = 4.8.2

CONDA_TEST_ENV = base
CONDA = $(patsubst %/bin/conda,%,$(shell find /opt/**/bin ${HOME}/**/bin -type f -iname 'conda' 2>/dev/null))

ifeq ($(CACHE),local)
CACHE = --import-cache type=local,src=/cache \
	--export-cache type=local,dest=/cache,mode=max
else
CACHE = --import-cache type=registry,ref=$(IMAGE):cache \
	--export-cache type=registry,ref=$(IMAGE):cache,mode=max
endif

ifeq ($(shell which buildctl-daemonless.sh),)
OUTPUT = --output type=docker,name=$(IMAGE):$(TAG),dest=/output/image.tar
POST_BUILD = cat output/image.tar | docker load
BUILD =  docker run \
				 -it --rm \
				 --privileged \
				 -v $(PWD)/cache:/cache \
				 -v $(PWD)/output:/output \
				 -v $(PWD):/build \
				 -w /build \
				 --entrypoint=/bin/sh \
				 moby/buildkit:master 
else
OUTPUT = --output type=image,name=$(IMAGE):$(TAG),push=true
BUILD = $(SHELL)
endif

ifeq ($(TARGET),testresult)
ifeq ($(shell which buildctl-daemonless.sh),)
OUTPUT = --output type=local,dest=/output
else
OUTPUT = --output type=local,dest=output
endif
endif

EXTRA = --opt build-arg:CONTAINER_IMAGE=$(IMAGE):$(TAG) \
	--opt build-arg:CONDA_VERSION=$(CONDA_VERSION)

test-env:
	conda install -n $(CONDA_TEST_ENV) -y -c conda-forge -c cdat pytest nbconvert nbformat xarray jupyter_client ipykernel

	source $(CONDA)/bin/activate $(CONDA_TEST_ENV); \
		python -m ipykernel install --name $(CONDA_TEST_ENV) --user

	mkdir notebooks/

provisioner: IMAGE := compute-provisioner
provisioner: DOCKERFILE := compute/compute_provisioner
provisioner:
	$(MAKE) build	

tasks: IMAGE := compute-tasks
tasks: DOCKERFILE := compute/compute_tasks
tasks:
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
	$(BUILD) build.sh $(DOCKERFILE) $(TARGET) $(EXTRA) $(CACHE) $(OUTPUT)
	
	$(POST_BUILD)
