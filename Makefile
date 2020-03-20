.PHONY: provisioner tasks wps thredds build

export

IMAGE := $(if $(REGISTRY),$(REGISTRY)/)$(IMAGE)
TAG := $(shell git rev-parse --short HEAD)
CACHE := local

ifeq ($(CACHE),local)
CACHE = --import-cache type=local,src=/cache \
	--export-cache type=local,dest=/cache,mode=max
else
CACHE = --import-cache type=registry,ref=$(IMAGE):cache \
	--export-cache type=registry,ref=$(IMAGE):cache,mode=max
endif

ifeq ($(shell which buildctl-daemonless.sh),)
OUTPUT = --output type=docker,name=$(IMAGE):$(TAG),dest=/output/image.tar
EXTRA = cat output/image.tar | docker load
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

TARGET = production

ifeq ($(TARGET),testresult)
ifeq ($(shell which buildctl-daemonless.sh),)
OUTPUT = --output type=local,dest=/output
else
OUTPUT = --output type=local,dest=output
endif
endif

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
	$(BUILD) build.sh $(DOCKERFILE) $(TARGET) $(CACHE) $(OUTPUT)
	
	$(EXTRA)
