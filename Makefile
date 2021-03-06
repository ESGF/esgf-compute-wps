GIT_REV := $(shell git rev-parse --short HEAD)
GIT_BRANCH := $(shell git rev-parse --abbrev-ref HEAD)

REGISTRY := $(if $(REGISTRY),$(REGISTRY)/,nimbus2.llnl.gov/default/)
VERSION = $(shell cat $(DOCKERFILE)/VERSION)

TAG = $(REGISTRY)$(NAME):$(VERSION)$(if $(findstring master,$(GIT_BRANCH)),,-$(GIT_REV))

TARGET ?= production
CONDA_VERSION ?= 4.8.2
CACHE_PATH ?= $(PWD)/cache
OUTPUT_PATH ?= $(PWD)/output
BASE_IMAGE = continuumio/miniconda3:4.9.2

CACHE_ARG = --import-cache type=local,src=$(CACHE_PATH) \
						--export-cache type=local,dest=$(CACHE_PATH),mode=max

CONFIG_ARG = --opt build-arg:CONTAINER_IMAGE=$(TAG) \
	--opt build-arg:CONDA_VERSION=$(CONDA_VERSION) \
	--opt build-arg:BASE_IMAGE=$(BASE_IMAGE)

ifeq ($(TARGET),testresult)
OUTPUT_ARG = --output type=local,dest=$(OUTPUT_PATH)
else ifeq ($(shell which docker 2>/dev/null),)
OUTPUT_ARG = --output type=image,name=$(TAG),push=true
else
OUTPUT_ARG = --output type=docker,name=$(TAG),dest=$(OUTPUT_PATH)/image.tar.gz
BUILD_POST = cat $(OUTPUT_PATH)/image.tar.gz | docker load; \
						 echo $(TAG)
endif

BUILD_ARG = /usr/bin/buildctl-daemonless.sh \
						build \
						--frontend dockerfile.v0 \
						--local context=$(PWD)/$(DOCKERFILE) \
						--local dockerfile=$(PWD)/$(DOCKERFILE) \
						--opt target=$(TARGET) \
						$(CACHE_ARG) \
						$(CONFIG_ARG) \
						$(OUTPUT_ARG)

ifeq ($(shell which buildctl-daemonless.sh 2>/dev/null),)
BUILD = docker run -it --rm \
				--privileged \
				--group-add $(shell id -g) \
				-v $(PWD):$(PWD) \
				-w $(PWD) \
				--entrypoint=/bin/sh \
				moby/buildkit:master \
				$(BUILD_ARG)
else
BUILD = /bin/sh \
				$(BUILD_ARG)
endif

.PHONY: provisioner
provisioner: NAME	:= compute-provisioner
provisioner: DOCKERFILE := compute/compute_provisioner
provisioner: build

.PHONY: tasks
tasks: NAME := compute-tasks
tasks: DOCKERFILE := compute/compute_tasks
tasks: build

.PHONY: wps
wps: NAME := compute-wps
wps: DOCKERFILE := compute/compute_wps
wps: build

.PHONY: thredds
thredds: NAME := compute-thredds
thredds: DOCKERFILE := docker/thredds
thredds: build

.PHONY: build
build:
	$(BUILD_PRE)

	$(BUILD)

	$(BUILD_POST)
