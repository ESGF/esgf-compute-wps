TARGET ?= production

OUTPUT_PATH ?= $(PWD)/output
CACHE_PATH ?= $(PWD)/cache

GIT_COMMIT := $(shell git rev-parse --short HEAD)
GIT_BRANCH := $(shell git branch --show-current)
GIT_TAG := $(shell git describe --tags)
# returns git branch name or tag 
GIT_NAME := $(or $(GIT_BRANCH),$(GIT_TAG))

IMAGE = $(if $(REGISTRY),$(REGISTRY)/)$(IMAGE_NAME)
# post-fix git revision if not building from tag
IMAGE_TAG = $(shell cat $(DOCKERFILE)/VERSION)$(if $(shell echo $(GIT_NAME) | grep -E ".*-v.*"),,-$(GIT_COMMIT))
# IMAGE_TAG = $(shell cat $(DOCKERFILE)/VERSION)$(if $(filter %-v%,$(GIT_NAME)),,-$(GIT_COMMIT))
IMAGE_PUSH ?= true

ifeq ($(shell which buildctl-daemonless.sh 2>/dev/null),)
BUILD := docker run -it --rm \
	--privileged \
	--group-add $(shell id -g) \
	-v $(HOME)/.docker:/root/.docker \
	-v $(OUTPUT_PATH):$(OUTPUT_PATH) \
	-v $(CACHE_PATH):$(CACHE_PATH) \
	-v $(PWD):$(PWD) \
	-w $(PWD) \
	--entrypoint=/bin/sh \
	moby/buildkit:master \
	build.sh
else
BUILD := $(SHELL) \
	build.sh
endif

BUILD_ARGS = --frontend dockerfile.v0 \
	--local context=$(PWD)/$(DOCKERFILE) \
	--local dockerfile=$(PWD)/$(DOCKERFILE) \
	--opt target=$(TARGET) \
	--opt build-arg:BASE_IMAGE=$(BASE_IMAGE) \
	--opt build-arg:CONTAINER_IMAGE=$(IMAGE):$(IMAGE_TAG)

BASE_IMAGE := continuumio/miniconda3:4.8.2

ifeq ($(CACHE_TYPE),local)
CACHE_ARGS = --export-cache type=local,dest=$(CACHE_PATH) \
	--import-cache type=local,src=$(CACHE_PATH)
else ifeq ($(CACHE_TYPE),registry)
CACHE_IMAGE = $(if $(REGISTRY_CACHE),$(REGISTRY_CACHE)/)$(IMAGE_NAME)
CACHE_ARGS = --export-cache type=registry,ref=$(CACHE_IMAGE):buildcache,registry.insecure=true \
	--import-cache type=registry,ref=$(CACHE_IMAGE):buildcache,registry.insecure=true
endif

ifeq ($(TARGET),testresult)
OUTPUT_ARGS = --output type=local,dest=$(OUTPUT_PATH)
else ifeq ($(OUTPUT_TYPE),local)
OUTPUT_ARGS = --output type=docker,name=$(IMAGE):$(IMAGE_TAG),dest=$(OUTPUT_PATH)/image.tar
else ifeq ($(OUTPUT_TYPE),registry)
OUTPUT_ARGS = --output type=image,name=$(IMAGE):$(IMAGE_TAG),push=$(IMAGE_PUSH)
endif

ifeq ($(OUTPUT_TYPE),local)
POST_CMDS += cat $(OUTPUT_PATH)/image.tar | docker load
endif

echo-tag:
	@echo $(IMAGE_TAG)

tag-tasks: DOCKERFILE := compute/compute_tasks
tag-tasks: echo-tag

tag-provisioner: DOCKERFILE := compute/compute_provisioner
tag-provisioner: echo-tag

tag-wps: DOCKERFILE := compute/compute_wps
tag-wps: echo-tag

tag-thredds: DOCKERFILE := docker/thredds
tag-thredds: echo-tag

tasks: IMAGE_NAME := compute-tasks
tasks: DOCKERFILE := compute/compute_tasks
tasks: build

provisioner: IMAGE_NAME := compute-provisioner
provisioner: DOCKERFILE := compute/compute_provisioner
provisioner: build

wps: IMAGE_NAME := compute-wps
wps: DOCKERFILE := compute/compute_wps
wps: build

thredds: IMAGE_NAME := compute-thredds
thredds: BASE_IMAGE := tomcat:8.5
thredds: DOCKERFILE := docker/thredds
thredds: build

build:
	$(BUILD) $(DOCKERFILE) $(BUILD_ARGS) $(OUTPUT_ARGS) $(CACHE_ARGS)

	$(POST_CMDS)
