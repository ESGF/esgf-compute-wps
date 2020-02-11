default: help

COMPONENTS = provisioner tasks wps thredds
BUILD = $(patsubst %,build-%,$(COMPONENTS))
RUN = $(patsubst %,run-%,$(COMPONENTS))
RM = $(patsubst %,rm-%,$(COMPONENTS))
TEST = $(patsubst %,testresult-%,$(COMPONENTS))
TARGET = production
IMAGE_TAG = $(shell git rev-parse --short HEAD)

OUTPUT_LOCAL=--output type=local,dest=output/
OUTPUT_REMOTE=--output type=docker,name=$(IMAGE_NAME):$(IMAGE_TAG),dest=/output/$(NAME)
OUTPUT_IMAGE=--output type=image,name=$(IMAGE_NAME):$(IMAGE_TAG),push=true

.PHONY: $(BUILD) $(RUN) $(RM) $(TEST) help

define common_template =
	$(eval COMP=$(1))
	$(eval NAME=compute-$(1))
	$(eval COMP_DIR=compute_$(1))
	$(eval DOCKERFILE_DIR?=compute/$(COMP_DIR))
	$(eval SRC_DIR=$(DOCKERFILE_DIR)/$(COMP_DIR))
	$(eval IMAGE_NAME=$(if $(OUTPUT_REGISTRY),$(OUTPUT_REGISTRY)/)$(NAME))
endef

$(TEST):
	$(eval $(call common_template,$(word 2,$(subst -, ,$@))))

	$(MAKE) build-$(COMP) TARGET=testresult	

$(RM):
	$(eval $(call common_template,$(word 2,$(subst -, ,$@))))

	docker stop $(NAME) && \
		docker rm $(NAME)

$(RUN):
	$(eval $(call common_template,$(word 2,$(subst -, ,$@))))

	docker run -it --name $(NAME) \
		-v $(PWD)/$(SRC_DIR):/testing/$(COMP_DIR) \
		$(IMAGE_NAME):$(IMAGE_TAG) \
		/bin/bash 2>/dev/null || \
		(docker start $(NAME) && docker exec -it $(NAME) /bin/bash)

$(BUILD):
	$(eval $(call common_template,$(word 2,$(subst -, ,$@))))

	$(eval DOCKERFILE_DIR = $(if $(findstring compute-thredds,$(NAME)),docker/thredds,$(DOCKERFILE_DIR)))

	$(eval MPC_HOST = $(if $(findstring compute-tasks,$(NAME)),esgf-node.llnl.gov))
	$(eval MPC_USERNAME = $(if $(findstring compute-tasks,$(NAME)),$(MPC_USR)))
	$(eval MPC_PASSWORD = $(if $(findstring compute-tasks,$(NAME)),$(MPC_PSW)))

ifeq ($(shell which buildctl-daemonless.sh),)
	$(eval OUTPUT = $(if $(findstring testresult,$(TARGET)),$(OUTPUT_LOCAL),$(OUTPUT_REMOTE)))

	docker run -it --rm --privileged \
		-v $(PWD):/data -w /data \
		-v $(PWD)/cache:/cache \
		-v $(PWD)/output:/output \
		-v $(PWD)/testresult:/testresult \
		--entrypoint buildctl-daemonless.sh \
		moby/buildkit:master \
		build \
		--frontend dockerfile.v0 \
		--local context=. \
		--local dockerfile=$(DOCKERFILE_DIR) \
		--opt target=$(TARGET) \
		--opt build-arg:MPC_HOST=$(MPC_HOST) \
		--opt build-arg:MPC_USERNAME=$(MPC_USERNAME) \
		--opt build-arg:MPC_PASSWORD=$(MPC_PASSWORD) \
		$(OUTPUT) \
		--export-cache type=local,dest=/cache \
		--import-cache type=local,src=/cache

	cat output/$(NAME) | docker load
else
	$(eval OUTPUT = $(if $(findstring testresult,$(TARGET)),$(OUTPUT_LOCAL),$(OUTPUT_IMAGE)))

	buildctl-daemonless.sh \
		build \
		--frontend dockerfile.v0 \
		--local context=. \
		--local dockerfile=$(DOCKERFILE_DIR) \
		--opt target=$(TARGET) \
		--opt build-arg:MPC_HOST=$(MPC_HOST) \
		--opt build-arg:MPC_USERNAME=$(MPC_USERNAME) \
		--opt build-arg:MPC_PASSWORD=$(MPC_PASSWORD) \
		$(OUTPUT) \
		--export-cache type=registry,ref=$(IMAGE_NAME):cache \
		--import-cache type=registry,ref=$(IMAGE_NAME):cache 
endif

help: #: Show help topics
	@grep "#:" Makefile* | grep -v "@grep" | sort | sed "s/\([A-Za-z_ -]*\):.*#\(.*\)/\1\2/g"
