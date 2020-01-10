default: help

COMPONENTS = provisioner tasks wps thredds
BUILD = $(patsubst %,build-%,$(COMPONENTS))
RUN = $(patsubst %,run-%,$(COMPONENTS))
RM = $(patsubst %,rm-%,$(COMPONENTS))
TARGET = production
IMAGE_TAG = $(shell git rev-parse --short HEAD)

.PHONY: $(BUILD) $(RUN) $(RM) help

define common_template =
	$(eval NAME=compute-$(1))
	$(eval COMP_DIR=compute_$(1))
	$(eval DOCKERFILE_DIR=compute/$(COMP_DIR))
	$(eval SRC_DIR=$(DOCKERFILE_DIR)/$(COMP_DIR))
	$(eval IMAGE_NAME=$(if $(OUTPUT_REGISTRY),$(OUTPUT_REGISTRY)/)$(NAME))
endef

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

	docker run -it --rm --privileged \
		-v $(PWD):/data -w /data \
		-v $(PWD)/cache:/cache \
		-v $(PWD)/output:/output \
		--entrypoint buildctl-daemonless.sh \
		moby/buildkit:master \
		build \
		--frontend dockerfile.v0 \
		--local context=. \
		--local dockerfile=$(DOCKERFILE_DIR) \
		--opt target=$(TARGET) \
		--output type=docker,name=$(IMAGE_NAME):$(IMAGE_TAG),dest=/output/$(NAME) \
		--export-cache type=local,dest=/cache \
		--import-cache type=local,src=/cache

	cat output/$(NAME) | docker load

help: #: Show help topics
	@grep "#:" Makefile* | grep -v "@grep" | sort | sed "s/\([A-Za-z_ -]*\):.*#\(.*\)/\1\2/g"
