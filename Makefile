# Licensed to Apache Software Foundation (ASF) under one or more contributor
# license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright
# ownership. Apache Software Foundation (ASF) licenses this file to you under
# the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

SHELL := /bin/bash -o pipefail

ROOT := $(shell dirname $(realpath $(lastword $(MAKEFILE_LIST))))

HUB ?= apache
IMAGE ?= seatunnel-engine
TAG ?= $(shell git rev-parse HEAD)

CONTEXT ?= $(ROOT)/dist/docker-build
DIST_TAR ?= $(ROOT)/seatunnel-dist/target/apache-seatunnel-*-bin.tar.gz

BUILD_ARGS ?=

docker: PLATFORMS =
docker: LOAD_OR_PUSH = --load
push.docker: PLATFORMS = --platform linux/amd64,linux/arm64
push.docker: LOAD_OR_PUSH = --push

$(DIST_TAR):
	./mvnw -B clean install -Dmaven.test.skip -Prelease

$(CONTEXT)/$(IMAGE): $(ROOT)/docker/seatunnel-engine/* $(DIST_TAR)
	mkdir -p $(CONTEXT)/$(IMAGE)
	cp -r $< $(CONTEXT)/$(IMAGE)
	tar -zxf $(DIST_TAR) --strip-components=1 -C $@
	$@/bin/install-plugin.sh $(shell ./mvnw help:evaluate -q -DforceStdout -D"expression=project.version")

.PHONY: clean
clean:
	rm -rf $(CONTEXT)
	docker buildx rm seatunnel_$(IMAGE) > /dev/null 2>&1 || true

.PHONY: docker push.docker
docker push.docker: $(CONTEXT)/$(IMAGE)
	docker buildx create --driver docker-container --name seatunnel_$(IMAGE) > /dev/null 2>&1 || true
	docker buildx build $(PLATFORMS) $(LOAD_OR_PUSH) \
		--no-cache $(BUILD_ARGS)  \
		--builder seatunnel_$(IMAGE) \
		-t $(HUB)/$(IMAGE):$(TAG) \
		-t $(HUB)/$(IMAGE):latest \
		$(CONTEXT)/$(IMAGE)
	docker buildx rm seatunnel_$(IMAGE) || true
