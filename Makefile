BUILDDIR=./artifacts
CADIR=${HOME}/.dev-ca
CERTDIR=${HOME}/.dev-cert
DOCKER_PREFIX=docker.cern.ch/flutter-dev

.PHONY: docker-all test-deps test

all: build

$(BUILDDIR):
	mkdir -p $(BUILDDIR)

# Binaries
binaries:
	export GOBIN="$$GOPATH/bin"; go get -v ./bin/...
	export GOBIN="$$GOPATH/bin"; echo ./bin/* | xargs go install

# Tests
test-deps:
	for i in `find . -name "*_test.go"`; do dirname $$i; done | sort | uniq | xargs go get -t

test: test-deps
	for i in `find . -name "*_test.go"`; do dirname $$i; done | sort | uniq | xargs go test -v -cover

# Base image with common dependencies
image-base:
	docker build -t $(DOCKER_PREFIX)/base -f docker/base/Dockerfile .

# Build image and container, used to actually build the code
image-build:
	docker build -t $(DOCKER_PREFIX)/build -f docker/build/Dockerfile .
	docker rm flutter-build || true

# Build binaries
build: $(BUILDDIR)
	docker ps -a | grep flutter-build || docker create --volume="$$PWD:/go/src/gitlab.cern.ch/flutter/fts" \
		--name=flutter-build $(DOCKER_PREFIX)/build \
		bash -c "cd /go/src/gitlab.cern.ch/flutter/fts; make binaries"
	docker start -a flutter-build && docker cp "flutter-build:/go/bin" $(BUILDDIR)

# Containers with the binaries
docker-sched: build
	docker build -t $(DOCKER_PREFIX)/sched -f docker/scheduler/Dockerfile .

docker-worker: build
	docker build -t $(DOCKER_PREFIX)/worker -f docker/worker/Dockerfile .

docker-optimizer: build
	docker build -t $(DOCKER_PREFIX)/optimizer -f docker/optimizer/Dockerfile .

docker-publisher: build
	docker build -t $(DOCKER_PREFIX)/publisher -f docker/publisher/Dockerfile .

docker-stager: build
	docker build -t $(DOCKER_PREFIX)/stager -f docker/stager/Dockerfile .

docker-store: build
	docker build -t $(DOCKER_PREFIX)/store -f docker/store/Dockerfile .

docker-db: build
	docker build -t $(DOCKER_PREFIX)/db -f docker/database/Dockerfile .

docker-broker: build
	docker build -t $(DOCKER_PREFIX)/broker -f docker/broker/Dockerfile .

docker-all: docker-broker docker-db \
	docker-publisher docker-store \
	docker-worker docker-stager \
	docker-sched docker-optimizer

# Development root CA
ca: $(CADIR)/private/ca_key.pem

$(CADIR)/private/cakey.pem:
	./hack/create_ca.sh "$(CADIR)"

cert: $(CERTDIR)/hostcert.pem

$(CERTDIR)/hostcert.pem: $(CADIR)/private/cakey.pem
	./hack/generate_cert.sh "$(CADIR)" "/CN="$(shell hostname -f) "$(CERTDIR)"
	cp $(CADIR)/certs/cacert.pem $(CERTDIR)

# We do not put docker as a dependency to avoid triggering all the builds with no actual need
up: $(CERTDIR)/hostcert.pem
	cd docker; CERT_DIR=${CERDIR}; docker-compose up
