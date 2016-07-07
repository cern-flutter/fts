BUILDDIR=./artifacts
CADIR=${HOME}/.dev-ca
CERTDIR=${HOME}/.dev-cert
DOCKER_PREFIX=docker.cern.ch/flutter-dev

.PHONY: artifacts

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

# Build base images, includes -devel packages, compiler...
image-build-base:
	docker build -t $(DOCKER_PREFIX)/build-base -f docker/build/Dockerfile.base .

# Build image and container, used to actually build the code
image-build:
	docker build -t $(DOCKER_PREFIX)/build -f docker/build/Dockerfile .

# Run the build
build: $(BUILDDIR)
	docker ps -a | grep flutter-build || docker create --volume="$$PWD:/src" --name=flutter-build $(DOCKER_PREFIX)/build
	docker start -a flutter-build && docker cp "flutter-build:/go/bin" $(BUILDDIR)

##
docker-worker: docker-base install
	docker build -t $(DOCKER_PREFIX)/worker -f docker/worker/Dockerfile .

docker-sched: docker-base install
	docker build -t $(DOCKER_PREFIX)/sched -f docker/scheduler/Dockerfile .

docker-rest: docker-base install
	docker build -t $(DOCKER_PREFIX)/rest -f docker/rest/Dockerfile .

docker-db:
	docker build -t $(DOCKER_PREFIX)/db -f docker/database/Dockerfile .

docker-broker:
	docker build -t $(DOCKER_PREFIX)/broker -f docker/broker/Dockerfile .
###


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
