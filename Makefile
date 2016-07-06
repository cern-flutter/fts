OUTDIR=build
CADIR=${HOME}/.dev-ca
CERTDIR=${HOME}/.dev-cert
DOCKER_PREFIX=docker.cern.ch/flutter-dev

.PHONY: all install test-deps test ca cert up

all: install

$(OUTDIR):
	mkdir -p $@
	go get ./bin/...

install: $(OUTDIR)
	export GOBIN=`readlink -f $(OUTDIR)`; echo ./bin/* | xargs go install

test-deps:
	for i in `find . -name "*_test.go"`; do dirname $$i; done | sort | uniq | xargs go get -t

test: test-deps
	for i in `find . -name "*_test.go"`; do dirname $$i; done | sort | uniq | xargs go test -v

docker: docker-db docker-broker docker-worker docker-sched docker-rest

docker-base:
	docker build -t $(DOCKER_PREFIX)/base -f docker/base/Dockerfile .

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

docker-build:
	docker build -t $(DOCKER_PREFIX)/build -f docker/build/Dockerfile .

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
