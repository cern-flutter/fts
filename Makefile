OUTDIR=build
CADIR=${HOME}/.dev-ca
CERTDIR=${HOME}/.dev-cert

.PHONY: all install ca cert up

all: install

$(OUTDIR):
	mkdir -p $@
	go get ./bin/...

install: $(OUTDIR)
	export GOBIN=`readlink -f $(OUTDIR)`; echo ./bin/* | xargs go install

docker: docker-db docker-broker docker-worker docker-sched docker-rest

docker-base:
	docker build -t fts-base -f docker/base/Dockerfile .

docker-worker: docker-base install
	docker build -t fts-worker -f docker/worker/Dockerfile .

docker-sched: docker-base install
	docker build -t fts-sched -f docker/scheduler/Dockerfile .

docker-rest: docker-base install
	docker build -t fts-rest -f docker/rest/Dockerfile .

docker-db:
	docker build -t fts-db -f docker/database/Dockerfile .

docker-broker:
	docker build -t fts-broker -f docker/broker/Dockerfile .

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
