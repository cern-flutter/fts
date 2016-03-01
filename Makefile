OUTDIR=build

.PHONY: all install

all: install

install: $(OUTDIR)
	export GOBIN=`readlink -f $(OUTDIR)`; echo ./bin/* | xargs go install

$(OUTDIR):
	mkdir -p $@

docker: docker-worker docker-sched

docker-base:
	docker build -t fts-base -f docker/base/Dockerfile .

docker-worker: docker-base install
	docker build -t fts-worker -f docker/worker/Dockerfile .

docker-sched: docker-base install
	docker build -t fts-sched -f docker/scheduler/Dockerfile .

up: docker
	cd docker; docker-compose up
