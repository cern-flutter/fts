FTS Experimental
================
Sandbox for playing and prototyping ideas for FTS

## Requirements
* [libdirq](http://grid-deployment.web.cern.ch/grid-deployment/dms/fts3/repos/testing/el6/x86_64/)
* gfal2-devel
* A [Go](https://golang.org/doc/install) environment installed and ready
* docker and docker-compose

## Build
Just run `make` to trigger the build, and the installation of the resulting binaries
into the output directoy (`build` by default).

## Run
If the previous step worked just fine, for running a fully functional instance
easily, you need to do:

### make docker
To build the docker images. It has to be done manually so we avoid building all
images from scratch before launching compose, when we may be interested on
rebuilding only one of them.

### make up
This command will trigger:

  * The creation of a local CA under ~/.dev-ca (if it doesn't exist)
  * The creation of a host certificate and key under ~/.dev-cert (if it doesn't exist)
  * docker-compose

Ideally, components should be pluggable, which means you can combine ways
of running the service.
For instance, if you want to try something new for the scheduler, you could use
docker-compose to set everything except the scheduler, and then run manually
from sources connecting to the same AMQP broker as used by the contained apps.
