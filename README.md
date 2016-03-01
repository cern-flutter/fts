FTS Experimental
================
Sandbox for playing and prototyping ideas for FTS

## Requirements
* [libdirq](http://grid-deployment.web.cern.ch/grid-deployment/dms/fts3/repos/testing/el6/x86_64/)
* gfal2-devel
* A [Go](https://golang.org/doc/install) environment ready
* docker and docker-compose

## Run it
### Build
Just run `make` to trigger the build, and the installation of the resulting binaries
into the output directoy (`build` by default).

## Run it
If the previous step worked just fine, you can run the service easily doing `make up`.
With this, a set of docker containers will be built with the binaries, and
docker-compose will be used to spawn a predefined set of containers (including
RabbitMQ and MongoDB) ready to try the service.

You can also do `make docker` to build the containers, and then run manually as you want,
or even run manually the binaries, or from sources (`go run bin/fts-workerd/*.go`,
for instance).

Ideally, components should be pluggable, which means you can combine ways
of running the service.
For instance, if you want to try something new for the scheduler, you could use
docker-compose to set everything except the scheduler, and then run manually
from sources connecting to the same AMQP broker as used by the contained apps.
