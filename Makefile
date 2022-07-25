BRANCH := $(shell git rev-parse --abbrev-ref HEAD)

clean:
	./build/mvn -pl :spark-core_2.12 clean

build-core:
	./elwin/core.sh

build-base:
	./elwin/base-build.sh

push-base:
	./elwin/base-push.sh

push-core:
	./elwin/docker.sh $(BRANCH)

start-cluster:
	./dist/sbin/start-master.sh
	./dist/sbin/start-worker.sh spark://Findhorn:7077

stop-cluster:
	./dist/sbin/stop-worker.sh
	./dist/sbin/stop-master.sh

restart-cluster: stop-cluster start-cluster
