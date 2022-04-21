build-core:
	./elwin/core.sh latest

start-cluster:
	./dist/sbin/start-master.sh
	./dist/sbin/start-worker.sh spark://Findhorn:7077

stop-cluster:
	./dist/sbin/stop-worker.sh
	./dist/sbin/stop-master.sh
