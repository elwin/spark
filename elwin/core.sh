set -e

./build/mvn -DskipTests -pl :spark-core_2.12 clean package

mkdir -p elwin/dist
cp core/target/spark-core_2.12-3.2.1.jar elwin/dist/spark-core_2.12-3.2.1.jar
cp core/target/spark-core_2.12-3.2.1.jar dist/jars/spark-core_2.12-3.2.1.jar
cp resource-managers/kubernetes/docker/src/main/dockerfiles/spark/entrypoint.sh elwin/dist/entrypoint.sh

