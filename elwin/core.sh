set -e

if [ $# -eq 0 ]; then
  echo "Usage: core.sh [tag]"
  exit 1
fi

REPO=ghcr.io/elwin/spark
TAG=$1

./build/mvn -DskipTests -pl :spark-core_2.12 clean package

mkdir -p elwin/dist
cp core/target/spark-core_2.12-3.2.1.jar elwin/dist/spark-core_2.12-3.2.1.jar
cp core/target/spark-core_2.12-3.2.1.jar dist/jars/spark-core_2.12-3.2.1.jar
cp resource-managers/kubernetes/docker/src/main/dockerfiles/spark/entrypoint.sh elwin/dist/entrypoint.sh

if [[ $* == *--docker* ]]; then
  IMAGE_NAME="${REPO}:${TAG}"
  docker build -f elwin/Dockerfile -t "${IMAGE_NAME}" elwin
  docker push "${IMAGE_NAME}"
fi
