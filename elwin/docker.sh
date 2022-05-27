set -e

if [ $# -eq 0 ]; then
  echo "Usage: build-docker.sh [tag]"
  exit 1
fi

REPO=ghcr.io/elwin/spark
TAG=$1


IMAGE_NAME="${REPO}:${TAG}"

echo "Pushing to \"${IMAGE_NAME}\""

docker build -f elwin/Dockerfile -t "${IMAGE_NAME}" elwin
docker push "${IMAGE_NAME}"
