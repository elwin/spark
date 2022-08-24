#!/usr/bin/env bash

#REPO=ghcr.io/elwin/spark
REPO=us.icr.io/elwin
TAG=$1

./bin/docker-image-tool.sh -r ${REPO} -t ${TAG} build
docker push $REPO:"$TAG"

scp -r dist zac25:~/spark-versions/"${TAG}"
