#!/usr/bin/env bash

REPO=ghcr.io/elwin/spark
TAG=hive

./bin/docker-image-tool.sh -r ${REPO} -t ${TAG} build
docker push $REPO:$TAG
scp -r dist zac25:~/spark-versions/"${TAG}"
