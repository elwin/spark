#!/usr/bin/env bash

VERSION=3.1.2
export HADOOP_VERSION=3.2

features=(
  -Pkubernetes
  -Phive
  -Phive-thriftserver
  -DskipTests
)

./dev/make-distribution.sh "${features[@]}"
cp elwin/base_jars/* dist/jars
