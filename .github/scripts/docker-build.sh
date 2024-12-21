#!/bin/bash

set -e

docker build -t quick-campaigns-socket --platform=linux/amd64 .
docker tag quick-campaigns-socket nas415/quick-campaigns-socket:latest
docker push nas415/quick-campaigns-socket:latest

echo "Built and pushed!!!"
