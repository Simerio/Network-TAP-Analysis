#!/usr/bin/env bash
# Stop
docker stop elasticsearch

# Remove previuos container 
docker container rm elasticsearch

# Build
docker build . --tag tap:elasticsearch

docker run -t  -p 9200:9200 -p 9300:9300 --ip 10.0.100.51 --name elasticsearch --network tap -e "discovery.type=single-node" -e ES_JAVA_OPTS="-Xms4g -Xmx4g" tap:elasticsearch
