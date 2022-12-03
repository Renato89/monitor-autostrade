#!/bin/bash
source ../config/path.config
cd $SUPERSET_PATH

docker-compose -f docker-compose-non-dev.yml pull
docker-compose -f docker-compose-non-dev.yml up