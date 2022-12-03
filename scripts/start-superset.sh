#!/bin/bash

cd $SUPERSET_PATH

docker-compose -f docker-compose-non-dev.yml pull
docker-compose -f docker-compose-non-dev.yml up