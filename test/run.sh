#!/bin/bash

rm -f ./*.deb
cp ../dist/*.deb ./
docker-compose down
docker-compose build --no-cache
docker-compose up -d 
docker exec -it test_backy2-api_1 bash