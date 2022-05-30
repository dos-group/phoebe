#!/bin/bash

SCRIPTPATH="$( cd -- "$(dirname "$0")" >/dev/null 2>&1 ; pwd -P )"
APP_NAME=phoebe_py

## Build the image
docker build -t $APP_NAME .

## Create folder, if not exists
mkdir -p app/artifacts
chmod -R 775 app/artifacts

## Stop and remove a running container
docker stop $APP_NAME
docker rm $APP_NAME

docker run -d -p 5000:5000 -v ${SCRIPTPATH}/app/artifacts:/app/artifacts:z --name="$APP_NAME" $APP_NAME