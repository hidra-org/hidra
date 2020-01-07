#!/bin/bash

# has to be called from the hidra dir to work

IMAGE_NAME=hidra_install_image
DOCKERFILE="./scripts/Dockerfile.install"

docker build -f "$DOCKERFILE" -t "$IMAGE_NAME" .

cmd="cd /opt/hidra; pip install --user ."

if [[ -x "$(command -v dockerrun)" ]]; then
    DOCKERRUN=dockerrun
    cmd="export PATH=\$PATH:/tmp/kuhnm/.local/bin; $cmd"
else
    DOCKERRUN=docker run
fi

echo "After the container is started, execute \"$cmd\" to install hidra in the container"

$DOCKERRUN -it --rm -v $(pwd):/opt/hidra $IMAGE_NAME bash
