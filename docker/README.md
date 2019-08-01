# Desciption

These Dockerfiles enable HiDRA usage inside an docker container. HiDRA is not set up to run as service there but the purpose of these containers is more for testing and experimenting purposes.

There are three different containers available.
One which runs the HiDRA datamanager, one for the datareceiver and one which sets up the environment to use the API.

# Usage

## sender

* Clone hidra repo
* If necessary modify conf/datamanager_docker.yaml
* Create docker image (give it for example the name "sender_test").
```
docker build -t sender_test -f Dockerfile.sender .
```
* Create the docker container and map directorie from outside the containter as source.
```
docker run -it -v <hidra location>:/opt/hidra -v <source>:/ramdisk sender_test
```

### Alternatives

#### Connect from outside

To be able to connect from outside to it, run
```
docker run -it -v <hidra location>:/opt/hidra -v <source>:/ramdisk --net=host sender_test
```

#### Migrate data locally

* Change conf/datamanager_docker.yaml: Enable storing of data
```
store_data = True
```
* Create the docker container and map directories from outside the containter as source and target.
```
docker run -it -v <hidra location>:/opt/hidra -v <source>:/ramdisk -v <target>:/target sender_test
```

## receiver

* Clone hidra repo
* If necessary modify conf/datareceiver_docker.yaml
* Create docker image (give it for example the name "receiver_test").
```
docker build -t receiver_test -f Dockerfile.receiver .
```
* Create the docker container and map directory from outside the containter as target.
```
docker run -it -v <hidra location>:/opt/hidra -v <target>:/target receiver_test
```

## API

* No cloning of the repo required, this is done inside the image.
* Create docker image (give it for example the name "api_test").
```
docker build -t api_test -f Dockerfile.api .
```
* Create the docker container.
```
docker run -it api_test
```

# Remarks
Currently pip 9.0.3 is used because when pip is updated to pip 10 an error occurs.
