# Usage

## build image

* Create docker image.
```
docker build -f ./Dockerfile.build_suse10-2 -t suse_build .
```

The image has to be called suse_build in order use the hidra build script.

## build hidra

* Run
```
./suse_build_hidra.sh
```

The zipped frozen hidra build can be found under /tmp/hidra_builds/build

# Remarks
Currently cx_Freeze 4.3.3 is used because of errors when migrating to higher versions.


# RPM dependencies

```
gcc-4.1.3-29.x86_64.rpm
    cpp-4.1.3-29.x86_64.rpm
        cpp41-4.1.2_20061115-5.x86_64.rpm
    gcc41-4.1.2_20061115-5.x86_64.rpm
        glibc-devel-2.5-25.x86_64.rpm
            linux-kernel-headers-2.6.18.2-3.x86_64.rpm
        libmudflap41-4.1.2_20061115-5.x86_64.rpm
gcc-c++-4.1.3-29.x86_64.rpm
    gcc41-c++-4.1.2_20061115-5.x86_64.rpm
        libstdc++41-devel-4.1.2_20061115-5.x86_64.rpm
    gcc-4.1.3-29.x86_64.rpm
zlib-devel-1.2.3-33.x86_64.rpm
readline-devel-5.1-55.x86_64.rpm
    ncurses-devel-5.5-42.x86_64.rpm
openssl-devel-0.9.8d-17.x86_64.rpm
make-3.81-23.x86_64.rpm
```
