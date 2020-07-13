FROM centos:7

# Setup repos
RUN yum update -y && yum install -y epel-release \
    && yum install -y less python3-pip \
    && python3 -m pip install -U pip && python3 -m pip install debugpy

COPY *.rpm /rpms/

RUN yum install -y /rpms/*

COPY control_client.yaml /opt/hidra/conf/

WORKDIR /hidra
