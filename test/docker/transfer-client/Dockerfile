FROM centos:7

# Setup repos
RUN yum update -y && yum install -y epel-release \
    && yum install -y less python-pip \
    && pip install -U pip && pip install debugpy

COPY *.rpm /rpms/

RUN yum install -y /rpms/*

COPY check_query.py /app/

WORKDIR /hidra
