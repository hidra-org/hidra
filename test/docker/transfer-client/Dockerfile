FROM centos:7

# Setup repos
RUN sed -i 's/mirrorlist/#mirrorlist/g' /etc/yum.repos.d/CentOS-* \
    && sed -i 's|#baseurl=http://mirror.centos.org|baseurl=http://vault.centos.org|g' /etc/yum.repos.d/CentOS-* \
    && yum update -y && yum install -y epel-release \
    && yum install -y less python-pip \
    && pip install --only-binary :all: -U pip \
    && pip install --prefer-binary debugpy

COPY *.rpm /rpms/

RUN yum install -y /rpms/*

COPY check_query.py /app/

WORKDIR /hidra
