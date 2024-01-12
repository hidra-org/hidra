FROM almalinux:9

# Setup repos
RUN yum update -y && yum install -y epel-release \
    && yum install -y sudo less python3-pip procps-ng \
    && python3 -m pip install -U pip && python3 -m pip install debugpy

# setup systemctl-replacement-script
COPY systemctl.py /usr/bin/systemctl
RUN chmod +x /usr/bin/systemctl


# prepare hidra environment
RUN adduser hidra \
    && groupadd --gid 2345 supplgroup \
    && adduser --uid 1234 -G supplgroup p00user

COPY hidra /etc/sudoers.d/

COPY --chown=hidra:hidra receiver_p00.yaml control_server_p00.yaml /opt/hidra/conf/

COPY post_install.sh /scripts/

COPY *.rpm /rpms/

RUN yum install -y /rpms/*

WORKDIR /hidra

CMD [ "/usr/bin/systemctl" ]