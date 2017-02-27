FROM ubuntu

RUN apt update && apt-get install -y \
    git \
    python \
    python-pip \
    vim \
    iputils-ping
RUN pip install --upgrade pip && pip install \
    pyzmq \
    setproctitle \
    six \
    inotifyx
RUN cd /opt && git clone https://stash.desy.de/scm/hidra/hidra.git
RUN cd /opt/hidra && git checkout develop

