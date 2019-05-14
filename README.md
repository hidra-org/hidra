# HiDRA

__HiDRA__ (High Data Rate Access) is a generic tool set for high performance data multiplexing with different qualities of service and based on __Python__ and __ZeroMQ__.
On the one hand, it was developed to support 20 or more of the next generation detectors generating data up to 10GB/sec and images with kHz frequencies.
On the other hand, its purpose is to decouple persistent storage and selective image collection to support next generations of experiment setups, where the experiment conditions have to be monitored/analyzed in close to real time to prevent wasting the precious sample. <br />
This open source and facility independent software can be used to store the data directly in the storage system but also to send it to some kind of online monitoring or analysis framework.
Together with the tool [OnDA] (https://stash.desy.de/projects/ONDA), data can be analyzed with a delay of seconds resulting in test to an increase of the quality of the generated scientific data by 20 %.
The modular architecture of the tool makes it easily extendible and even gives the possibility to adapt the software to specific detectors directly (e.g. Eiger detector).

# License

The project is licensed under __AGPL v3__.

# Getting Started

## Data Sending Side

### Requirements
* pyzmq, version 14.6.0 or newer
* setproctitle
* six
* logutils
* pyyaml
* inotifyx (for inotifyx event detector: file transfer on Linux), version 0.2.2 or newer
* watchdog (for watchdog event detector: file transfer on Windows)
* requests (for HTTP event detector, used for Eiger)
* pathlib2 (when using python 2)

### Installation and Usage
* Download and unpack or clone HiDRA from [Stash](https://stash.desy.de/projects/HIDRA/repos/hidra/browse)
* Configure HiDRA as described in the [Datamanager](https://confluence.desy.de/display/hidra/Datamanager) page
* Start with verbose mode displayed on screen
```
% python <hidra_path>/src/sender/datamanger.py --verbose --onscreen debug
```

## Data Receiving Side

### Requirements
* pyzmq, version 14.5.0 or newer
* setproctitle
* pyyaml
* six
* logutils
* pathlib2 (when using python 2)

### Installation and Usage
* Download and unpack or clone HiDRA from [Stash](https://stash.desy.de/projects/HIDRA/repos/hidra/browse)
* Configure HiDRA as described in the [Datareceiver](https://confluence.desy.de/display/hidra/Datareceiver) page
* Start with verbose mode displayed on screen
```
% python <hidra_path>/src/receiver/datareceiver.py --verbose --onscreen debug
```

## Alternative for users in the DESY network

### Install from Package

Users with access to the DESY network have multiple possibilities to install HiDRA:
* CentOS: via rpm from [here](http://nims.desy.de/extra/hidra/)
* Debian and if the PC was set up by FS-EC: via apt get

There are three different packages available:
* hidra: This package contains the datamanager, the datareceiver including the APIs as well as the control server.
* python-hidra: This package contains only the API for developing tools against HiDRA.
* hidra-control-client: This package contains only the client to interact with the control server in the HIDRA package.

### Use prebuild executables

Executables build for Linux and Windows systems are available [here](http://nims.desy.de/extra/hidra/freeze/) (see [Setup for Pilatus](https://confluence.desy.de/display/hidra/Pilatus))

# Contributors

HiDRA is developed by [Deutsches Elektronen-Synchrotron DESY] (http://www.desy.de).

Further information can be found [in the documentation space] (https://confluence.desy.de/display/hidra/HiDRA)
