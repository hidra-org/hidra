HiDRA
======

__HiDRA__ (High Data Rate Access) is a generic tool set for high performance data multiplexing with different qualities of service and based on __Python__ and __ZeroMQ__.
On the one hand, it was developed to support 20 or more of the next generation detectors generating data up to 10GB/sec and images with kHz frequencies.
On the other hand, its purpose is to decouple persistent storage and selective image collection to support next generations of experiment setups, where the experiment conditions have to be monitored/analyzed in close to real time to prevent wasting the precious sample. <br />
This open source and facility independent software can be used to store the data directly in the storage system but also to send it to some kind of online monitoring or analysis framework.
Together with the tool [OnDA] (https://github.com/ondateam/onda), data can be analyzed with a delay of seconds resulting in test to an increase of the quality of the generated scientific data by 20 %.
The modular architecture of the tool makes it easily extendible and even gives the possibility to adapt the software to specific detectors directly (e.g. Eiger detector).


Contributors
============
HiDRA is developed by [Deutsches Elektronen-Synchrotron DESY] (http://www.desy.de).

License
=======

The project is licensed under __AGPL v3__.

Getting Started
===============

Further information can be found [in the documentation space] (https://confluence.desy.de/display/hidra/HiDRA)
