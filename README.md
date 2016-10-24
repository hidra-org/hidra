HiDRA
======

__HiDRA__ is a generic tool set for high performance data multiplexing with different qualities of service
and is based on __Python__ and __ZeroMQ__. It can be used to directly store the data in the storage
system but also to send it to some kind of online monitoring or analysis framework. Together
with [OnDA] (https://github.com/ondateam/onda), data can be analyzed with a delay of seconds resulting in
an increase of the quality of the generated scientific data by 20 %. The modular architecture of
the tool (divided into event detectors, data fetchers and receivers) makes it easily extendible and
even gives the possibility to adapt the software to specific detectors directly (for example, Eiger
and Lambda detector).

Contributors
============
HiDRA is developed by [Deutsches Elektronen-Synchrotron DESY] (http://www.desy.de).

License
=======

The project is licensed under __AGPL v3__.

Getting Started
===============

The file [INSTALL.md](INSTALL.md) describes how to compile HiDRA code and build various packages.

How to contribute
=================

**HiDRA** uses the linux kernel model where git is not only source repository,
but also the way to track contributions and copyrights.

Each submitted patch must have a "Signed-off-by" line.  Patches without
this line will not be accepted.

The sign-off is a simple line at the end of the explanation for the
patch, which certifies that you wrote it or otherwise have the right to
pass it on as an open-source patch.  The rules are pretty simple: if you
can certify the below:
```

    Developer's Certificate of Origin 1.1

    By making a contribution to this project, I certify that:

    (a) The contribution was created in whole or in part by me and I
         have the right to submit it under the open source license
         indicated in the file; or

    (b) The contribution is based upon previous work that, to the best
        of my knowledge, is covered under an appropriate open source
        license and I have the right under that license to submit that
        work with modifications, whether created in whole or in part
        by me, under the same open source license (unless I am
        permitted to submit under a different license), as indicated
        in the file; or

    (c) The contribution was provided directly to me by some other
        person who certified (a), (b) or (c) and I have not modified
        it.

    (d) I understand and agree that this project and the contribution
        are public and that a record of the contribution (including all
        personal information I submit with it, including my sign-off) is
        maintained indefinitely and may be redistributed consistent with
        this project or the open source license(s) involved.

```
then you just add a line saying ( git commit -s )

    Signed-off-by: Random J Developer <random@developer.example.org>

using your real name (sorry, no pseudonyms or anonymous contributions.)

