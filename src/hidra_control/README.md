# HiDRA Control

# Server

* has to run on the node where HiDRA should be started and stopped

# Client

* change to folder
```
cd /opt/hidra/src/hidra_control
```
* The script only is allowed to be executed from beamline PCs corresponding to
  the specified beamline.

## Start

```
./hidra_control_client.py --beamline <beamline> --det <detector ip or dns name> --start
```

* The argument '--beamline' is mandatory for all commands. It can be one of the
  following:
  "p01", "p02.1", "p02.2", "p03", "p04", "p05", "p06", "p07", "p08", "p09",
  "p10", "p11"
* For starting hidra the detector IP or DNS name is mandatory and the API
  version of the filewriter may be set (default is 1.5.0). The detector
  specified has to be registered for the beamline.

## Stop

```
./hidra_control_client.py --beamline <beamline> --det <detector ip or dns name> --stop
```

* The arguments '--beamline' and '--det' are mandatory (see description of start)

## Check status

```
./hidra_control_client.py --beamline <beamline> --det <detector ip or dns name> --status
```

* The arguments '--beamline' and '--det' are mandatory (see description of start)

## Get the current settings

```
./hidra_control_client.py --beamline <beamline> --det <detector ip or dns name> --getsettings
```

* The arguments '--beamline' and '--det' are mandatory (see description of start)
* Displays the settings with which HiDRA was started on the server
