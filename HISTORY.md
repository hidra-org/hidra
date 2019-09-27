# develop
# 4.1.1

- Fix RPM package building
- Move ext_ip into base_sender config
- Fix bumbversion script
- Fix backwards compatibility to yaml config type
- Fix with_confirmation when mixing python2 and 3
- Fix debian package building
- Fix data removal for multi chunk files
- Fix dispatcher name in logging
- Add pid of hanging proccess into log
- Simplify parameter handover in custom modules
- Fix misspellings and typos
- Fix pid display if hanging
- Fix hanging of datadispatcher on shutdow
- Fix version check between remote and local version
- Fix CPU usage of watchdog events (on linux)
- Fix freezing into executable
- Add file name information to watchdog event log
- Add information about registered streams to log
- Add option choose tag in debian build script
- Fix job forwarding from http events to cleaner

# 4.1.0

## general
- Move, clean up and structure of utils into separate module files
- Use hierarchical config format for sender, receiver and control server
- Changed default config to yaml
- Improve test suite to handle individual modules
- Remove scripts to build SuSE packages via docker (new kernels incompatible with SUSE 10)
- Fix python3 compatibility
- Change \__version__ to str
- Fix hidra usage via docker
- Enable mixture of netgroup and host list config for whitelist
- Rename ModuleNotFound to NotFound for versatility
- Remove hard dependency on logutils in python3
- Remove dependency on six
- Add script to build CentOS packages via docker
- Remove manual .egg tracking for setproctitle when freezing

## hidra base
- Changed effective user id inside of datamanager and datareceiver directly
- Removed underscore from event_detector and data_fetcher
- Renamed event_det_port and data_fetch_port
- Simplify log setup (no separated calls for with or without onscreen)
- Fix stopping of datadispatcher and taskprovider
- Event detector: Check monitored_dir when stopping
- Simplify inotifyx event detector
- Remove check for closed file in metadata mode
- Enable display of hanging processes on shut down
- Add inotify event detector
- Fix start up of eventdetector
- Remove restriction to supported modules only
- Add experimental ewmscp (kafka) syncing event detector
- Add data fetcher only distributing metadata (no data)
- Enable modules which do not need conf parameter
- Change default config for Pilatus and on Windows
- Fix log file permission problem if user starting hidra and the one as which is runs differ
- Enable random port usage for com_port and request_port
- Reduce number of workers when using Eiger (from 32 to 16)
- Fix netgroup resolving when result is not a fully qualified domain name
- Fix error message if writing fails (http_fetcher)
- Fix hanging processes due of missed control signal

## hidra control
- Control: Fix unicode compatibility
- Control client: show already running instances when start fails
- Control-server: Fixed file handle exception handling
- Extracted control server and control client config into config file
- Generalize control signals reaction
- Add host checks on control server
- Enable stopping of still running instances via control client
- Add option to show running instances via control client
- Improve error message when usage of control server
- Structured config server
- Remove config file when hidra is stopped via control server
- Introduce statserver to expose config (locally only)
- Enable control server and statserver communication
- Enable setting of beamline in config file of control client
- Fix starting of wrong beamline instances via control server
- Fix permission checking in control server
- Use one backup file per beamline

## APIs
- Transfer: Checking chunk number before opening file
- Enable port receiving via transfer api
- Fix double query for request port in transfer API

# 4.0.25

- Fix debian package building via docker
- Fix freezing into executable
- Remove user change as default value from sender

# 4.0.24

- Fix version check between remote and local version
- Fix CPU usage of watchdog events

# 4.0.23

- Fix crash of signal handler when removing multiple leftover connections

# 4.0.22

- Fix pathlib2 dependency in freeze_setup, rpm and deb packages
- Fix requests with target groups
- Fix stop command for redhat init script
- Remove default config loading in hidra.sh
- Fix debian package building via docker

# 4.0.21

- Fix reading of events for watchdog event detector
- Fix typo in http_fetcher
- Fix with_confirmation on Windows
- Fix python3 compatibility
- Add workaround for log file rotation on Windows
- Add debug information for WindowsError

# 4.0.20

- Fix sending of data multiple times (after crash+restart of application)
- Fix watchdog on_move events

# 4.0.19

- Fix double sending after wake up
- Ensure that stop of datadispatcher is communicated

# 4.0.18

- Undo ignoring of accumulated events during sleeping
- Fix control server crash when backup dir missing

# 4.0.17

- Fixed MOVE_TO events in watchdog event detector
- Fixed hanging of datadispatcher
- Fixed missing events in watchdog event detector

# 4.0.16

- Fixed control signal reaction during data handling
- Fixed confirmation socket reconnect after wakeup
- Control-server: Fixed file handle exception handling
- Fixed debian package build

# 4.0.15

- Restart hidra instances on control_server start
- Control_client: fixed detector argument (fqdn)
- Control_server: Add detector id to procname and log file name

# 4.0.14

- Fixed windows startup and cx_Freeze 5.x compatibility
- Fix bumpversion for multiple changelog entries
- Fixed LD_LIBRARY_PATH for receiver status (init script)

# 4.0.13

- Transfer: Fixed timeout in get command
- Transfer: Fix started connection bookkeeping
- Receiver: Fixed deactivated whitelist
- Show receiver status in status message of hidra.sh
- Fixed typo in suse build Dockerfile
- Added branch support in suse build

# 4.0.12

- Added verbose output if ldapsearch fails.

# 4.0.11

- Fixed slow stopping of receiver when netgroup_check_time is high
- Fixed None values in ipc address cleanup
- Fixed version checks
- Receiver: handle empty LDAP return lists

# 4.0.10

- Fixed usage of netgroup in whitelist for control API
- Do not start cleaner when no fix data stream is active
- Fixed combination store_data enabled and with_confirmation set
- Changed ipc cleanup: reuse attribute instead of redefining paths

# 4.0.9

- Fixed directories where to find build packages
- Fixed backward compatibility to 4.0.x versions

# 4.0.8

- Fixed subdir creation in http_fetcher
- Fixed get in transfer API if timeout is reached
- Fixed debian package naming
- Fixed log dir permissions for debian packages
- Fixed building script for suse 10
- Added building script for debian
- Improved init script: debug option, more info when failing
- Added HiDRA Control Server to fallback script
- Fixed with_confirmation to check all chunks
- Fixed beamline to host mapping
- Fixed Windows environment

# 4.0.7

- Fixed subdir creation in transfer API
- Fixed confirmation endpoint
- Fixed hidra control client  and datamanager default config values
- Init script: Fixed status output + changed commands
- Control client: Removed not supported argument target
- API: Fixed socket creation in control

# 4.0.6

- Fixed default config
- Fix config_file option in hidra.sh
- Fix freeze startup
- Added exampled

# 4.0.5

- Added debian package build scripts to bumpversion script
- Fixed undefined variable error in datamanager shutdown
- Fixed debian package build scripts

# 4.0.4

- Added automatic freeze build for suse 10 in docker container
- Changed default value for whitelist on Windows to localhost
- Fixed start up if fix_subdir cannot be created
- Fixed windows paths parsing in config
- Fixed endpoints in forwarder device
- Fixed: no ipc cleanup for Windows
- Fixed: startup problems on Windows

# 4.0.3

- Fixed version number
- Fixed Fixed bump version config for HISTORY.md

# 4.0.2

- Fixed spec file
- Fixed typo in LD_LIBRARY_PATH
- Fixed auth

# 4.0.1

- Fixed LD_IBRARY_PATH in initscipt for all platforms
- Fixed freeze (newline went missing in cleanup)

# 4.0.0

- Run hidra as user defined in the config file
- Added netgroup support in datamanager whitelist
- Added option to create fixed subdirs in monitored directory
- Added fix_subdirs awareness to http_events
- Do not create fix_subdirs in data_receiver
- If store_data is disabled requests for metadata gets an error
- Converted hosts to fully qualified domain names
- Added unittests
- Fixed sys path extention (prepend instead of append)
- Fixes stopping of datareceiver if start of transfer failed
- Fixed signal exchange with transfer API
- Fixed file loss when shutting down receiver
- More detailed response from SignalHandler to API (multipart)
- Fixed datareceiver: Include error message in log file
- Fixed return code if datareceiver stops because of an error
- Fixed memory leak in inotifyx -> version requirement of inotifyx
- Moved ldap uri into configuration files
- Changed usage and added additional options in hidra.sh
- Fixed help message in init script
- Use modified init script also for running frozen code
- Use current directory when init script is called in executable mode
- Added additional remove data strategies (stop_on_error and with_confirmation)
- Split config file (ports/internal settings vs user settings)
- Reopen not closed file if a first chunk is received again
- Added api calls to get and reset the receiver status
- Freeze: Create and use exe script
- Transfer API:
    - Added method to get remote version
    - Added regex support
    - Added appid to signal exchange
    - Added getter and setter for appid
    - Renames QUERY_METADATA into QUERY_NEXT_METADATA
    - Fixed get for intermixed chunks + 'reopen'
    - Renamed get to get_chunk and added get method


# 3.1.3

- Fixed searching path for service file in control server
- Changed defaults for store_data and remove_data in control client


# 3.1.2

- Removed check for fix_subdirs in local_dir


# 3.1.1

- Disabled interactive shell in systemctl call (control server)
- Fixed missing files in spe


# 3.1.0

- Adjusted host names in mapping for control API
- Run control server as user hidra
- RPM: Split single package into multiple subpackages (hidra, python-hidra, hidra-control-client)
- Fixed logging in transfer when logutils is used
- Updated requirement version of zmq in spec
- Transfer API responds to confirmation requests after data handling
- Control server sets subdirectories <commissioning|current>/scratch_bl and <commissioning|current>/raw instead of commissioning and current
- Path names have always unix format (even when send from windows)
- Added parameter test to DataManager
- Removed setup method in data fetchers
- Fixed data socket reconnection after netgroup change
- Notice netgroup changes during runtime
- Fixed relative path starting with a backslash in transfer
- Changed structure of event detectors to inherit from a base class
- Converted data fetchers to classes which inherit from a base class
- Added first versions of event detectors and data fetchers for using HiDRA in series
- Fixed file closing if message size equals chunksize (Transfer)
- Renamed Eiger specific parameters to be more generic
- Removed old beamline specific control clients
- Renamed options in hidra control
- Added support for configuration of multiple detector with one HiDRA control
- Added procname to init script
- Added msi building support for freeze
- HiDRA control: Exchanged normal socket communication with ZMQ communication
- Added Dockerfiles for sender, receiver and API usage (based on Ubuntu)
- Added platform specific config file to freeze
- Changed meaning when whitelist is to None in datamanager config
- Added option "getsettings" to init script (SuSE)
- Fixed start of transfer API if one host in whitelist id not known
- Metadata dict as call by reference for datafetcher modules
- Automatic beamline extraction from config dict (in _constants.py )for hidra control
- Fixed examples and generalized them
- Added test file for hidra-fs vs API
- Added first version of hidra filesystem based on FUSE
- Changed config parameter for data stream target (one list instead of two single parameters)
- Added script to check if receiver is running
- Added beamline to receiver init script
- Closing still open files on shutdown (transfer)
- Added GPFS fallback script
- Receiver runs as pxxuser
- Fixed poller shutdown in transfer API
- Fixed bumpversion usage
- Changed log directory to /var/log/hidra
- Cleanup of freeze setup
- Reduced config file for Pilatus detector to minimum
- Added options for handling the removal of the source data:
    - regular check of file writing status of receiver
    - only with confirmation for each file
- Added and used cfel_optargs for config parsing
- If the receiver is not responding all processes in the datamanager enters sleep mode until this is resolved
- Added init script for SuSE
- Added type checking for parameters in config
- Made existence of fixed subdirectories mandatory
- Enabled differentiation between 32 and 64bit architecture when freezing HiDRA
- Added HiDRA receiver unit file


# 3.0.2

- Fixed parallel hidra control client usage of script and permanent open connection (e.g. Tango)


# 3.0.1

- Added option to get configured settings to hidra control client
- The connection list is now contained in a separate module


# 3.0.0

- Automatic bump versioning
- Added generic hidra control client with included authentication (host-based)
- Clean up and consolidation of helper functions
- PEP8 compliance
- Make API classes available at the package level
- Fixed init script for Ubuntu and Debian
- Reordered directory structure
- Added DNS name support
- Added support for building RPMs
- Added automatic argument and config parsing (and fixed bugs in parsing)
- Added Python 3 support
- Renamed APIs and API arguments


# 2.4.2

- Changed process name in init script
- Building executables with cx_Freeze
- IPC directory is set to world/write readable when created
- IPC directory is removed (if empty) when HiDRA is stopped


# 2.4.1

- Changed config file to use file mode by default


# 2.4.0

- exchanged all cPickle calls with json calls
- renamed init script to hidra.sh
- Added functional tests
- Fixing memory leaks
- Minor bug fixing
- Working C-API for data transfer (nexus use case only)
- Working C-API for data ingest
- Removed PyTango dependency
- Fixed file opening (only open and close once and keep it open)
- Example client (based on API) for controlling HiDRA remotely
- The communication with the controlling-server can be done with an API
- HiDRA can be controlled (start, stop,...) via an addtional server (HiDRAControl...)
- Working version of dataIngestAPI with dataTransferAPI (python)
- Merged nexusTransferAPI into dataTranferAPI
- Handling of open connections to known hosts with different send configurations
- Changed socket format in dataIngestAPI to IPC (on Linux)
- Fixed receiving of CLOSE_FILE messages in nexusTransferAPI
- Fixed getMetadata if no targets are specified
- Fixed IPC socket cleanup
- Tracker in ALIVE_TEST only used if ZMQ version is higher than 14.5.0 due to error in older ZMQ versions
- Fixed double adding to watch in InotifyxDetector
- Choose config file for dataReceiver
- Removed timeout warning in dataTransferAPI


# 2.3.2

- Fixed log rotation


# 2.3.1

- Fixed filename sending for requests (unicode problem)
- Fixed missing metadata if no target is specified
- Fixed parallel directory creation attempts


# 2.3.0

- Added method to dataTransferAPI to manually stop streams/queries
- Added option to specify which file formats to be send via zeromq
- Added option look for multiple event types in parallel (combined with file suffixes)
- DataManager can now be controlled via tango
- Added systemd service script
- Added cleanup arguments into config file
- Files get accessed only if data or metadata is send via zeromq
- Fixed DataReceiver (no shell)
- Added command line argument error handling
- Removed ringbuffer remains of old architecture


# 2.2.1

- Fixed data receiving problems with dataTransferAPI due to ZAP


# 2.2.0

- Fixed stopping: The service is shut down if one process dies
- Enabled whitelist for data receiver
- Added tests to check status of fixed data receiver
- Added init script
- Fixed clean up after shut down
- Enabled combination of data receiver whitelist with ldapsearch
- Added option to enable a clean up thread which checks the directory for missed files
- Version check does not consider bugfixes anymore


# 2.1.4

- Fixed copied file removal (Part 2)


# 2.1.3

- Fixed copied file removal (Part 1)


# 2.1.2

- Fixed too high processor usage
- Fixed suffix check in treewalk after creation of directory


# 2.1.1

- Fixed error handling with incorrect whitelists
- Fixed version checking
- Added file create time to metadata


# 2.1.0

- Added file removal safeguard
- Enabled use of IPC internal communication for Linux nodes
- Added exception definitions for dataTransferAPI
- Misc bug fixing


# 2.0.0

- Added functionality to get Data via HTTP Get
- Redesigned architecture


# 1.0.0

- Initial implementation
