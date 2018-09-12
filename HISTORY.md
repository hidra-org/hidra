# develop
# 4.0.9

- Fixed directories where to find build packages
- Fixed backward compatibility to 4.0.x versions

# 4.0.8

- Fixed subdir creation in http_fetcher
- Fixed get in transfer API if timeout is reached
- Fixed debian package nameing
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
- Changed default value for whitlist on Windows to localhost
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
