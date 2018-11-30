Name:		hidra
Version:	4.0.15
Release:	1%{?dist}
Summary:	High performance data multiplexing tool

License:	AGPLv3
URL:		https://stash.desy.de/projects/HIDRA/repos/hidra
Source0:	hidra-v%{version}.zip
#Source1:	hidra.service

BuildArch:	noarch
BuildRequires:	python-devel
BuildRequires:	python-setuptools
BuildRequires:	systemd-units
Requires:	systemd
Requires:	python-logutils
Requires:	python-zmq >= 14.5.0
Requires:	python-inotifyx >= 0.2.2
Requires:	python-requests
Requires:	python-setproctitle
Requires:	python-six
Requires:	python-hidra = %{version}

%description
HiDRA is a generic tool set for high performance data multiplexing with different qualities of service and is based on Python and ZeroMQ. It can be used to directly store the data in the storage system but also to send it to some kind of online monitoring or analysis framework. Together with OnDA, data can be analyzed with a delay of seconds resulting in an increase of the quality of the generated scientific data by 20 %. The modular architecture of the tool (divided into event detectors, data fetchers and receivers) makes it easily extendible and even gives the possibility to adapt the software to specific detectors directly (for example, Eiger and Lambda detector).

# python libraries
%package -n python-hidra
Summary:	High performance data multiplexing tool - Python Library

BuildArch:	noarch

BuildRequires:	python-devel
BuildRequires:	python-setuptools
#Requires:	python-logutils
Requires:	python-zmq >= 14.5.0

%description -n python-hidra
This package contains only the API for developing tools against HiDRA.

# control client
%package -n hidra-control-client
Summary:	High performance data multiplexing tool - control client

BuildArch:	noarch

BuildRequires:	python-devel
BuildRequires:	python-setuptools
Requires:	python-hidra = %{version}

%description -n hidra-control-client
This package contains only the client to interact with the control server in the HIDRA package.

%prep
%setup -q -c %{name}-%{version}

#%build
#%{__python} setup.py build

%install
# Packaging Python API
mkdir -p %{buildroot}/%{python_sitelib}/%{name}
cp -r src/APIs/hidra/*.py %{buildroot}/%{python_sitelib}/%{name}/

# src receiver/sender and shared
mkdir -p %{buildroot}/opt/%{name}/src
cp -ra src/receiver %{buildroot}/opt/%{name}/src/
cp -ra src/sender %{buildroot}/opt/%{name}/src/
mkdir -p %{buildroot}/opt/%{name}/src/shared/
cp -a src/shared/*.py %{buildroot}/opt/%{name}/src/shared/

mkdir -p %{buildroot}/opt/%{name}/src/hidra_control
cp -a src/hidra_control/*.py %{buildroot}/opt/%{name}/src/hidra_control/

# conf
mkdir -p %{buildroot}/opt/%{name}/conf
cp conf/datamanager.conf conf/datareceiver.conf conf/base_receiver.conf conf/base_sender.conf %{buildroot}/opt/%{name}/conf/

# systemd unit files
mkdir -p %{buildroot}/%{_unitdir}
cp initscripts/*.service %{buildroot}/%{_unitdir}/

# log directory
mkdir -p %{buildroot}/var/log/%{name}

#%{__python} setup.py install -O1 --skip-build --root %{buildroot}

%post
%systemd_post %{name}@.service

%preun
%systemd_preun %{name}@.service

%postun
%systemd_postun_with_restart %{name}@.service

%files
%attr(0755,root,root) /opt/%{name}/src/receiver/*
/opt/%{name}/src/sender/*
%attr(0755,root,root) /opt/%{name}/src/sender/datamanager.py
/opt/%{name}/src/shared/*
/opt/%{name}/src/hidra_control/hidra_control_server.py
/opt/%{name}/src/hidra_control/hidra_control_server.pyc
/opt/%{name}/src/hidra_control/hidra_control_server.pyo
%{_unitdir}/*.service
%config(noreplace) /opt/%{name}/conf/*
%attr(1777,root,root) /var/log/%{name}

%files -n python-hidra
%doc examples
%{python_sitelib}/*

%files -n hidra-control-client
/opt/%{name}/src/hidra_control/hidra_control_client.py
/opt/%{name}/src/hidra_control/hidra_control_client.pyc
/opt/%{name}/src/hidra_control/hidra_control_client.pyo

%changelog
* Wed Nov 28 2018 Manuela Kuhn <manuela.kuhn@desy.de> - 4.0.15-1
- Bump version
* Tue Nov 13 2018 Manuela Kuhn <manuela.kuhn@desy.de> - 4.0.14-1
- Bump version
* Wed Nov 07 2018 Manuela Kuhn <manuela.kuhn@desy.de> - 4.0.13-1
- Bump version
* Thu Oct 25 2018 Manuela Kuhn <manuela.kuhn@desy.de> - 4.0.12-1
- Bump version
* Wed Oct 24 2018 Manuela Kuhn <manuela.kuhn@desy.de> - 4.0.11-1
- Bump version
* Thu Oct 18 2018 Manuela Kuhn <manuela.kuhn@desy.de> - 4.0.10-1
- Bump version
* Wed Sep 12 2018 Manuela Kuhn <manuela.kuhn@desy.de> - 4.0.9-1
- Bump version
* Tue Sep 11 2018 Manuela Kuhn <manuela.kuhn@desy.de> - 4.0.8-1
- Bump version
* Tue Aug 28 2018 Manuela Kuhn <manuela.kuhn@desy.de> - 4.0.7-1
- Bump version
* Fri Aug 24 2018 Manuela Kuhn <manuela.kuhn@desy.de> - 4.0.6-1
- Bump version
* Thu Aug 23 2018 Manuela Kuhn <manuela.kuhn@desy.de> - 4.0.5-1
- Bump version
* Thu Aug 23 2018 Manuela Kuhn <manuela.kuhn@desy.de> - 4.0.4-1
- Bump version
* Fri Aug 17 2018 Manuela Kuhn <manuela.kuhn@desy.de> - 4.0.3-1
- Bump version
* Fri Aug 17 2018 Manuela Kuhn <manuela.kuhn@desy.de> - 4.0.2-1
- Bump version
* Thu Aug 09 2018 Manuela Kuhn <manuela.kuhn@desy.de> - 4.0.1-1
- Bump version
* Wed Aug 08 2018 Manuela Kuhn <manuela.kuhn@desy.de> - 4.0.0-1
- Bump version
* Mon May 22 2017 Manuela Kuhn <manuela.kuhn@desy.de> - 3.1.3-1
- Bump version
* Fri May 12 2017 Manuela Kuhn <manuela.kuhn@desy.de> - 3.1.2-1
- Bump version
* Wed Apr 19 2017 Manuela Kuhn <manuela.kuhn@desy.de> - 3.1.1-1
- Bump version
* Wed Apr 19 2017 Manuela Kuhn <manuela.kuhn@desy.de> - 3.1.0-1
- Bump version
* Tue Apr 18 2017 Manuela Kuhn <manuela.kuhn@desy.de> - 3.0.2-4
- Separated control client package
* Mon Apr 17 2017 Manuela Kuhn <manuela.kuhn@desy.de> - 3.0.2-3
- Separated lib package
* Mon Jan 30 2017 Manuela Kuhn <manuela.kuhn@desy.de> - 3.0.2-2
- Change log directory
* Tue Dec 20 2016 Manuela Kuhn <manuela.kuhn@desy.de> - 3.0.2-1
- Bump version
* Fri Dec 16 2016 Manuela Kuhn <manuela.kuhn@desy.de> - 3.0.1-1
- Bump version
* Wed Dec 14 2016 Manuela Kuhn <manuela.kuhn@desy.de> - 3.0.0-1
- Bump version
* Tue Nov 22 2016 Stefan Dietrich <stefan.dietrich@desy.de> - 2.4.2-1
- Initial packaging
