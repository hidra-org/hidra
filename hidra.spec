Name:		hidra
Version:	2.4.2
Release:	1%{?dist}
Summary:	High performance data multiplexing tool

License:	AGPLv3
URL:		https://stash.desy.de/projects/LSDMA/repos/hidra
Source0:	hidra-%{version}.zip
#Source1:	hidra.service

BuildArch:	noarch
BuildRequires:	python-devel
BuildRequires:	python-setuptools
BuildRequires:	systemd-units
Requires:	systemd
Requires:	python-logutils
Requires:	python-zmq >= 14.1.0
Requires:	python-inotifyx
Requires:	python-setproctitle
Requires:	python-six

%description
HiDRA is a generic tool set for high performance data multiplexing with different qualities of service and is based on Python and ZeroMQ. It can be used to directly store the data in the storage system but also to send it to some kind of online monitoring or analysis framework. Together with OnDA, data can be analyzed with a delay of seconds resulting in an increase of the quality of the generated scientific data by 20 %. The modular architecture of the tool (divided into event detectors, data fetchers and receivers) makes it easily extendible and even gives the possibility to adapt the software to specific detectors directly (for example, Eiger and Lambda detector).

%prep
%setup -q -n %{name}-%{version}

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
cp conf/dataManager.conf conf/dataReceiver.conf conf/nexusReceiver.conf %{buildroot}/opt/%{name}/conf/

# systemd unit files
mkdir -p %{buildroot}/%{_unitdir}
cp initscripts/*.service %{buildroot}/%{_unitdir}/

# log directory
mkdir -p %{buildroot}/opt/%{name}/logs

#%{__python} setup.py install -O1 --skip-build --root %{buildroot}

%post
%systemd_post %{name}@.service

%preun
%systemd_preun %{name}@.service

%postun
%systemd_postun_with_restart %{name}@.service

%files
%doc docs/*
%{python_sitelib}/*
/opt/%{name}/conf/*
%attr(0755,root,root) /opt/%{name}/src/receiver/*
/opt/%{name}/src/sender/*
%attr(0755,root,root) /opt/%{name}/src/sender/DataManager.py
/opt/%{name}/src/shared/*
/opt/%{name}/src/hidra_control/*
%{_unitdir}/*.service
%config(noreplace) /opt/%{name}/conf/*
%attr(1777,root,root) /opt/%{name}/logs

%changelog
* Tue Nov 22 2016 Stefan Dietrich <stefan.dietrich@desy.de> - 2.4.2-1
- Initial packaging
