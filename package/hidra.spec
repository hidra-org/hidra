Name:		hidra
Version:	4.2.1
Release:	2%{?dist}
Summary:	High performance data multiplexing tool

License:	AGPLv3
URL:		https://github.com/hidra-org/hidra
Source0:	hidra-%{version}.tar.gz
#Source1:	hidra.service

BuildArch:	noarch
BuildRequires:	python3-devel
BuildRequires:	python3-setuptools
BuildRequires:	systemd-units
Requires:	systemd
Requires:	python36-zmq >= 14.5.0
Requires:	python3-inotifyx >= 0.2.2
Requires:	python36-requests
Requires:	python36-setproctitle
Requires:	python36-future
Requires:	python3-hidra = %{version}

%description
HiDRA is a generic tool set for high performance data multiplexing with different qualities of service and is based on Python and ZeroMQ. It can be used to directly store the data in the storage system but also to send it to some kind of online monitoring or analysis framework. Together with OnDA, data can be analyzed with a delay of seconds resulting in an increase of the quality of the generated scientific data by 20 %. The modular architecture of the tool (divided into event detectors, data fetchers and receivers) makes it easily extendible and even gives the possibility to adapt the software to specific detectors directly (for example, Eiger and Lambda detector).

# python2 libraries
%package -n python2-hidra
Summary:	High performance data multiplexing tool - Python Library
%{?python_provide:%python_provide python2-hidra}
BuildArch:	noarch
BuildRequires:	python-devel
BuildRequires:	python-setuptools
Requires:	python-logutils
Requires:	python-pathlib
Requires:	python-zmq >= 14.5.0
Requires:	PyYAML
Requires:	python2-ldap3

%description -n python2-hidra
This package contains only the API for developing tools against HiDRA.

# python3 libraries
%package -n python3-hidra
Summary:	High performance data multiplexing tool - Python Library
%{?python_provide:%python_provide python3-hidra}
BuildArch:	noarch
BuildRequires:	python3-devel
BuildRequires:	python3-setuptools
Requires:	python36-zmq >= 14.5.0
# centos 7 does not provide general python3 version
Requires:	python36-PyYAML
# centos 7 does not provide general python3 version
Requires:	python36-ldap3

%description -n python3-hidra
This package contains only the API for developing tools against HiDRA.

# control client
%package -n hidra-control-client
Summary:	High performance data multiplexing tool - control client
BuildArch:	noarch
BuildRequires:	python3-devel
BuildRequires:	python3-setuptools
Requires:	python3-hidra = %{version}

%description -n hidra-control-client
This package contains only the client to interact with the control server in the HIDRA package.

%prep
%setup -q

#%build
#%{__python} setup.py build

%install
# Packaging Python API
mkdir -p %{buildroot}/%{python_sitelib}/%{name}
cp -r src/api/python/hidra/* %{buildroot}/%{python2_sitelib}/%{name}/
mkdir -p %{buildroot}/%{python3_sitelib}/%{name}
cp -r src/api/python/hidra/* %{buildroot}/%{python3_sitelib}/%{name}/

# src receiver/sender
mkdir -p %{buildroot}/opt/%{name}/src/hidra
cp -ra src/hidra/receiver %{buildroot}/opt/%{name}/src/hidra
cp -ra src/hidra/sender %{buildroot}/opt/%{name}/src/hidra

mkdir -p %{buildroot}/opt/%{name}/src/hidra/hidra_control
cp -a src/hidra/hidra_control/*.py %{buildroot}/opt/%{name}/src/hidra/hidra_control/
rm %{buildroot}/opt/%{name}/src/hidra/hidra_control/__init__.py

# conf
mkdir -p %{buildroot}/opt/%{name}/conf
cp conf/datamanager.yaml conf/datareceiver.yaml conf/base_receiver.yaml conf/base_sender.yaml conf/control_server.yaml conf/control_client.yaml %{buildroot}/opt/%{name}/conf/

# systemd unit files
mkdir -p %{buildroot}/%{_unitdir}
cp scripts/init_scripts/*.service %{buildroot}/%{_unitdir}/

# log directory
mkdir -p %{buildroot}/var/log/%{name}

#%{__python} setup.py install -O1 --skip-build --root %{buildroot}

%post
%systemd_post %{name}@.service %{name}-receiver@.service %{name}-control-server@.service

%preun
%systemd_preun %{name}@.service %{name}-receiver@.service %{name}-control-server@.service

%postun
%systemd_postun_with_restart %{name}@.service %{name}-receiver@.service %{name}-control-server@.service

%files
%attr(0755,root,root) /opt/%{name}/src/hidra/receiver/*
/opt/%{name}/src/hidra/sender/*
%attr(0755,root,root) /opt/%{name}/src/hidra/sender/datamanager.py
/opt/%{name}/src/hidra/hidra_control/server.py
/opt/%{name}/src/hidra/hidra_control/server.pyc
/opt/%{name}/src/hidra/hidra_control/server.pyo
%{_unitdir}/*.service
%config(noreplace) /opt/%{name}/conf/*
%attr(1777,root,root) /var/log/%{name}

%files -n python2-hidra
%doc examples
%{python2_sitelib}/*

%files -n python3-hidra
%doc examples
%{python3_sitelib}/*

%files -n hidra-control-client
/opt/%{name}/src/hidra/hidra_control/client.py
/opt/%{name}/src/hidra/hidra_control/client.pyc
/opt/%{name}/src/hidra/hidra_control/client.pyo
%config(noreplace) /opt/%{name}/conf/control_client.yaml

%changelog
* Fri Jul 10 2020 Manuela Kuhn <manuela.kuhn@desy.de> - 4.2.1-2
- Switch hidra and hidra-control-client to python3
* Wed Jul 08 2020 Manuela Kuhn <manuela.kuhn@desy.de> - 4.2.1-1
- Bump version
* Mon Jun 22 2020 Manuela Kuhn <manuela.kuhn@desy.de> - 4.2.0-2
- Add hidra lib packages for python2 and 3
* Tue Jun 16 2020 Manuela Kuhn <manuela.kuhn@desy.de> - 4.2.0-1
- Bump version
* Thu Apr 09 2020 Manuela Kuhn <manuela.kuhn@desy.de> - 4.1.8-1
- Bump version
* Wed Apr 01 2020 Manuela Kuhn <manuela.kuhn@desy.de> - 4.1.7-2
- Fix paths after restructuring
* Thu Feb 13 2020 Manuela Kuhn <manuela.kuhn@desy.de> - 4.1.7-1
- Bump version
* Wed Dec 11 2019 Manuela Kuhn <manuela.kuhn@desy.de> - 4.1.6-1
- Bump version
* Wed Nov 20 2019 Manuela Kuhn <manuela.kuhn@desy.de> - 4.1.5-1
- Bump version
* Mon Nov 04 2019 Manuela Kuhn <manuela.kuhn@desy.de> - 4.1.4-1
- Bump version
* Thu Oct 10 2019 Manuela Kuhn <manuela.kuhn@desy.de> - 4.1.3-1
- Bump version
* Tue Oct 01 2019 Manuela Kuhn <manuela.kuhn@desy.de> - 4.1.2-1
- Bump version
* Wed Sep 04 2019 Manuela Kuhn <manuela.kuhn@desy.de> - 4.1.1-1
- Bump version
* Thu Aug 01 2019 Manuela Kuhn <manuela.kuhn@desy.de> - 4.1.0-1
- Bump version
* Fri Jun 14 2019 Manuela Kuhn <manuela.kuhn@desy.de> - 4.0.23-1
- Bump version
* Tue Apr 23 2019 Manuela Kuhn <manuela.kuhn@desy.de> - 4.0.22-1
- Bump version
* Mon Apr 08 2019 Manuela Kuhn <manuela.kuhn@desy.de> - 4.0.21-1
- Bump version
* Fri Mar 22 2019 Manuela Kuhn <manuela.kuhn@desy.de> - 4.0.20-1
- Bump version
* Tue Mar 19 2019 Manuela Kuhn <manuela.kuhn@desy.de> - 4.0.19-1
- Bump version
* Fri Mar 08 2019 Manuela Kuhn <manuela.kuhn@desy.de> - 4.0.18-1
- Bump version
* Mon Mar 04 2019 Manuela Kuhn <manuela.kuhn@desy.de> - 4.0.17-1
- Bump version
* Fri Nov 30 2018 Manuela Kuhn <manuela.kuhn@desy.de> - 4.0.16-1
- Bump version
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
