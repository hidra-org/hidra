Name:		hidra-lib
Version:	3.0.2
Release:	1%{?dist}
Summary:	Libraries to connect or control HiDRA

License:	AGPLv3
URL:		https://stash.desy.de/projects/HIDRA/repos/hidra
Source0:	hidra-lib-v%{version}.zip
#Source1:	hidra.service

BuildArch:	noarch
BuildRequires:	python-devel
BuildRequires:	python-setuptools
Requires:	python-zmq >= 14.1.0

%description
Libraries to connect or control HiDRA

%prep
%setup -q -c %{name}-%{version}

#%build
#%{__python} setup.py build

%install
# Packaging Python API
mkdir -p %{buildroot}/%{python_sitelib}/%{name}
cp -r src/APIs/hidra/*.py %{buildroot}/%{python_sitelib}/%{name}/

%files
%doc docs/*
%{python_sitelib}/*

%changelog
* Tue Apr 12 2016 Manuela Kuhn <manuela.kuhn@desy.de> - 3.0.2-1
- Initial packaging
