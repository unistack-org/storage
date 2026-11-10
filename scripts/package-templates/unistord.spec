%global debug_package %{nil}

Name:           unistord
Version:        %{pkgversion}
Release:        %{pkgrelease}%{?dist}
Summary:        Unistack SDS storage daemon (NBD/VDUSE)
License:        Apache-2.0
URL:            https://github.com/unistack-org/storage
AutoReqProv:    no

%description
Unistack software-defined storage: unistord serves block volumes over
NBD (TCP/TLS) and VDUSE, with unistorctl for administration and sdsbench
for benchmarks.

%prep

%build

%install
rm -rf %{buildroot}
mkdir -p %{buildroot}
cp -a %{_pkgroot}/usr %{_pkgroot}/etc %{_pkgroot}/var %{_pkgroot}/lib %{buildroot}/

%post
systemctl daemon-reload >/dev/null 2>&1 || true

%preun
if [ $1 -eq 0 ]; then
  systemctl --no-reload disable --now unistord >/dev/null 2>&1 || true
fi

%files
%defattr(-,root,root,-)
/usr/bin/unistord
/usr/bin/unistorctl
/usr/bin/sdsbench
/lib/systemd/system/unistord.service
%dir /etc/unistord
%config(noreplace) /etc/unistord/unistord.env
%dir /var/lib/unistord
