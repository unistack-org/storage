#!/usr/bin/env bash
#
# package.sh — build installable .deb / .rpm / .apk packages from dist/.
#
# Run from anywhere; paths resolve relative to this script. Inputs:
#   VERSION    release version, "v" prefix allowed (required). Pre-release
#              suffixes are adapted per packager:
#                deb: 1.2-rc1 -> 1.2rc1
#                rpm: 1.2-rc1 -> version 1.2, release 1.rc1
#                apk: 1.2-rc1 -> 1.2_rc1
#   DIST_DIR   directory with the built binaries (default <sds-nbd>/dist)
#   ARCHES     space list of GOARCH to package (default "amd64 arm64")
#   APK_DIR    where to copy built apks (apk needs an alpine container;
#              the release workflow runs this script there. Skipped when
#              abuild is absent.)
#
# Requirements: dpkg-deb and rpmbuild (both present on ubuntu-latest).
#
# Output: $DIST_DIR/unistord_<ver>_<arch>.deb
#         $DIST_DIR/unistord-<ver>-<rel>.<arch>.rpm
#         $APK_DIR/unistord-<ver>-r0.apk
set -euo pipefail

VERSION="${VERSION:?VERSION is required}"
NBD_DIR="$(cd "$(dirname "$0")/.." && pwd)"
DIST_DIR="${DIST_DIR:-$NBD_DIR/dist}"
APK_DIR="${APK_DIR:-}"
ARCHES="${ARCHES:-amd64 arm64}"
TPL="$NBD_DIR/scripts/package-templates"
CMDS=(unistord unistorctl sdsbench)

VER="${VERSION#v}"
DEB_VER="${VER//-/}"
RPM_VER="${VER%%-*}"
if [ "$VER" = "$RPM_VER" ]; then
  RPM_REL="1"
else
  RPM_REL="1.${VER#*-}"
fi
APK_VER="${VER//-/_}"

have() { command -v "$1" >/dev/null 2>&1; }

# Stage the filesystem tree a package ships: binaries in /usr/bin, the
# systemd unit (repointed from the source-build path /usr/local/bin),
# a placeholder env file and the state directory.
pkg_tree() { # $1=arch $2=root
  local arch="$1" root="$2" c
  rm -rf "$root"
  install -d -m 0755 "$root/usr/bin" "$root/etc/unistord" "$root/var/lib/unistord" \
    "$root/lib/systemd/system"
  for c in "${CMDS[@]}"; do
    install -m 0755 "$DIST_DIR/$c-linux-$arch" "$root/usr/bin/$c"
  done
  sed 's|/usr/local/bin/unistord|/usr/bin/unistord|' "$NBD_DIR/deploy/unistord.service" \
    > "$root/lib/systemd/system/unistord.service"
  : > "$root/etc/unistord/unistord.env"
  chmod 0640 "$root/etc/unistord/unistord.env"
}

# ---------- .deb ----------
if have dpkg-deb; then
  for arch in $ARCHES; do
    tree="$(mktemp -d)"
    pkg_tree "$arch" "$tree"
    install -d -m 0755 "$tree/DEBIAN"
    {
      echo "Package: unistord"
      echo "Version: $DEB_VER"
      echo "Section: admin"
      echo "Priority: optional"
      echo "Architecture: $arch"
      echo "Maintainer: Unistack <maintainers@unistack.org>"
      echo "Description: Unistack SDS storage daemon (NBD/VDUSE)"
      echo " Unistack software-defined storage: unistord serves block volumes"
      echo " over NBD (TCP/TLS) and VDUSE, with unistorctl for administration"
      echo " and sdsbench for benchmarks."
    } > "$tree/DEBIAN/control"
    cat > "$tree/DEBIAN/postinst" <<'EOF'
#!/bin/sh
set -e
systemctl daemon-reload >/dev/null 2>&1 || true
EOF
    chmod 0755 "$tree/DEBIAN/postinst"
    cat > "$tree/DEBIAN/prerm" <<'EOF'
#!/bin/sh
set -e
systemctl --no-reload disable --now unistord >/dev/null 2>&1 || true
EOF
    chmod 0755 "$tree/DEBIAN/prerm"
    dpkg-deb --root-owner-group -Zgzip -b "$tree" "$DIST_DIR/unistord_${DEB_VER}_$arch.deb" >/dev/null
    rm -rf "$tree"
    echo "built $DIST_DIR/unistord_${DEB_VER}_$arch.deb"
  done
else
  echo "package.sh: dpkg-deb not found, skipping deb" >&2
fi

# ---------- .rpm ----------
if have rpmbuild; then
  for arch in $ARCHES; do
    tree="$(mktemp -d)"
    pkg_tree "$arch" "$tree"
    rpmbuild -bb --define "_topdir $tree/rpm" \
      --define "_pkgroot $tree" \
      --define "pkgversion $RPM_VER" \
      --define "pkgrelease $RPM_REL" \
      --target "$arch-redhat-linux" \
      "$TPL/unistord.spec" > "$tree/rpmbuild.log" 2>&1 || {
        cat "$tree/rpmbuild.log" >&2
        exit 1
      }
    built="$(find "$tree/rpm/RPMS" -name 'unistord-*.rpm' -print -quit)"
    cp "$built" "$DIST_DIR/"
    rm -rf "$tree"
    echo "built $DIST_DIR/$(basename "$built")"
  done
else
  echo "package.sh: rpmbuild not found, skipping rpm" >&2
fi

# ---------- .apk ----------
if have abuild && [ -n "$APK_DIR" ]; then
  mkdir -p "$APK_DIR" "$HOME/.abuild"
  if [ ! -f "$HOME/.abuild/abuild.conf" ]; then
    abuild-keygen -a -i -n >/dev/null
  fi
  work="$(mktemp -d)"
  {
    echo '# Maintainer: Unistack <maintainers@unistack.org>'
    echo "pkgname=unistord"
    echo "pkgver=$APK_VER"
    echo 'pkgrel=0'
    echo 'pkgdesc="Unistack SDS storage daemon (NBD/VDUSE)"'
    echo 'url="https://github.com/unistack-org/storage"'
    echo 'license="Apache-2.0"'
    echo 'arch="all"'
    echo 'source=""'
    echo 'options="!check"'
    echo 'builddir="$srcdir"'
    echo
    echo 'package() {'
    echo '  case "$CARCH" in x86_64) goarch=amd64 ;; aarch64) goarch=arm64 ;; *) echo "unsupported CARCH=$CARCH" >&2; exit 1 ;; esac'
    for c in "${CMDS[@]}"; do
      echo "  install -D -m 0755 \"$DIST_DIR/$c-linux-\$goarch\" \"\$pkgdir/usr/bin/$c\""
    done
    echo "  install -D -m 0755 \"$TPL/unistord-openrc\" \"\$pkgdir/etc/init.d/unistord\""
    echo '  install -d -m 0755 "$pkgdir/etc/unistord" "$pkgdir/var/lib/unistord"'
    echo '  : > "$pkgdir/etc/unistord/unistord.env"'
    echo '  chmod 0640 "$pkgdir/etc/unistord/unistord.env"'
    echo '}'
  } > "$work/APKBUILD"
  ( cd "$work" && abuild -r >/dev/null )
  # abuild installs built packages into $PKGDEST (default $HOME/packages).
  find "${PKGDEST:-$HOME/packages}" -name 'unistord-*.apk' -exec cp -t "$APK_DIR/" {} +
  rm -rf "$work"
  echo "built apks into $APK_DIR"
else
  echo "package.sh: abuild not found (the apk job runs in an alpine container), skipping" >&2
fi
