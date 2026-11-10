# Unistack SDS (storage) release build.
#
# Mirrors unistack/Makefile: date-based VERSION stamped into the binaries
# via internal/buildinfo, linux/amd64 + linux/arm64 artifacts + SHA256SUMS.
# The release workflow (GitHub Releases) builds through this file, so a
# local `make release` produces exactly what CI publishes.

VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
COMMIT ?= $(shell git rev-parse HEAD 2>/dev/null || echo unknown)
CHANNEL ?= develop
DATE ?= $(shell git log -1 --format=%cI 2>/dev/null || echo "")
PKG := github.com/unistack-org/storage/internal/buildinfo
LDFLAGS := -s -w \
	-X $(PKG).Version=$(VERSION) \
	-X $(PKG).Commit=$(COMMIT) \
	-X $(PKG).Channel=$(CHANNEL) \
	-X $(PKG).Date=$(DATE)

CMDS := unistord unistorctl sdsbench
ARCHES := amd64 arm64

export GOWORK := off
export CGO_ENABLED := 0

.PHONY: release clean

release:
	@mkdir -p dist
	@for a in $(ARCHES); do \
		for c in $(CMDS); do \
			echo "building $$c-linux-$$a"; \
			GOOS=linux GOARCH=$$a go build -trimpath -ldflags="$(LDFLAGS)" \
				-o dist/$$c-linux-$$a ./cmd/$$c || exit 1; \
		done; \
	done
	cd dist && sha256sum unistord-linux-* unistorctl-linux-* sdsbench-linux-* > SHA256SUMS

clean:
	rm -rf dist
