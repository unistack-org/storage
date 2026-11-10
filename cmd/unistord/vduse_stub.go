//go:build !linux

package main

import (
	"context"
	"log"

	"github.com/unistack-org/storage/pkg/client"
	"github.com/unistack-org/storage/pkg/meta"
	"github.com/unistack-org/storage/pkg/nbd"
)

type vduseManager struct {
	logger *log.Logger
}

func newVDUSEManager(logger *log.Logger, flags vduseFlags) *vduseManager {
	return &vduseManager{logger: logger}
}

func (m *vduseManager) setContext(ctx context.Context) {}

func (m *vduseManager) start(ctx context.Context, es []*nbd.Export, specs []string) error {
	if len(specs) > 0 {
		return errVDUSEUnsupported
	}
	return nil
}

func (m *vduseManager) add(volume, devname string, cl *client.Cluster, spec meta.VolumeSpec, readOnly bool) (bool, error) {
	return false, errVDUSEUnsupported
}

func (m *vduseManager) remove(name string) error { return errVDUSEUnsupported }

func (m *vduseManager) list() []vduseDeviceInfo { return []vduseDeviceInfo{} }

func (m *vduseManager) closeAll() {}

// startVDUSE keeps the boot entry point: on this platform it fails when
// -vduse specs were requested, otherwise returns an idle manager.
func startVDUSE(ctx context.Context, logger *log.Logger, es []*nbd.Export, specs []string, trim, writeZeroes, readAhead bool, readAheadWindow, queues int) (*vduseManager, error) {
	m := newVDUSEManager(logger, vduseFlags{Trim: trim, WriteZeroes: writeZeroes, ReadAhead: readAhead, ReadAheadWindow: readAheadWindow, Queues: queues})
	if err := m.start(ctx, es, specs); err != nil {
		return nil, err
	}
	return m, nil
}
