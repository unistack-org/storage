//go:build !linux

package store

// deviceIDImpl: no mount table, no /dev/disk/by-*, no sysfs off
// Linux. Path and the fstab label (where /etc/fstab exists) are all
// the identity a non-Linux dev box can offer; Device stays empty and
// ValidateDeviceID reports the platform cannot verify a pinned
// identity, so the daemon refuses strict pins off Linux rather than
// guessing.
func deviceIDImpl(string, *DeviceIdentity) error { return nil }
