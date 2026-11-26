package session

import (
	"errors"
	"os"

	"github.com/Project-Helianthus/helianthus-eebus-go/discovery"
	spineapi "github.com/Project-Helianthus/helianthus-spine-go/api"
)

var errDeviceAddress = errors.New("local SPINE device address is missing")

// AttachConfig uses the driver's local SPINE device and instance state root.
// If Transport is nil, the session uses the device's existing SPINE connections.
type AttachConfig struct {
	Device    spineapi.DeviceLocalInterface
	StateRoot string
	Transport Transport
}

// Attach loads retained feature pairs for a driver-owned SPINE device without
// starting SHIP or discovery or subscribing to process-wide events. The driver
// supplies trust, connections, results and notifications via Trust, Connect,
// NoteResult and ReceiveNotify.
//
// Attach once per device before SPINE dispatch starts. The driver must quiesce
// SPINE access to the device's features during Attach and Close: they install
// and remove the retained-subscription handler in the device's feature list.
func Attach(cfg AttachConfig) (*Instance, error) {
	if cfg.Device == nil {
		return nil, errDeviceAddress
	}
	address := cfg.Device.Address()
	if address == nil || *address == "" {
		return nil, errDeviceAddress
	}
	info, err := os.Stat(cfg.StateRoot)
	if err != nil || !info.IsDir() {
		return nil, errState
	}
	stored, err := loadState(cfg.StateRoot)
	if err != nil {
		return nil, err
	}
	s := &Instance{
		root:          cfg.StateRoot,
		device:        cfg.Device,
		localDevice:   *address,
		view:          discovery.NewNode(discovery.Announcement{}),
		transport:     cfg.Transport,
		trusted:       make(map[string]bool),
		connected:     make(map[string]bool),
		book:          newBook(),
		cache:         make(map[string]notified),
		peerHeld:      make(map[string][]Pair),
		peerHeldReady: make(map[string]bool),
		peerHeldAsked: make(map[string]bool),
		heldCounter:   make(map[string]uint64),
	}
	s.book.adopt(stored)
	if err := s.installRetainedRead(); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *Instance) restoreRetainedRead() {
	for _, entity := range s.device.Entities() {
		features := entity.Features()
		for i, feature := range features {
			if wrapped, ok := feature.(*retainedRead); ok && wrapped.instance == s {
				features[i] = wrapped.FeatureLocalInterface
				return
			}
		}
	}
}
