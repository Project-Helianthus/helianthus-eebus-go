package internal

import (
	"sync"
	"sync/atomic"

	spineapi "github.com/Project-Helianthus/helianthus-spine-go/api"
)

// EventScope is shared by the use-case client and its base handler.
type EventScope struct {
	stopped atomic.Bool
	mu      sync.Mutex
	remotes map[spineapi.DeviceRemoteInterface]string
}

func (s *EventScope) Owns(localEntity spineapi.EntityLocalInterface, payload spineapi.EventPayload) bool {
	if s.stopped.Load() || localEntity == nil {
		return false
	}
	local := localEntity.Device()
	if local == nil {
		return false
	}
	if payload.LocalFeature != nil {
		featureDevice := payload.LocalFeature.Device()
		if featureDevice == nil {
			return false
		}
		a, b := local.Address(), featureDevice.Address()
		if a == nil || b == nil || *a != *b {
			return false
		}
	}

	remote := payload.Device
	if payload.Entity != nil {
		entityDevice := payload.Entity.Device()
		if entityDevice == nil || (remote != nil && remote != entityDevice) {
			return false
		}
		remote = entityDevice
	}
	if remote == nil {
		return payload.LocalFeature != nil
	}
	if payload.Ski == "" {
		return false
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stopped.Load() {
		return false
	}
	if local.RemoteDeviceForSki(payload.Ski) == remote {
		if s.remotes == nil {
			s.remotes = make(map[spineapi.DeviceRemoteInterface]string)
		}
		s.remotes[remote] = payload.Ski
		return true
	}
	// A device removal is published after the remote has left the local map.
	return s.remotes[remote] == payload.Ski && (IsDeviceDisconnected(payload) || IsEntityDisconnected(payload))
}

func (s *EventScope) ForgetRemote(remote spineapi.DeviceRemoteInterface) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.remotes, remote)
}

func (s *EventScope) Stop() {
	s.stopped.Store(true)
	s.mu.Lock()
	defer s.mu.Unlock()
	clear(s.remotes)
}
