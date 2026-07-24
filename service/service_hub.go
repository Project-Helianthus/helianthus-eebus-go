package service

import (
	"sort"

	"github.com/Project-Helianthus/helianthus-eebus-go/api"
	shipapi "github.com/Project-Helianthus/helianthus-ship-go/api"
	shipmodel "github.com/Project-Helianthus/helianthus-ship-go/model"
)

var _ shipapi.HubReaderInterface = (*Service)(nil)
var _ shipapi.OutgoingAttemptHubReaderInterface = (*Service)(nil)
var _ shipapi.PairingCandidateHubReaderInterface = (*Service)(nil)

// report a connection to a SKI
func (s *Service) RemoteSKIConnected(ski string) {
	s.serviceHandler.RemoteSKIConnected(s, ski)
}

// report a disconnection to a SKI
func (s *Service) RemoteSKIDisconnected(ski string) {
	if s.spineLocalDevice != nil {
		s.spineLocalDevice.RemoveRemoteDeviceConnection(ski)
	}

	s.serviceHandler.RemoteSKIDisconnected(s, ski)
}

// report an approved handshake by a remote device
func (s *Service) SetupRemoteDevice(ski string, writeI shipapi.ShipConnectionDataWriterInterface) shipapi.ShipConnectionDataReaderInterface {
	return s.LocalDevice().SetupRemoteDevice(ski, writeI)
}

// report all currently visible EEBUS services
func (s *Service) VisibleRemoteServicesUpdated(entries []shipapi.RemoteService) {
	s.serviceHandler.VisibleRemoteServicesUpdated(s, entries)
}

// VisiblePairingCandidatesUpdated forwards only opaque, process-local SHIP
// candidate references to an optional experimental reader.
func (s *Service) VisiblePairingCandidatesUpdated(candidates []shipapi.PairingCandidateRef) {
	reader, ok := s.serviceHandler.(api.PairingCandidateReader)
	if !ok || isNilInterface(reader) {
		return
	}
	reader.VisiblePairingCandidatesUpdated(s, cloneAndSortPairingCandidateRefs(candidates))
}

func cloneAndSortPairingCandidateRefs(candidates []shipapi.PairingCandidateRef) []shipapi.PairingCandidateRef {
	cloned := append([]shipapi.PairingCandidateRef(nil), candidates...)
	sort.Slice(cloned, func(left, right int) bool {
		return pairingCandidateRefLess(cloned[left], cloned[right])
	})
	return cloned
}

func pairingCandidateRefLess(left, right shipapi.PairingCandidateRef) bool {
	for _, values := range [][2]string{
		{left.SKI, right.SKI},
		{left.CandidateRef, right.CandidateRef},
		{left.Name, right.Name},
		{left.Identifier, right.Identifier},
		{left.Brand, right.Brand},
		{left.Type, right.Type},
		{left.Model, right.Model},
	} {
		if values[0] != values[1] {
			return values[0] < values[1]
		}
	}
	return false
}

// Provides the SHIP ID the remote service reported during the handshake process
// This needs to be persisted and passed on for future remote service connections
// when using `PairRemoteService`
func (s *Service) ServiceShipIDUpdate(ski string, shipdID string) {
	s.serviceHandler.ServiceShipIDUpdate(ski, shipdID)
}

// Provides the current pairing state for the remote service
// This is called whenever the state changes and can be used to
// provide user information for the pairing/connection process
func (s *Service) ServicePairingDetailUpdate(ski string, detail *shipapi.ConnectionStateDetail) {
	s.serviceHandler.ServicePairingDetailUpdate(ski, detail)
}

func (s *Service) OutgoingAttemptConnectionClosed(
	ski string,
	complete bool,
	metadata shipapi.OutgoingAttemptMetadata,
) {
	if isNilInterface(s.outgoingAttemptSink) {
		return
	}
	s.outgoingAttemptSink.OutgoingAttemptConnectionClosed(ski, complete, metadata)
}

func (s *Service) OutgoingAttemptHandshakeStateUpdate(
	ski string,
	state shipmodel.ShipState,
	metadata shipapi.OutgoingAttemptMetadata,
) {
	if isNilInterface(s.outgoingAttemptSink) {
		return
	}
	s.outgoingAttemptSink.OutgoingAttemptHandshakeStateUpdate(ski, state, metadata)
}

// return if the user is still able to trust the connection
func (s *Service) AllowWaitingForTrust(ski string) bool {
	s.mux.Lock()
	defer s.mux.Unlock()

	return s.isPairingPossible
}
