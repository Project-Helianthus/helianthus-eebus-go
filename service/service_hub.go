package service

import (
	"net"
	"net/netip"
	"sort"

	"github.com/Project-Helianthus/helianthus-eebus-go/api"
	shipapi "github.com/Project-Helianthus/helianthus-ship-go/api"
	shipmodel "github.com/Project-Helianthus/helianthus-ship-go/model"
)

var _ shipapi.HubReaderInterface = (*Service)(nil)
var _ shipapi.OutgoingAttemptHubReaderInterface = (*Service)(nil)
var _ shipapi.PairingCandidateHubReaderInterface = (*Service)(nil)
var _ shipapi.PairingCandidateDiscoverySnapshotHubReaderInterface = (*Service)(nil)

type legacyPairingCandidateReader interface {
	VisiblePairingCandidatesUpdated(service api.ServiceInterface, candidates []shipapi.PairingCandidateRef)
}

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

// VisiblePairingCandidatesUpdated retains the former endpoint-redacted callback
// for compatibility. A native reader receives only the complete V1 callback.
func (s *Service) VisiblePairingCandidatesUpdated(candidates []shipapi.PairingCandidateRef) {
	if reader, ok := s.serviceHandler.(api.PairingCandidateReader); ok && !isNilInterface(reader) {
		return
	}
	reader, ok := s.serviceHandler.(legacyPairingCandidateReader)
	if !ok || isNilInterface(reader) {
		return
	}
	dispatchPairingCandidateVisibility(s, pairingCandidateVisibilityEvent{
		legacyReader: reader,
		candidates:   cloneAndSortPairingCandidateRefs(candidates),
	})
}

// VisiblePairingCandidateDiscoverySnapshotUpdated forwards a complete native
// snapshot without granting any pairing, connection, or trust authority.
func (s *Service) VisiblePairingCandidateDiscoverySnapshotUpdated(
	snapshot shipapi.PairingCandidateDiscoverySnapshotV1,
) {
	reader, ok := s.serviceHandler.(api.PairingCandidateReader)
	if !ok || isNilInterface(reader) {
		return
	}
	dispatchPairingCandidateVisibility(s, pairingCandidateVisibilityEvent{
		nativeReader: reader,
		snapshot:     cloneAndSortPairingCandidateDiscoverySnapshot(snapshot),
	})
}

type pairingCandidateVisibilityEvent struct {
	nativeReader api.PairingCandidateReader
	legacyReader legacyPairingCandidateReader
	snapshot     shipapi.PairingCandidateDiscoverySnapshotV1
	candidates   []shipapi.PairingCandidateRef
	done         chan struct{}
}

func dispatchPairingCandidateVisibility(
	service *Service,
	event pairingCandidateVisibilityEvent,
) {
	service.candidateVisibilityMux.Lock()
	if service.candidateClosed {
		service.candidateVisibilityMux.Unlock()
		return
	}
	if !isNilInterface(event.nativeReader) {
		service.candidateRevision = event.snapshot.ObservationRevision
	}
	if pending := len(service.candidateVisibility); pending > 0 &&
		service.candidateVisibility[pending-1].done == nil {
		service.candidateVisibility[pending-1] = event
	} else {
		service.candidateVisibility = append(service.candidateVisibility, event)
	}
	if service.candidateDispatching {
		service.candidateVisibilityMux.Unlock()
		return
	}
	service.candidateDispatching = true
	service.candidateVisibilityMux.Unlock()
	drainPairingCandidateVisibility(service)
}

func closePairingCandidateVisibilityAdmissions(service *Service) {
	service.candidateVisibilityMux.Lock()
	service.candidateClosed = true
	service.candidateVisibilityMux.Unlock()
}

func emitTerminalPairingCandidateVisibility(service *Service) {
	event := pairingCandidateVisibilityEvent{done: make(chan struct{})}
	if reader, ok := service.serviceHandler.(api.PairingCandidateReader); ok && !isNilInterface(reader) {
		event.nativeReader = reader
	} else if reader, ok := service.serviceHandler.(legacyPairingCandidateReader); ok && !isNilInterface(reader) {
		event.legacyReader = reader
		event.candidates = []shipapi.PairingCandidateRef{}
	} else {
		return
	}

	service.candidateVisibilityMux.Lock()
	if service.candidateTerminal {
		service.candidateVisibilityMux.Unlock()
		return
	}
	service.candidateTerminal = true
	if !isNilInterface(event.nativeReader) {
		event.snapshot = shipapi.PairingCandidateDiscoverySnapshotV1{
			ObservationRevision: service.candidateRevision,
			Candidates:          []shipapi.PairingCandidateDiscoveryObservationV1{},
		}
	}
	service.candidateVisibility = append(service.candidateVisibility, event)
	shouldDrain := !service.candidateDispatching
	if shouldDrain {
		service.candidateDispatching = true
	}
	service.candidateVisibilityMux.Unlock()

	if shouldDrain {
		drainPairingCandidateVisibility(service)
		<-event.done
	}
}

func drainPairingCandidateVisibility(service *Service) {
	for {
		service.candidateVisibilityMux.Lock()
		if len(service.candidateVisibility) == 0 {
			service.candidateDispatching = false
			service.candidateVisibilityMux.Unlock()
			return
		}
		event := service.candidateVisibility[0]
		service.candidateVisibility[0] = pairingCandidateVisibilityEvent{}
		service.candidateVisibility = service.candidateVisibility[1:]
		service.candidateVisibilityMux.Unlock()

		if !isNilInterface(event.nativeReader) {
			event.nativeReader.VisiblePairingCandidateDiscoverySnapshotUpdated(service, event.snapshot)
		} else {
			event.legacyReader.VisiblePairingCandidatesUpdated(service, event.candidates)
		}
		if event.done != nil {
			close(event.done)
		}
	}
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

func cloneAndSortPairingCandidateDiscoverySnapshot(
	snapshot shipapi.PairingCandidateDiscoverySnapshotV1,
) shipapi.PairingCandidateDiscoverySnapshotV1 {
	cloned := snapshot
	if snapshot.Candidates != nil {
		cloned.Candidates = make([]shipapi.PairingCandidateDiscoveryObservationV1, len(snapshot.Candidates))
		for index, candidate := range snapshot.Candidates {
			cloned.Candidates[index] = candidate
			cloned.Candidates[index].Addresses = clonePairingCandidateAddresses(candidate.Addresses)
			cloned.Candidates[index].ScopedAddresses = append([]netip.Addr(nil), candidate.ScopedAddresses...)
		}
	}
	sort.SliceStable(cloned.Candidates, func(left, right int) bool {
		return pairingCandidateDiscoveryObservationLess(cloned.Candidates[left], cloned.Candidates[right])
	})
	return cloned
}

func clonePairingCandidateAddresses(addresses []net.IP) []net.IP {
	if addresses == nil {
		return nil
	}
	cloned := make([]net.IP, len(addresses))
	for index, address := range addresses {
		cloned[index] = append(net.IP(nil), address...)
	}
	return cloned
}

func pairingCandidateDiscoveryObservationLess(
	left shipapi.PairingCandidateDiscoveryObservationV1,
	right shipapi.PairingCandidateDiscoveryObservationV1,
) bool {
	for _, values := range [][2]string{
		{left.CandidateRef, right.CandidateRef},
		{left.SKI, right.SKI},
		{left.Name, right.Name},
		{left.Identifier, right.Identifier},
		{left.Brand, right.Brand},
		{left.Type, right.Type},
		{left.Model, right.Model},
		{left.Path, right.Path},
		{left.Host, right.Host},
	} {
		if values[0] != values[1] {
			return values[0] < values[1]
		}
	}
	return left.Port < right.Port
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
