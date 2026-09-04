package service

import (
	"net"
	"net/netip"
	"sync"
	"testing"

	"github.com/Project-Helianthus/helianthus-eebus-go/api"
	shipapi "github.com/Project-Helianthus/helianthus-ship-go/api"
)

type nativeDiscoveryReader struct {
	mux       sync.Mutex
	snapshots []shipapi.PairingCandidateDiscoverySnapshotV1
}

func (*nativeDiscoveryReader) RemoteSKIConnected(api.ServiceInterface, string)    {}
func (*nativeDiscoveryReader) RemoteSKIDisconnected(api.ServiceInterface, string) {}
func (*nativeDiscoveryReader) VisibleRemoteServicesUpdated(api.ServiceInterface, []shipapi.RemoteService) {
}
func (*nativeDiscoveryReader) ServiceShipIDUpdate(string, string) {}
func (*nativeDiscoveryReader) ServicePairingDetailUpdate(string, *shipapi.ConnectionStateDetail) {
}
func (reader *nativeDiscoveryReader) VisiblePairingCandidateDiscoverySnapshotUpdated(
	_ api.ServiceInterface,
	snapshot shipapi.PairingCandidateDiscoverySnapshotV1,
) {
	reader.mux.Lock()
	reader.snapshots = append(reader.snapshots, snapshot)
	reader.mux.Unlock()
}

func (reader *nativeDiscoveryReader) snapshot() []shipapi.PairingCandidateDiscoverySnapshotV1 {
	reader.mux.Lock()
	defer reader.mux.Unlock()
	return append([]shipapi.PairingCandidateDiscoverySnapshotV1(nil), reader.snapshots...)
}

var _ api.PairingCandidateReader = (*nativeDiscoveryReader)(nil)
var _ shipapi.PairingCandidateDiscoverySnapshotHubReaderInterface = (*Service)(nil)

func TestNativeDiscoverySnapshotRoundTripsAllFieldsOrderedAndDetached(t *testing.T) {
	reader := &nativeDiscoveryReader{}
	service := &Service{serviceHandler: reader}
	input := shipapi.PairingCandidateDiscoverySnapshotV1{
		ObservationRevision: 41,
		NewEntries:          true,
		Candidates: []shipapi.PairingCandidateDiscoveryObservationV1{
			{
				CandidateRef: "candidate-z", Name: "second", SKI: "ski-z", Identifier: "id-z",
				Brand: "brand-z", Type: "type-z", Model: "model-z", Path: "/ship/z", Host: "z.invalid",
				Port: 4712, Register: true, Addresses: []net.IP{net.ParseIP("192.0.2.41")},
				ScopedAddresses:           []netip.Addr{netip.MustParseAddr("fe80::41").WithZone("synthetic1")},
				UnscopedLinkLocalObserved: true,
			},
			{
				CandidateRef: "candidate-a", Name: "first", SKI: "ski-a", Identifier: "id-a",
				Brand: "brand-a", Type: "type-a", Model: "model-a", Path: "/ship/a", Host: "a.invalid",
				Port: 4713, Addresses: []net.IP{net.ParseIP("2001:db8::41")},
				ScopedAddresses: []netip.Addr{netip.MustParseAddr("2001:db8::41")},
			},
		},
	}

	service.VisiblePairingCandidateDiscoverySnapshotUpdated(input)
	input.Candidates[1].Name = "mutated-input"
	input.Candidates[1].Addresses[0][0] ^= 0xff
	input.Candidates[1].ScopedAddresses[0] = netip.MustParseAddr("2001:db8::99")

	got := reader.snapshot()
	if len(got) != 1 || got[0].ObservationRevision != 41 || !got[0].NewEntries {
		t.Fatalf("snapshot context = %#v, want revision 41 with new entries", got)
	}
	if len(got[0].Candidates) != 2 || got[0].Candidates[0].CandidateRef != "candidate-a" ||
		got[0].Candidates[1].CandidateRef != "candidate-z" {
		t.Fatalf("candidate order = %#v, want candidate-a then candidate-z", got[0].Candidates)
	}
	first := got[0].Candidates[0]
	if first.Name != "first" || first.SKI != "ski-a" || first.Identifier != "id-a" ||
		first.Brand != "brand-a" || first.Type != "type-a" || first.Model != "model-a" ||
		first.Path != "/ship/a" || first.Host != "a.invalid" || first.Port != 4713 || first.Register ||
		len(first.Addresses) != 1 || !first.Addresses[0].Equal(net.ParseIP("2001:db8::41")) ||
		len(first.ScopedAddresses) != 1 || first.ScopedAddresses[0] != netip.MustParseAddr("2001:db8::41") ||
		first.UnscopedLinkLocalObserved {
		t.Fatalf("first native observation changed or aliased input: %#v", first)
	}
	second := got[0].Candidates[1]
	if !second.Register || !second.UnscopedLinkLocalObserved || second.Path != "/ship/z" ||
		second.Host != "z.invalid" || second.Port != 4712 ||
		len(second.ScopedAddresses) != 1 || second.ScopedAddresses[0].Zone() != "synthetic1" {
		t.Fatalf("second native observation lost endpoint or context: %#v", second)
	}
}

func TestNativeDiscoverySnapshotPreservesAbsenceAndRestartRevision(t *testing.T) {
	firstReader := &nativeDiscoveryReader{}
	first := &Service{serviceHandler: firstReader}
	first.VisiblePairingCandidateDiscoverySnapshotUpdated(shipapi.PairingCandidateDiscoverySnapshotV1{
		ObservationRevision: 99,
		Candidates: []shipapi.PairingCandidateDiscoveryObservationV1{{
			CandidateRef: "candidate-before-restart", SKI: "ski-before-restart",
		}},
	})

	restartedReader := &nativeDiscoveryReader{}
	restarted := &Service{serviceHandler: restartedReader}
	restarted.VisiblePairingCandidateDiscoverySnapshotUpdated(shipapi.PairingCandidateDiscoverySnapshotV1{
		ObservationRevision: 1,
		Candidates:          []shipapi.PairingCandidateDiscoveryObservationV1{},
	})

	got := restartedReader.snapshot()
	if len(got) != 1 || got[0].ObservationRevision != 1 || got[0].Candidates == nil || len(got[0].Candidates) != 0 {
		t.Fatalf("restart clear = %#v, want authoritative non-nil empty revision 1", got)
	}
}

func TestNativeDiscoverySnapshotShutdownClearsOnceAndSuppressesLaterUpdates(t *testing.T) {
	hub := &candidateVisibilityShutdownHub{
		entered: make(chan struct{}),
		release: make(chan struct{}),
		done:    make(chan struct{}),
	}
	close(hub.release)
	reader := &nativeDiscoveryReader{}
	service := &Service{serviceHandler: reader, connectionsHub: hub, lifecycle: lifecycleRunning}
	service.VisiblePairingCandidateDiscoverySnapshotUpdated(shipapi.PairingCandidateDiscoverySnapshotV1{
		ObservationRevision: 7,
		NewEntries:          true,
		Candidates: []shipapi.PairingCandidateDiscoveryObservationV1{{
			CandidateRef: "candidate-before-shutdown", SKI: "ski-before-shutdown",
		}},
	})
	service.Shutdown()
	service.VisiblePairingCandidateDiscoverySnapshotUpdated(shipapi.PairingCandidateDiscoverySnapshotV1{
		ObservationRevision: 8,
		Candidates: []shipapi.PairingCandidateDiscoveryObservationV1{{
			CandidateRef: "candidate-after-shutdown", SKI: "ski-after-shutdown",
		}},
	})
	service.Shutdown()

	got := reader.snapshot()
	if len(got) != 2 || got[0].ObservationRevision != 7 || len(got[0].Candidates) != 1 ||
		got[1].ObservationRevision != 7 || got[1].NewEntries || got[1].Candidates == nil || len(got[1].Candidates) != 0 {
		t.Fatalf("shutdown snapshots = %#v, want revision 7 observation then one terminal clear", got)
	}
}

func TestNativeDiscoverySnapshotIgnoresAbsentAndTypedNilReaders(t *testing.T) {
	snapshot := shipapi.PairingCandidateDiscoverySnapshotV1{ObservationRevision: 1}
	(&Service{}).VisiblePairingCandidateDiscoverySnapshotUpdated(snapshot)
	var typedNil *nativeDiscoveryReader
	(&Service{serviceHandler: typedNil}).VisiblePairingCandidateDiscoverySnapshotUpdated(snapshot)
}
