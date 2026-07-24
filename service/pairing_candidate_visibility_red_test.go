package service

import (
	"crypto/tls"
	"sync"
	"testing"
	"time"

	"github.com/Project-Helianthus/helianthus-eebus-go/api"
	shipapi "github.com/Project-Helianthus/helianthus-ship-go/api"
	"github.com/Project-Helianthus/helianthus-ship-go/cert"
	spinemodel "github.com/Project-Helianthus/helianthus-spine-go/model"
)

type pairingCandidateVisibilityReader struct {
	candidates []shipapi.PairingCandidateRef
}

func (*pairingCandidateVisibilityReader) RemoteSKIConnected(api.ServiceInterface, string)    {}
func (*pairingCandidateVisibilityReader) RemoteSKIDisconnected(api.ServiceInterface, string) {}
func (*pairingCandidateVisibilityReader) VisibleRemoteServicesUpdated(api.ServiceInterface, []shipapi.RemoteService) {
}
func (*pairingCandidateVisibilityReader) ServiceShipIDUpdate(string, string) {}
func (*pairingCandidateVisibilityReader) ServicePairingDetailUpdate(string, *shipapi.ConnectionStateDetail) {
}

func (reader *pairingCandidateVisibilityReader) VisiblePairingCandidatesUpdated(
	_ api.ServiceInterface,
	candidates []shipapi.PairingCandidateRef,
) {
	reader.candidates = candidates
}

var _ shipapi.PairingCandidateHubReaderInterface = (*Service)(nil)
var _ api.PairingCandidateReader = (*pairingCandidateVisibilityReader)(nil)

func TestServiceForwardsClonedAndDeterministicallyOrderedPairingCandidates(t *testing.T) {
	reader := &pairingCandidateVisibilityReader{}
	service := &Service{serviceHandler: reader}
	input := []shipapi.PairingCandidateRef{
		{CandidateRef: "candidate-b", Name: "zeta", SKI: "ski-z"},
		{CandidateRef: "candidate-b", Name: "alpha", SKI: "ski-a"},
		{CandidateRef: "candidate-a", Name: "zeta", SKI: "ski-z", Identifier: "identifier-b"},
		{CandidateRef: "candidate-a", Name: "zeta", SKI: "ski-z", Identifier: "identifier-a", Brand: "brand-b"},
		{CandidateRef: "candidate-a", Name: "zeta", SKI: "ski-z", Identifier: "identifier-a", Brand: "brand-a", Type: "type-b"},
		{CandidateRef: "candidate-a", Name: "alpha", SKI: "ski-z"},
	}

	service.VisiblePairingCandidatesUpdated(input)

	want := []shipapi.PairingCandidateRef{
		{CandidateRef: "candidate-b", Name: "alpha", SKI: "ski-a"},
		{CandidateRef: "candidate-a", Name: "alpha", SKI: "ski-z"},
		{CandidateRef: "candidate-a", Name: "zeta", SKI: "ski-z", Identifier: "identifier-a", Brand: "brand-a", Type: "type-b"},
		{CandidateRef: "candidate-a", Name: "zeta", SKI: "ski-z", Identifier: "identifier-a", Brand: "brand-b"},
		{CandidateRef: "candidate-a", Name: "zeta", SKI: "ski-z", Identifier: "identifier-b"},
		{CandidateRef: "candidate-b", Name: "zeta", SKI: "ski-z"},
	}
	if !pairingCandidateRefsEqual(reader.candidates, want) {
		t.Fatalf("forwarded candidates = %#v, want %#v", reader.candidates, want)
	}

	input[1].Name = "mutated-input"
	if reader.candidates[0].Name != "alpha" {
		t.Fatalf("reader candidates alias input: %#v", reader.candidates)
	}
	reader.candidates[0].Name = "mutated-reader"
	if input[1].Name != "mutated-input" {
		t.Fatalf("input aliases reader candidates: %#v", input)
	}
}

func TestServiceIgnoresAbsentUnsupportedAndTypedNilPairingCandidateReaders(t *testing.T) {
	candidates := []shipapi.PairingCandidateRef{{CandidateRef: "candidate-a", SKI: "ski-a"}}

	service := &Service{}
	service.VisiblePairingCandidatesUpdated(candidates)

	service.serviceHandler = &legacyPairingCandidateVisibilityReader{}
	service.VisiblePairingCandidatesUpdated(candidates)

	var typedNil *pairingCandidateVisibilityReader
	service.serviceHandler = typedNil
	service.VisiblePairingCandidatesUpdated(candidates)
}

func TestServiceForwardsEmptyCandidateSnapshotToClearReaderState(t *testing.T) {
	reader := &pairingCandidateVisibilityReader{}
	service := &Service{serviceHandler: reader}
	service.VisiblePairingCandidatesUpdated([]shipapi.PairingCandidateRef{{CandidateRef: "candidate-a", SKI: "ski-a"}})
	service.VisiblePairingCandidatesUpdated(nil)

	if len(reader.candidates) != 0 {
		t.Fatalf("empty candidate snapshot did not clear reader state: %#v", reader.candidates)
	}
}

func TestSetupPassesCandidateCapableServiceToSHIPHub(t *testing.T) {
	certificate, err := cert.CreateCertificate("unit", "org", "de", "cn")
	if err != nil {
		t.Fatalf("create certificate: %v", err)
	}
	configuration, err := api.NewConfiguration(
		"vendor", "brand", "model", "serial", spinemodel.DeviceTypeTypeEnergyManagementSystem,
		[]spinemodel.EntityTypeType{spinemodel.EntityTypeTypeCEM}, 4729, certificate, time.Second,
	)
	if err != nil {
		t.Fatalf("new configuration: %v", err)
	}
	service := NewService(configuration, &pairingCandidateVisibilityReader{})
	var reader shipapi.HubReaderInterface
	service.connectionsHubFactory = func(
		candidate shipapi.HubReaderInterface,
		_ shipapi.MdnsInterface,
		_ int,
		_ tls.Certificate,
		_ *shipapi.ServiceDetails,
	) shipapi.HubInterface {
		reader = candidate
		return &hubRuntimeRecorder{}
	}

	if err := service.Setup(); err != nil {
		t.Fatalf("setup: %v", err)
	}
	if reader != service {
		t.Fatalf("SHIP hub reader = %T, want Service", reader)
	}
	if _, ok := reader.(shipapi.PairingCandidateHubReaderInterface); !ok {
		t.Fatalf("SHIP hub reader %T does not expose pairing candidate visibility", reader)
	}
}

func TestServiceForwardsCandidateVisibilityAcrossLifecycleStates(t *testing.T) {
	for _, lifecycle := range []lifecycleState{
		lifecycleReady,
		lifecycleStarting,
		lifecycleRunning,
		lifecycleStopping,
		lifecycleStopped,
		lifecycleTerminal,
	} {
		t.Run(lifecycleName(lifecycle), func(t *testing.T) {
			reader := &pairingCandidateVisibilityReader{}
			service := &Service{serviceHandler: reader, lifecycle: lifecycle}
			service.VisiblePairingCandidatesUpdated([]shipapi.PairingCandidateRef{{CandidateRef: "candidate-a", SKI: "ski-a"}})
			if len(reader.candidates) != 1 {
				t.Fatalf("lifecycle %s suppressed read-only candidate visibility", lifecycleName(lifecycle))
			}
		})
	}
}

func TestServiceForwardsConcurrentCandidateVisibility(t *testing.T) {
	reader := &concurrentPairingCandidateVisibilityReader{}
	service := &Service{serviceHandler: reader}
	const callbacks = 32
	start := make(chan struct{})
	var wait sync.WaitGroup
	for index := 0; index < callbacks; index++ {
		wait.Add(1)
		go func(index int) {
			defer wait.Done()
			<-start
			service.VisiblePairingCandidatesUpdated([]shipapi.PairingCandidateRef{{CandidateRef: "candidate", Name: string(rune('a' + index%26))}})
		}(index)
	}
	close(start)
	wait.Wait()

	if reader.count() != callbacks {
		t.Fatalf("callbacks = %d, want %d", reader.count(), callbacks)
	}
}

func TestServiceAllowsReentrantCandidateVisibilityWithoutAliasingSnapshots(t *testing.T) {
	reader := &reentrantPairingCandidateVisibilityReader{}
	service := &Service{serviceHandler: reader}
	done := make(chan struct{})
	go func() {
		service.VisiblePairingCandidatesUpdated([]shipapi.PairingCandidateRef{{CandidateRef: "outer", Name: "outer"}})
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("reentrant candidate callback blocked")
	}

	if len(reader.snapshots) != 2 {
		t.Fatalf("snapshots = %#v, want outer and reentrant snapshots", reader.snapshots)
	}
	reader.snapshots[1][0].Name = "mutated-inner"
	if reader.snapshots[0][0].Name != "outer" {
		t.Fatalf("candidate snapshots alias each other: %#v", reader.snapshots)
	}
}

type legacyPairingCandidateVisibilityReader struct{}

func (*legacyPairingCandidateVisibilityReader) RemoteSKIConnected(api.ServiceInterface, string)    {}
func (*legacyPairingCandidateVisibilityReader) RemoteSKIDisconnected(api.ServiceInterface, string) {}
func (*legacyPairingCandidateVisibilityReader) VisibleRemoteServicesUpdated(api.ServiceInterface, []shipapi.RemoteService) {
}
func (*legacyPairingCandidateVisibilityReader) ServiceShipIDUpdate(string, string) {}
func (*legacyPairingCandidateVisibilityReader) ServicePairingDetailUpdate(string, *shipapi.ConnectionStateDetail) {
}

func pairingCandidateRefsEqual(left, right []shipapi.PairingCandidateRef) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

type concurrentPairingCandidateVisibilityReader struct {
	legacyPairingCandidateVisibilityReader
	mux       sync.Mutex
	snapshots [][]shipapi.PairingCandidateRef
}

func (reader *concurrentPairingCandidateVisibilityReader) VisiblePairingCandidatesUpdated(
	_ api.ServiceInterface,
	candidates []shipapi.PairingCandidateRef,
) {
	reader.mux.Lock()
	defer reader.mux.Unlock()
	reader.snapshots = append(reader.snapshots, candidates)
}

func (reader *concurrentPairingCandidateVisibilityReader) count() int {
	reader.mux.Lock()
	defer reader.mux.Unlock()
	return len(reader.snapshots)
}

type reentrantPairingCandidateVisibilityReader struct {
	legacyPairingCandidateVisibilityReader
	snapshots [][]shipapi.PairingCandidateRef
}

func (reader *reentrantPairingCandidateVisibilityReader) VisiblePairingCandidatesUpdated(
	service api.ServiceInterface,
	candidates []shipapi.PairingCandidateRef,
) {
	reader.snapshots = append(reader.snapshots, candidates)
	if len(reader.snapshots) == 1 {
		service.(*Service).VisiblePairingCandidatesUpdated([]shipapi.PairingCandidateRef{{CandidateRef: "inner", Name: "inner"}})
	}
}
