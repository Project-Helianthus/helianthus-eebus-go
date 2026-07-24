package service

import (
	"testing"

	"github.com/Project-Helianthus/helianthus-eebus-go/api"
	shipapi "github.com/Project-Helianthus/helianthus-ship-go/api"
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
		{CandidateRef: "candidate-a", Name: "zeta", SKI: "ski-z", Identifier: "identifier-b"},
		{CandidateRef: "candidate-a", Name: "zeta", SKI: "ski-z", Identifier: "identifier-a", Brand: "brand-b"},
		{CandidateRef: "candidate-a", Name: "zeta", SKI: "ski-z", Identifier: "identifier-a", Brand: "brand-a", Type: "type-b"},
		{CandidateRef: "candidate-a", Name: "alpha", SKI: "ski-z"},
	}

	service.VisiblePairingCandidatesUpdated(input)

	want := []shipapi.PairingCandidateRef{
		{CandidateRef: "candidate-a", Name: "alpha", SKI: "ski-z"},
		{CandidateRef: "candidate-a", Name: "zeta", SKI: "ski-z", Identifier: "identifier-a", Brand: "brand-a", Type: "type-b"},
		{CandidateRef: "candidate-a", Name: "zeta", SKI: "ski-z", Identifier: "identifier-a", Brand: "brand-b"},
		{CandidateRef: "candidate-a", Name: "zeta", SKI: "ski-z", Identifier: "identifier-b"},
		{CandidateRef: "candidate-b", Name: "zeta", SKI: "ski-z"},
	}
	if !pairingCandidateRefsEqual(reader.candidates, want) {
		t.Fatalf("forwarded candidates = %#v, want %#v", reader.candidates, want)
	}

	input[0].Name = "mutated-input"
	if reader.candidates[len(reader.candidates)-1].Name != "zeta" {
		t.Fatalf("reader candidates alias input: %#v", reader.candidates)
	}
	reader.candidates[0].Name = "mutated-reader"
	if input[4].Name != "alpha" {
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
