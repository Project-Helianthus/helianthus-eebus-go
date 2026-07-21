package service

import (
	"errors"
	"testing"

	eebusapi "github.com/Project-Helianthus/helianthus-eebus-go/api"
	shipapi "github.com/Project-Helianthus/helianthus-ship-go/api"
)

const (
	pairingCandidateFacadeTestRef = "shipc_observation-generation-17"
	pairingCandidateFacadeTestSKI = "b1b7197b064084e4cfef2365105d8d36ff185e5b"
)

type pairingCandidateCall struct {
	ref string
	ski string
}

type pairingCandidateHubSpy struct {
	shipapi.HubInterface
	err   error
	calls []pairingCandidateCall
}

func (hub *pairingCandidateHubSpy) QueuePairingCandidate(candidateRef, expectedSKI string) error {
	hub.calls = append(hub.calls, pairingCandidateCall{ref: candidateRef, ski: expectedSKI})
	return hub.err
}

func TestServiceExposesOptionalPairingCandidateQueue(t *testing.T) {
	var _ eebusapi.PairingCandidateQueuer = (*Service)(nil)
}

func TestServiceForwardsPairingCandidateReferenceAndExpectedSKIExactlyOnce(t *testing.T) {
	hub := &pairingCandidateHubSpy{}
	service := &Service{connectionsHub: hub}

	if err := service.QueuePairingCandidate(pairingCandidateFacadeTestRef, pairingCandidateFacadeTestSKI); err != nil {
		t.Fatalf("queue pairing candidate: %v", err)
	}
	want := pairingCandidateCall{ref: pairingCandidateFacadeTestRef, ski: pairingCandidateFacadeTestSKI}
	if len(hub.calls) != 1 || hub.calls[0] != want {
		t.Fatalf("pairing candidate calls = %v, want [%+v]", hub.calls, want)
	}
}

func TestServicePropagatesPairingCandidateFailure(t *testing.T) {
	wantErr := errors.New("candidate rejected")
	hub := &pairingCandidateHubSpy{err: wantErr}
	service := &Service{connectionsHub: hub}

	if err := service.QueuePairingCandidate(pairingCandidateFacadeTestRef, pairingCandidateFacadeTestSKI); !errors.Is(err, wantErr) {
		t.Fatalf("queue error = %v, want %v", err, wantErr)
	}
}

func TestServiceRejectsHubWithoutPairingCandidateCapability(t *testing.T) {
	service := &Service{connectionsHub: &hubRuntimeRecorder{}}
	if err := service.QueuePairingCandidate(pairingCandidateFacadeTestRef, pairingCandidateFacadeTestSKI); err == nil {
		t.Fatal("hub without pairing candidate capability was accepted")
	}
}
