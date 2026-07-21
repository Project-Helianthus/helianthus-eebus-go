package service

import (
	"errors"
	"testing"

	eebusapi "github.com/Project-Helianthus/helianthus-eebus-go/api"
	shipapi "github.com/Project-Helianthus/helianthus-ship-go/api"
)

const pairingCandidateFacadeTestSKI = "b1b7197b064084e4cfef2365105d8d36ff185e5b"

type pairingCandidateHubSpy struct {
	shipapi.HubInterface
	err   error
	calls []string
}

func (hub *pairingCandidateHubSpy) QueuePairingCandidate(ski string) error {
	hub.calls = append(hub.calls, ski)
	return hub.err
}

func TestServiceExposesOptionalPairingCandidateQueue(t *testing.T) {
	var _ eebusapi.PairingCandidateQueuer = (*Service)(nil)
}

func TestServiceForwardsPairingCandidateSKIExactlyOnce(t *testing.T) {
	hub := &pairingCandidateHubSpy{}
	service := &Service{connectionsHub: hub}

	if err := service.QueuePairingCandidate(pairingCandidateFacadeTestSKI); err != nil {
		t.Fatalf("queue pairing candidate: %v", err)
	}
	if len(hub.calls) != 1 || hub.calls[0] != pairingCandidateFacadeTestSKI {
		t.Fatalf("pairing candidate calls = %v, want [%s]", hub.calls, pairingCandidateFacadeTestSKI)
	}
}

func TestServicePropagatesPairingCandidateFailure(t *testing.T) {
	wantErr := errors.New("candidate rejected")
	hub := &pairingCandidateHubSpy{err: wantErr}
	service := &Service{connectionsHub: hub}

	if err := service.QueuePairingCandidate(pairingCandidateFacadeTestSKI); !errors.Is(err, wantErr) {
		t.Fatalf("queue error = %v, want %v", err, wantErr)
	}
}

func TestServiceRejectsHubWithoutPairingCandidateCapability(t *testing.T) {
	service := &Service{connectionsHub: &hubRuntimeRecorder{}}
	if err := service.QueuePairingCandidate(pairingCandidateFacadeTestSKI); err == nil {
		t.Fatal("hub without pairing candidate capability was accepted")
	}
}
