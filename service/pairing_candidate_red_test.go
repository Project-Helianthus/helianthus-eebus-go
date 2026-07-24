package service

import (
	"errors"
	"sync"
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
	mux             sync.Mutex
	err             error
	calls           []pairingCandidateCall
	shutdownEntered chan struct{}
	shutdownRelease chan struct{}
}

func (hub *pairingCandidateHubSpy) QueuePairingCandidate(candidateRef, expectedSKI string) error {
	hub.mux.Lock()
	defer hub.mux.Unlock()
	hub.calls = append(hub.calls, pairingCandidateCall{ref: candidateRef, ski: expectedSKI})
	return hub.err
}

func (hub *pairingCandidateHubSpy) Shutdown() {
	if hub.shutdownEntered != nil {
		close(hub.shutdownEntered)
	}
	if hub.shutdownRelease != nil {
		<-hub.shutdownRelease
	}
}

func (hub *pairingCandidateHubSpy) pairingCandidateCalls() []pairingCandidateCall {
	hub.mux.Lock()
	defer hub.mux.Unlock()
	return append([]pairingCandidateCall(nil), hub.calls...)
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
	calls := hub.pairingCandidateCalls()
	if len(calls) != 1 || calls[0] != want {
		t.Fatalf("pairing candidate calls = %v, want [%+v]", calls, want)
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

func TestServiceRejectsPairingCandidateAfterTerminalLifecycle(t *testing.T) {
	for _, lifecycle := range []lifecycleState{lifecycleStopping, lifecycleStopped, lifecycleTerminal} {
		t.Run(lifecycleName(lifecycle), func(t *testing.T) {
			hub := &pairingCandidateHubSpy{}
			service := &Service{connectionsHub: hub, lifecycle: lifecycle}

			err := service.QueuePairingCandidate(pairingCandidateFacadeTestRef, pairingCandidateFacadeTestSKI)
			if !errors.Is(err, errPairingCandidateServiceTerminal) {
				t.Fatalf("queue error = %v, want %v", err, errPairingCandidateServiceTerminal)
			}
			if calls := hub.pairingCandidateCalls(); len(calls) != 0 {
				t.Fatalf("terminal service forwarded pairing candidate: %v", calls)
			}
		})
	}
}

func TestServiceRejectsPairingCandidateWhileShutdownIsInProgress(t *testing.T) {
	hub := &pairingCandidateHubSpy{
		shutdownEntered: make(chan struct{}),
		shutdownRelease: make(chan struct{}),
	}
	service := &Service{connectionsHub: hub, lifecycle: lifecycleRunning}
	shutdownDone := make(chan struct{})
	go func() {
		service.Shutdown()
		close(shutdownDone)
	}()
	<-hub.shutdownEntered

	err := service.QueuePairingCandidate(pairingCandidateFacadeTestRef, pairingCandidateFacadeTestSKI)
	if !errors.Is(err, errPairingCandidateServiceTerminal) {
		t.Fatalf("queue error = %v, want %v", err, errPairingCandidateServiceTerminal)
	}
	if calls := hub.pairingCandidateCalls(); len(calls) != 0 {
		t.Fatalf("stopping service forwarded pairing candidate: %v", calls)
	}

	close(hub.shutdownRelease)
	<-shutdownDone
}

func lifecycleName(state lifecycleState) string {
	switch state {
	case lifecycleStopping:
		return "stopping"
	case lifecycleStopped:
		return "stopped"
	case lifecycleTerminal:
		return "terminal"
	default:
		return "unexpected"
	}
}
