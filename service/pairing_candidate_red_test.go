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

type pairingCandidateControllerHubSpy struct {
	shipapi.HubInterface
	mux             sync.Mutex
	reservation     shipapi.PairingCandidateReservation
	selectErr       error
	connectErr      error
	selectCalls     []pairingCandidateCall
	connectCalls    []shipapi.PairingCandidateReservation
	shutdownEntered chan struct{}
	shutdownRelease chan struct{}
}

func (hub *pairingCandidateControllerHubSpy) SelectPairingCandidate(
	candidateRef string,
	expectedSKI string,
) (shipapi.PairingCandidateReservation, error) {
	hub.mux.Lock()
	defer hub.mux.Unlock()
	hub.selectCalls = append(hub.selectCalls, pairingCandidateCall{ref: candidateRef, ski: expectedSKI})
	return hub.reservation, hub.selectErr
}

func (hub *pairingCandidateControllerHubSpy) ConnectPairingCandidate(
	reservation shipapi.PairingCandidateReservation,
) error {
	hub.mux.Lock()
	defer hub.mux.Unlock()
	hub.connectCalls = append(hub.connectCalls, reservation)
	return hub.connectErr
}

func (hub *pairingCandidateControllerHubSpy) Shutdown() {
	if hub.shutdownEntered != nil {
		close(hub.shutdownEntered)
	}
	if hub.shutdownRelease != nil {
		<-hub.shutdownRelease
	}
}

func (hub *pairingCandidateControllerHubSpy) snapshots() (
	[]pairingCandidateCall,
	[]shipapi.PairingCandidateReservation,
) {
	hub.mux.Lock()
	defer hub.mux.Unlock()
	return append([]pairingCandidateCall(nil), hub.selectCalls...),
		append([]shipapi.PairingCandidateReservation(nil), hub.connectCalls...)
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

func TestServiceExposesOptionalSplitPairingCandidateController(t *testing.T) {
	var _ eebusapi.PairingCandidateController = (*Service)(nil)
}

func TestServiceForwardsSplitPairingCandidateControlExactly(t *testing.T) {
	var token [32]byte
	token[0] = 0x5a
	wantReservation := shipapi.NewPairingCandidateReservation(token)
	hub := &pairingCandidateControllerHubSpy{reservation: wantReservation}
	service := &Service{connectionsHub: hub}

	gotReservation, err := service.SelectPairingCandidate(pairingCandidateFacadeTestRef, pairingCandidateFacadeTestSKI)
	if err != nil {
		t.Fatalf("select pairing candidate: %v", err)
	}
	if !gotReservation.Matches(wantReservation) {
		t.Fatal("select translated or replaced the opaque SHIP reservation")
	}
	if err := service.ConnectPairingCandidate(gotReservation); err != nil {
		t.Fatalf("connect pairing candidate: %v", err)
	}
	selectCalls, connectCalls := hub.snapshots()
	wantSelect := pairingCandidateCall{ref: pairingCandidateFacadeTestRef, ski: pairingCandidateFacadeTestSKI}
	if len(selectCalls) != 1 || selectCalls[0] != wantSelect {
		t.Fatalf("select calls = %v, want [%+v]", selectCalls, wantSelect)
	}
	if len(connectCalls) != 1 || !connectCalls[0].Matches(wantReservation) {
		t.Fatalf("connect calls = %v, want exact opaque reservation", connectCalls)
	}
}

func TestServiceSplitPairingCandidateControlFailsClosed(t *testing.T) {
	var token [32]byte
	token[0] = 0x7b
	reservation := shipapi.NewPairingCandidateReservation(token)
	wantErr := errors.New("candidate rejected")

	t.Run("propagates select failure", func(t *testing.T) {
		hub := &pairingCandidateControllerHubSpy{selectErr: wantErr}
		service := &Service{connectionsHub: hub}
		if _, err := service.SelectPairingCandidate(pairingCandidateFacadeTestRef, pairingCandidateFacadeTestSKI); !errors.Is(err, wantErr) {
			t.Fatalf("select error = %v, want %v", err, wantErr)
		}
	})
	t.Run("propagates connect failure", func(t *testing.T) {
		hub := &pairingCandidateControllerHubSpy{connectErr: wantErr}
		service := &Service{connectionsHub: hub}
		if err := service.ConnectPairingCandidate(reservation); !errors.Is(err, wantErr) {
			t.Fatalf("connect error = %v, want %v", err, wantErr)
		}
	})
	t.Run("rejects unsupported hub", func(t *testing.T) {
		service := &Service{connectionsHub: &hubRuntimeRecorder{}}
		if _, err := service.SelectPairingCandidate(pairingCandidateFacadeTestRef, pairingCandidateFacadeTestSKI); err == nil {
			t.Fatal("select accepted hub without split candidate capability")
		}
		if err := service.ConnectPairingCandidate(reservation); err == nil {
			t.Fatal("connect accepted hub without split candidate capability")
		}
	})
	for _, lifecycle := range []lifecycleState{lifecycleStopping, lifecycleStopped, lifecycleTerminal} {
		t.Run("terminal "+lifecycleName(lifecycle), func(t *testing.T) {
			hub := &pairingCandidateControllerHubSpy{}
			service := &Service{connectionsHub: hub, lifecycle: lifecycle}
			if _, err := service.SelectPairingCandidate(pairingCandidateFacadeTestRef, pairingCandidateFacadeTestSKI); !errors.Is(err, errPairingCandidateServiceTerminal) {
				t.Fatalf("select error = %v, want %v", err, errPairingCandidateServiceTerminal)
			}
			if err := service.ConnectPairingCandidate(reservation); !errors.Is(err, errPairingCandidateServiceTerminal) {
				t.Fatalf("connect error = %v, want %v", err, errPairingCandidateServiceTerminal)
			}
			selectCalls, connectCalls := hub.snapshots()
			if len(selectCalls) != 0 || len(connectCalls) != 0 {
				t.Fatalf("terminal service forwarded select=%v connect=%v", selectCalls, connectCalls)
			}
		})
	}
}

func TestServiceRejectsSplitPairingCandidateControlWhileShutdownIsInProgress(t *testing.T) {
	var token [32]byte
	token[0] = 0x4c
	reservation := shipapi.NewPairingCandidateReservation(token)
	hub := &pairingCandidateControllerHubSpy{
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

	if _, err := service.SelectPairingCandidate(pairingCandidateFacadeTestRef, pairingCandidateFacadeTestSKI); !errors.Is(err, errPairingCandidateServiceTerminal) {
		t.Fatalf("select error = %v, want %v", err, errPairingCandidateServiceTerminal)
	}
	if err := service.ConnectPairingCandidate(reservation); !errors.Is(err, errPairingCandidateServiceTerminal) {
		t.Fatalf("connect error = %v, want %v", err, errPairingCandidateServiceTerminal)
	}
	selectCalls, connectCalls := hub.snapshots()
	if len(selectCalls) != 0 || len(connectCalls) != 0 {
		t.Fatalf("stopping service forwarded select=%v connect=%v", selectCalls, connectCalls)
	}

	close(hub.shutdownRelease)
	<-shutdownDone
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
