package service

import (
	"errors"
	"sync"
	"testing"

	eebusapi "github.com/Project-Helianthus/helianthus-eebus-go/api"
	shipapi "github.com/Project-Helianthus/helianthus-ship-go/api"
)

const trustedRemoteRetryTestSKI = "0123456789abcdef0123456789abcdef01234567"

type trustedRemoteRetryHubSpy struct {
	shipapi.HubInterface
	mux             sync.Mutex
	err             error
	calls           []string
	shutdownEntered chan struct{}
	shutdownRelease chan struct{}
}

func (hub *trustedRemoteRetryHubSpy) RetryTrustedRemote(expectedSKI string) error {
	hub.mux.Lock()
	hub.calls = append(hub.calls, expectedSKI)
	hub.mux.Unlock()
	return hub.err
}

func (hub *trustedRemoteRetryHubSpy) Shutdown() {
	if hub.shutdownEntered != nil {
		close(hub.shutdownEntered)
	}
	if hub.shutdownRelease != nil {
		<-hub.shutdownRelease
	}
}

func (hub *trustedRemoteRetryHubSpy) snapshot() []string {
	hub.mux.Lock()
	defer hub.mux.Unlock()
	return append([]string(nil), hub.calls...)
}

func TestServiceExposesOptionalTrustedRemoteRetryController(t *testing.T) {
	var _ eebusapi.TrustedRemoteRetryController = (*Service)(nil)
}

func TestServiceForwardsTrustedRemoteRetryIdentityExactlyOnce(t *testing.T) {
	hub := &trustedRemoteRetryHubSpy{}
	service := &Service{connectionsHub: hub}

	if err := service.RetryTrustedRemote(trustedRemoteRetryTestSKI); err != nil {
		t.Fatalf("retry trusted remote: %v", err)
	}
	if calls := hub.snapshot(); len(calls) != 1 || calls[0] != trustedRemoteRetryTestSKI {
		t.Fatalf("retry calls = %v, want exact identity once", calls)
	}
}

func TestServiceTrustedRemoteRetryPropagatesSHIPError(t *testing.T) {
	wantErr := errors.New("retry rejected")
	hub := &trustedRemoteRetryHubSpy{err: wantErr}
	service := &Service{connectionsHub: hub}

	if err := service.RetryTrustedRemote(trustedRemoteRetryTestSKI); !errors.Is(err, wantErr) {
		t.Fatalf("retry error = %v, want %v", err, wantErr)
	}
	if calls := hub.snapshot(); len(calls) != 1 || calls[0] != trustedRemoteRetryTestSKI {
		t.Fatalf("failed retry calls = %v, want exact identity once", calls)
	}
}

func TestServiceTrustedRemoteRetryFailsClosedWithoutCapability(t *testing.T) {
	tests := []struct {
		name string
		hub  shipapi.HubInterface
	}{
		{name: "missing hub"},
		{name: "unsupported hub", hub: &hubRuntimeRecorder{}},
		{name: "typed nil retry hub", hub: (*trustedRemoteRetryHubSpy)(nil)},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			service := &Service{connectionsHub: test.hub}
			if err := service.RetryTrustedRemote(trustedRemoteRetryTestSKI); err == nil {
				t.Fatal("retry unexpectedly succeeded")
			}
		})
	}
}

func TestServiceTrustedRemoteRetryRejectsTerminalLifecycle(t *testing.T) {
	for _, lifecycle := range []lifecycleState{lifecycleStopping, lifecycleStopped, lifecycleTerminal} {
		t.Run(lifecycleName(lifecycle), func(t *testing.T) {
			hub := &trustedRemoteRetryHubSpy{}
			service := &Service{connectionsHub: hub, lifecycle: lifecycle}

			if err := service.RetryTrustedRemote(trustedRemoteRetryTestSKI); !errors.Is(err, errTrustedRemoteRetryServiceTerminal) {
				t.Fatalf("retry error = %v, want %v", err, errTrustedRemoteRetryServiceTerminal)
			}
			if calls := hub.snapshot(); len(calls) != 0 {
				t.Fatalf("terminal service forwarded retry: %v", calls)
			}
		})
	}
}

func TestServiceTrustedRemoteRetryRejectsShutdownInProgress(t *testing.T) {
	hub := &trustedRemoteRetryHubSpy{
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

	if err := service.RetryTrustedRemote(trustedRemoteRetryTestSKI); !errors.Is(err, errTrustedRemoteRetryServiceTerminal) {
		t.Fatalf("retry error = %v, want %v", err, errTrustedRemoteRetryServiceTerminal)
	}
	if calls := hub.snapshot(); len(calls) != 0 {
		t.Fatalf("stopping service forwarded retry: %v", calls)
	}

	close(hub.shutdownRelease)
	<-shutdownDone
}
