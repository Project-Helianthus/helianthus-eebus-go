package service

import (
	"crypto/tls"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Project-Helianthus/helianthus-eebus-go/api"
	shipapi "github.com/Project-Helianthus/helianthus-ship-go/api"
	"github.com/Project-Helianthus/helianthus-ship-go/cert"
	shiphub "github.com/Project-Helianthus/helianthus-ship-go/hub"
	shipmodel "github.com/Project-Helianthus/helianthus-ship-go/model"
	spinemocks "github.com/Project-Helianthus/helianthus-spine-go/mocks"
	spinemodel "github.com/Project-Helianthus/helianthus-spine-go/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var legacyNewServiceConstructor func(*api.Configuration, api.ServiceReaderInterface) *Service = NewService
var additiveNewServiceConstructor func(
	*api.Configuration,
	api.ServiceReaderInterface,
	OutgoingAttemptBridgeConfiguration,
) *Service = NewServiceWithOutgoingAttemptBridge

type legacyServiceReaderRecorder struct {
	connectedService api.ServiceInterface
	connectedSKI     string
	connectedCalls   int
	disconnected     int
	pairingDetail    *shipapi.ConnectionStateDetail
	pairingCalls     int
}

func (r *legacyServiceReaderRecorder) RemoteSKIConnected(service api.ServiceInterface, ski string) {
	r.connectedService = service
	r.connectedSKI = ski
	r.connectedCalls++
}

func (r *legacyServiceReaderRecorder) RemoteSKIDisconnected(api.ServiceInterface, string) {
	r.disconnected++
}

func (*legacyServiceReaderRecorder) VisibleRemoteServicesUpdated(api.ServiceInterface, []shipapi.RemoteService) {
}

func (*legacyServiceReaderRecorder) ServiceShipIDUpdate(string, string) {}

func (r *legacyServiceReaderRecorder) ServicePairingDetailUpdate(_ string, detail *shipapi.ConnectionStateDetail) {
	r.pairingDetail = detail
	r.pairingCalls++
}

var _ api.ServiceReaderInterface = (*legacyServiceReaderRecorder)(nil)

type outgoingAttemptGateStub struct{}

func (*outgoingAttemptGateStub) Prepare(shipapi.OutgoingAttemptRequest) (shipapi.OutgoingAttemptHandle, error) {
	return nil, nil
}

func (*outgoingAttemptGateStub) AuthorizeLaunch(shipapi.OutgoingAttemptHandle) (shipapi.OutgoingAttemptPermit, error) {
	return shipapi.OutgoingAttemptPermit{}, nil
}

func (*outgoingAttemptGateStub) AbortPrepared(shipapi.OutgoingAttemptHandle) (shipapi.OutgoingAttemptAbortResult, error) {
	return shipapi.OutgoingAttemptAbortStaleNoOp, nil
}

var _ shipapi.OutgoingAttemptGate = (*outgoingAttemptGateStub)(nil)

type outgoingAttemptClosedEvent struct {
	remoteSKI string
	complete  bool
	metadata  shipapi.OutgoingAttemptMetadata
}

type outgoingAttemptHandshakeEvent struct {
	remoteSKI string
	state     shipmodel.ShipState
	metadata  shipapi.OutgoingAttemptMetadata
}

type outgoingAttemptSinkRecorder struct {
	closed    []outgoingAttemptClosedEvent
	handshake []outgoingAttemptHandshakeEvent
}

func (r *outgoingAttemptSinkRecorder) OutgoingAttemptConnectionClosed(
	remoteSKI string,
	complete bool,
	metadata shipapi.OutgoingAttemptMetadata,
) {
	r.closed = append(r.closed, outgoingAttemptClosedEvent{remoteSKI, complete, metadata})
}

func (r *outgoingAttemptSinkRecorder) OutgoingAttemptHandshakeStateUpdate(
	remoteSKI string,
	state shipmodel.ShipState,
	metadata shipapi.OutgoingAttemptMetadata,
) {
	r.handshake = append(r.handshake, outgoingAttemptHandshakeEvent{remoteSKI, state, metadata})
}

var _ shipapi.OutgoingAttemptHubReaderInterface = (*outgoingAttemptSinkRecorder)(nil)
var _ shipapi.OutgoingAttemptHubReaderInterface = (*Service)(nil)

type hubRuntimeRecorder struct {
	startCalls    int
	shutdownCalls int
	onStart       func()
	onShutdown    func()
}

func (h *hubRuntimeRecorder) Start() {
	h.startCalls++
	if h.onStart != nil {
		h.onStart()
	}
}

func (h *hubRuntimeRecorder) Shutdown() {
	h.shutdownCalls++
	if h.onShutdown != nil {
		h.onShutdown()
	}
}

func (*hubRuntimeRecorder) ServiceForSKI(string) *shipapi.ServiceDetails {
	return nil
}

func (*hubRuntimeRecorder) PairingDetailForSki(string) *shipapi.ConnectionStateDetail {
	return nil
}

func (*hubRuntimeRecorder) SetAutoAccept(bool)           {}
func (*hubRuntimeRecorder) RegisterRemoteSKI(string)     {}
func (*hubRuntimeRecorder) UnregisterRemoteSKI(string)   {}
func (*hubRuntimeRecorder) DisconnectSKI(string, string) {}
func (*hubRuntimeRecorder) CancelPairingWithSKI(string)  {}

var _ shipapi.HubInterface = (*hubRuntimeRecorder)(nil)

type outgoingAttemptGateAwareHub struct {
	hubRuntimeRecorder
	setGateCalls int
	gate         shipapi.OutgoingAttemptGate
	setGateErr   error
	onSetGate    func(shipapi.OutgoingAttemptGate)
}

func (h *outgoingAttemptGateAwareHub) SetOutgoingAttemptGate(gate shipapi.OutgoingAttemptGate) error {
	h.setGateCalls++
	h.gate = gate
	if h.onSetGate != nil {
		h.onSetGate(gate)
	}
	return h.setGateErr
}

var _ shipapi.HubInterface = (*outgoingAttemptGateAwareHub)(nil)
var _ shipapi.OutgoingAttemptGateSetter = (*outgoingAttemptGateAwareHub)(nil)

func TestNewServiceLegacyPathRemainsSourceAndRuntimeCompatible(t *testing.T) {
	handler := &legacyServiceReaderRecorder{}
	sut := legacyNewServiceConstructor(newOutgoingAttemptTestConfiguration(t), handler)
	hub := &outgoingAttemptGateAwareHub{}
	setTestConnectionsHubFactory(sut, func(
		shipapi.HubReaderInterface,
		shipapi.MdnsInterface,
		int,
		tls.Certificate,
		*shipapi.ServiceDetails,
	) shipapi.HubInterface {
		return hub
	})

	require.NoError(t, sut.Setup())
	assert.Zero(t, hub.setGateCalls, "legacy setup must not install an absent gate")

	detail := &shipapi.ConnectionStateDetail{}
	sut.RemoteSKIConnected("legacy-ski")
	sut.ServicePairingDetailUpdate("legacy-ski", detail)
	sut.Start()
	sut.Shutdown()

	assert.Same(t, sut, handler.connectedService)
	assert.Equal(t, "legacy-ski", handler.connectedSKI)
	assert.Equal(t, 1, handler.connectedCalls)
	assert.Same(t, detail, handler.pairingDetail)
	assert.Equal(t, 1, handler.pairingCalls)
	assert.Equal(t, 1, hub.startCalls)
	assert.Equal(t, 1, hub.shutdownCalls)
}

func TestOutgoingAttemptBridgeSetupRejectsNilAndTypedNilDependencies(t *testing.T) {
	validGate := &outgoingAttemptGateStub{}
	validSink := &outgoingAttemptSinkRecorder{}
	var typedNilGate *outgoingAttemptGateStub
	var typedNilSink *outgoingAttemptSinkRecorder

	tests := []struct {
		name string
		gate shipapi.OutgoingAttemptGate
		sink shipapi.OutgoingAttemptHubReaderInterface
	}{
		{name: "nil gate", sink: validSink},
		{name: "typed nil gate", gate: typedNilGate, sink: validSink},
		{name: "nil sink", gate: validGate},
		{name: "typed nil sink", gate: validGate, sink: typedNilSink},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			sut := newOutgoingAttemptBridgeService(
				newOutgoingAttemptTestConfiguration(t),
				&legacyServiceReaderRecorder{},
				test.gate,
				test.sink,
			)
			hub := &outgoingAttemptGateAwareHub{}
			factoryCalls := 0
			setTestConnectionsHubFactory(sut, func(
				shipapi.HubReaderInterface,
				shipapi.MdnsInterface,
				int,
				tls.Certificate,
				*shipapi.ServiceDetails,
			) shipapi.HubInterface {
				factoryCalls++
				return hub
			})

			var setupErr error
			require.NotPanics(t, func() { setupErr = sut.Setup() })
			require.Error(t, setupErr)
			assertBridgeRuntimeUnpublished(t, sut)
			assert.Zero(t, factoryCalls, "invalid bridge dependencies must fail before hub construction")
			assertRuntimeMethodsAreSafeAfterFailedSetup(t, sut)
			assert.Zero(t, hub.startCalls)
			assert.Zero(t, hub.shutdownCalls)
		})
	}
}

func TestOutgoingAttemptBridgeSetupRejectsUninstallableHubsTransactionally(t *testing.T) {
	setterFailure := errors.New("gate installation failed")

	tests := []struct {
		name   string
		newHub func() shipapi.HubInterface
	}{
		{
			name:   "nil hub",
			newHub: func() shipapi.HubInterface { return nil },
		},
		{
			name: "typed nil hub",
			newHub: func() shipapi.HubInterface {
				var typedNil *outgoingAttemptGateAwareHub
				return typedNil
			},
		},
		{
			name: "missing gate setter",
			newHub: func() shipapi.HubInterface {
				return &hubRuntimeRecorder{}
			},
		},
		{
			name: "gate setter failure",
			newHub: func() shipapi.HubInterface {
				return &outgoingAttemptGateAwareHub{setGateErr: setterFailure}
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			sut := newOutgoingAttemptBridgeService(
				newOutgoingAttemptTestConfiguration(t),
				&legacyServiceReaderRecorder{},
				&outgoingAttemptGateStub{},
				&outgoingAttemptSinkRecorder{},
			)
			factoryCalls := 0
			var candidate shipapi.HubInterface
			setTestConnectionsHubFactory(sut, func(
				shipapi.HubReaderInterface,
				shipapi.MdnsInterface,
				int,
				tls.Certificate,
				*shipapi.ServiceDetails,
			) shipapi.HubInterface {
				factoryCalls++
				candidate = test.newHub()
				return candidate
			})

			var setupErr error
			require.NotPanics(t, func() { setupErr = sut.Setup() })
			require.Error(t, setupErr)
			assert.Equal(t, 1, factoryCalls)
			assertBridgeRuntimeUnpublished(t, sut)
			assertRuntimeMethodsAreSafeAfterFailedSetup(t, sut)
			if runtimeHub := runtimeRecorderFor(candidate); runtimeHub != nil {
				assert.Zero(t, runtimeHub.startCalls)
				assert.Zero(t, runtimeHub.shutdownCalls)
			}
		})
	}
}

func TestOutgoingAttemptBridgeSetupInstallsExactGateBeforePublication(t *testing.T) {
	gate := &outgoingAttemptGateStub{}
	sink := &outgoingAttemptSinkRecorder{}
	sut := newOutgoingAttemptBridgeService(
		newOutgoingAttemptTestConfiguration(t),
		&legacyServiceReaderRecorder{},
		gate,
		sink,
	)
	hub := &outgoingAttemptGateAwareHub{}
	var hubReader shipapi.HubReaderInterface
	hub.onSetGate = func(installed shipapi.OutgoingAttemptGate) {
		assert.Same(t, gate, installed)
		assertBridgeRuntimeUnpublished(t, sut)
		assert.Zero(t, hub.startCalls)
	}
	setTestConnectionsHubFactory(sut, func(
		reader shipapi.HubReaderInterface,
		_ shipapi.MdnsInterface,
		_ int,
		_ tls.Certificate,
		_ *shipapi.ServiceDetails,
	) shipapi.HubInterface {
		hubReader = reader
		return hub
	})

	require.NoError(t, sut.Setup())
	require.NotNil(t, hubReader)
	assert.Same(t, sut, hubReader)
	assert.Equal(t, 1, hub.setGateCalls)
	assert.Same(t, gate, hub.gate)
	assert.Same(t, hub, sut.connectionsHub)
	assert.NotNil(t, sut.localService)
	assert.NotNil(t, sut.spineLocalDevice)
	assert.Zero(t, hub.startCalls, "Setup must not start listeners or runtime activity")
}

func TestOutgoingAttemptBridgeDefaultFactoryPublishesProductionHub(t *testing.T) {
	sut := newOutgoingAttemptBridgeService(
		newOutgoingAttemptTestConfiguration(t),
		&legacyServiceReaderRecorder{},
		&outgoingAttemptGateStub{},
		&outgoingAttemptSinkRecorder{},
	)

	require.NoError(t, sut.Setup())
	require.IsType(t, &shiphub.Hub{}, sut.connectionsHub)
}

func TestOutgoingAttemptBridgeSetupFailureDoesNotPublishLocalCandidates(t *testing.T) {
	sut := newOutgoingAttemptBridgeService(
		newOutgoingAttemptOversizedConfiguration(t),
		&legacyServiceReaderRecorder{},
		&outgoingAttemptGateStub{},
		&outgoingAttemptSinkRecorder{},
	)
	factoryCalls := 0
	setTestConnectionsHubFactory(sut, func(
		shipapi.HubReaderInterface,
		shipapi.MdnsInterface,
		int,
		tls.Certificate,
		*shipapi.ServiceDetails,
	) shipapi.HubInterface {
		factoryCalls++
		return &outgoingAttemptGateAwareHub{}
	})

	require.Error(t, sut.Setup())
	assert.Zero(t, factoryCalls)
	assertBridgeRuntimeUnpublished(t, sut)
	assertRuntimeMethodsAreSafeAfterFailedSetup(t, sut)
}

func TestOutgoingAttemptBridgeSetupRetryIsDeterministicAndLifecycleIsIdempotent(t *testing.T) {
	sut := newOutgoingAttemptBridgeService(
		newOutgoingAttemptTestConfiguration(t),
		&legacyServiceReaderRecorder{},
		&outgoingAttemptGateStub{},
		&outgoingAttemptSinkRecorder{},
	)
	failedHub := &outgoingAttemptGateAwareHub{setGateErr: errors.New("first install fails")}
	successfulHub := &outgoingAttemptGateAwareHub{}
	factoryCalls := 0
	setTestConnectionsHubFactory(sut, func(
		shipapi.HubReaderInterface,
		shipapi.MdnsInterface,
		int,
		tls.Certificate,
		*shipapi.ServiceDetails,
	) shipapi.HubInterface {
		factoryCalls++
		if factoryCalls == 1 {
			return failedHub
		}
		return successfulHub
	})

	require.NotPanics(t, sut.Start)
	require.NotPanics(t, sut.Shutdown)
	require.Error(t, sut.Setup())
	assertBridgeRuntimeUnpublished(t, sut)
	assertRuntimeMethodsAreSafeAfterFailedSetup(t, sut)
	assert.Zero(t, failedHub.startCalls)
	assert.Zero(t, failedHub.shutdownCalls)

	require.NoError(t, sut.Setup(), "Setup should preserve the existing retry-after-failure lifecycle")
	assert.Same(t, successfulHub, sut.connectionsHub)
	require.NotPanics(t, func() {
		sut.Start()
		sut.Start()
		sut.Shutdown()
		sut.Shutdown()
	})
	assert.Equal(t, 1, successfulHub.startCalls)
	assert.Equal(t, 1, successfulHub.shutdownCalls)
	assert.Equal(t, 2, factoryCalls)
}

func TestServiceLifecycleShutdownBeforeStartIsTerminal(t *testing.T) {
	sut := legacyNewServiceConstructor(newOutgoingAttemptTestConfiguration(t), &legacyServiceReaderRecorder{})
	hub := &hubRuntimeRecorder{}
	setTestConnectionsHubFactory(sut, func(
		shipapi.HubReaderInterface,
		shipapi.MdnsInterface,
		int,
		tls.Certificate,
		*shipapi.ServiceDetails,
	) shipapi.HubInterface {
		return hub
	})

	require.NoError(t, sut.Setup())
	sut.Shutdown()
	sut.Start()
	require.NoError(t, sut.Setup(), "repeated setup must not revive a stopped service")
	sut.Start()
	sut.Shutdown()

	assert.Zero(t, hub.startCalls)
	assert.Equal(t, 1, hub.shutdownCalls)
}

func TestServiceLifecycleConcurrentStartShutdownIsBounded(t *testing.T) {
	for iteration := 0; iteration < 100; iteration++ {
		sut := legacyNewServiceConstructor(newOutgoingAttemptTestConfiguration(t), &legacyServiceReaderRecorder{})
		hub := &hubRuntimeRecorder{}
		setTestConnectionsHubFactory(sut, func(
			shipapi.HubReaderInterface,
			shipapi.MdnsInterface,
			int,
			tls.Certificate,
			*shipapi.ServiceDetails,
		) shipapi.HubInterface {
			return hub
		})
		require.NoError(t, sut.Setup())

		start := make(chan struct{})
		var wait sync.WaitGroup
		for call := 0; call < 32; call++ {
			wait.Add(1)
			go func(startCall bool) {
				defer wait.Done()
				<-start
				if startCall {
					sut.Start()
					return
				}
				sut.Shutdown()
			}(call%2 == 0)
		}
		close(start)
		wait.Wait()
		sut.Shutdown()
		sut.Start()

		assert.LessOrEqual(t, hub.startCalls, 1, "iteration %d", iteration)
		assert.Equal(t, 1, hub.shutdownCalls, "iteration %d", iteration)
	}
}

func TestServiceLifecycleReentrantHubCallbacksAreTerminalAndNonBlocking(t *testing.T) {
	sut := legacyNewServiceConstructor(newOutgoingAttemptTestConfiguration(t), &legacyServiceReaderRecorder{})
	hub := &hubRuntimeRecorder{}
	hub.onStart = sut.Shutdown
	hub.onShutdown = sut.Start
	setTestConnectionsHubFactory(sut, func(
		shipapi.HubReaderInterface,
		shipapi.MdnsInterface,
		int,
		tls.Certificate,
		*shipapi.ServiceDetails,
	) shipapi.HubInterface {
		return hub
	})
	require.NoError(t, sut.Setup())

	done := make(chan struct{})
	go func() {
		sut.Start()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("re-entrant Start/Shutdown callbacks deadlocked")
	}
	sut.Start()
	sut.Shutdown()

	assert.Equal(t, 1, hub.startCalls)
	assert.Equal(t, 1, hub.shutdownCalls)
}

func TestOutgoingAttemptBridgeProductionHubReaderRouteForwardsExactCallbacks(t *testing.T) {
	legacy := &legacyServiceReaderRecorder{}
	sink := &outgoingAttemptSinkRecorder{}
	gate := &outgoingAttemptGateStub{}
	sut := newOutgoingAttemptBridgeService(
		newOutgoingAttemptTestConfiguration(t),
		legacy,
		gate,
		sink,
	)
	hub := &outgoingAttemptGateAwareHub{}
	var productionReader shipapi.HubReaderInterface
	setTestConnectionsHubFactory(sut, func(
		reader shipapi.HubReaderInterface,
		_ shipapi.MdnsInterface,
		_ int,
		_ tls.Certificate,
		_ *shipapi.ServiceDetails,
	) shipapi.HubInterface {
		productionReader = reader
		return hub
	})

	require.NoError(t, sut.Setup())
	assert.Same(t, gate, hub.gate)
	assert.Same(t, sut, productionReader)
	emitter, ok := productionReader.(shipapi.OutgoingAttemptHubReaderInterface)
	require.True(t, ok, "the exact reader passed to ship.NewHub must expose attempt callbacks")
	metadata := shipapi.OutgoingAttemptMetadata{
		AttemptID:    "attempt-73\x00opaque",
		Scope:        "remote-scope/value",
		ControlEpoch: ^uint64(0) - 41,
	}
	secondMetadata := shipapi.OutgoingAttemptMetadata{
		AttemptID:    "attempt-74/distinct",
		Scope:        "second-scope\x00opaque",
		ControlEpoch: 0,
	}
	state := shipmodel.ShipState{State: shipmodel.SmeStateError}
	remoteSKI := "remote-ski\x00exact"

	emitter.OutgoingAttemptConnectionClosed(remoteSKI, false, metadata)
	emitter.OutgoingAttemptConnectionClosed("second-remote-ski", true, secondMetadata)
	emitter.OutgoingAttemptHandshakeStateUpdate(remoteSKI, state, metadata)

	assert.Equal(t, []outgoingAttemptClosedEvent{
		{remoteSKI, false, metadata},
		{"second-remote-ski", true, secondMetadata},
	}, sink.closed)
	assert.Equal(t, []outgoingAttemptHandshakeEvent{{remoteSKI, state, metadata}}, sink.handshake)
	assert.Zero(t, legacy.connectedCalls)
	assert.Zero(t, legacy.disconnected)
	assert.Zero(t, legacy.pairingCalls)

	localDevice := spinemocks.NewDeviceLocalInterface(t)
	localDevice.EXPECT().RemoveRemoteDeviceConnection("legacy-disconnect").Return().Once()
	sut.spineLocalDevice = localDevice
	productionReader.RemoteSKIDisconnected("legacy-disconnect")
	assert.Equal(t, 1, legacy.disconnected, "the independent legacy path must still notify its handler")
}

func newOutgoingAttemptBridgeService(
	configuration *api.Configuration,
	handler api.ServiceReaderInterface,
	gate shipapi.OutgoingAttemptGate,
	sink shipapi.OutgoingAttemptHubReaderInterface,
) *Service {
	return additiveNewServiceConstructor(
		configuration,
		handler,
		OutgoingAttemptBridgeConfiguration{Gate: gate, Sink: sink},
	)
}

func setTestConnectionsHubFactory(sut *Service, factory func(
	shipapi.HubReaderInterface,
	shipapi.MdnsInterface,
	int,
	tls.Certificate,
	*shipapi.ServiceDetails,
) shipapi.HubInterface) {
	sut.connectionsHubFactory = factory
}

func assertBridgeRuntimeUnpublished(t *testing.T, sut *Service) {
	t.Helper()
	assert.Nil(t, sut.connectionsHub, "failed setup must not publish a startable hub")
	assert.Nil(t, sut.localService, "failed setup must not publish local SHIP state")
	assert.Nil(t, sut.spineLocalDevice, "failed setup must not publish local SPINE state")
}

func assertRuntimeMethodsAreSafeAfterFailedSetup(t *testing.T, sut *Service) {
	t.Helper()
	require.NotPanics(t, func() {
		sut.Start()
		sut.Start()
		sut.Shutdown()
		sut.Shutdown()
	})
}

func runtimeRecorderFor(candidate shipapi.HubInterface) *hubRuntimeRecorder {
	switch hub := candidate.(type) {
	case *hubRuntimeRecorder:
		return hub
	case *outgoingAttemptGateAwareHub:
		if hub == nil {
			return nil
		}
		return &hub.hubRuntimeRecorder
	default:
		return nil
	}
}

func newOutgoingAttemptTestConfiguration(t *testing.T) *api.Configuration {
	t.Helper()
	return newOutgoingAttemptConfiguration(t, "vendor", "brand", "model", "serial")
}

func newOutgoingAttemptOversizedConfiguration(t *testing.T) *api.Configuration {
	t.Helper()
	return newOutgoingAttemptConfiguration(
		t,
		"vendor",
		"brand",
		strings.Repeat("model", 50),
		strings.Repeat("serial", 20),
	)
}

func newOutgoingAttemptConfiguration(
	t *testing.T,
	vendor string,
	brand string,
	model string,
	serial string,
) *api.Configuration {
	t.Helper()
	certificate, err := cert.CreateCertificate("unit", "org", "de", "cn")
	require.NoError(t, err)
	configuration, err := api.NewConfiguration(
		vendor,
		brand,
		model,
		serial,
		spinemodel.DeviceTypeTypeEnergyManagementSystem,
		[]spinemodel.EntityTypeType{spinemodel.EntityTypeTypeCEM},
		4729,
		certificate,
		4*time.Second,
	)
	require.NoError(t, err)
	return configuration
}
