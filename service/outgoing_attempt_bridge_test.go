package service

import (
	"testing"
	"time"

	"github.com/enbility/eebus-go/api"
	shipapi "github.com/Project-Helianthus/helianthus-ship-go/api"
	"github.com/Project-Helianthus/helianthus-ship-go/cert"
	shipmodel "github.com/Project-Helianthus/helianthus-ship-go/model"
	spinemodel "github.com/enbility/spine-go/model"
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

func TestNewServiceLegacyPathRemainsSourceCompatible(t *testing.T) {
	handler := &legacyServiceReaderRecorder{}
	sut := legacyNewServiceConstructor(newOutgoingAttemptTestConfiguration(t), handler)

	require.NoError(t, sut.Setup())

	detail := &shipapi.ConnectionStateDetail{}
	sut.RemoteSKIConnected("legacy-ski")
	sut.ServicePairingDetailUpdate("legacy-ski", detail)

	assert.Same(t, sut, handler.connectedService)
	assert.Equal(t, "legacy-ski", handler.connectedSKI)
	assert.Equal(t, 1, handler.connectedCalls)
	assert.Same(t, detail, handler.pairingDetail)
	assert.Equal(t, 1, handler.pairingCalls)
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

			require.Error(t, sut.Setup())
			assert.Nil(t, sut.connectionsHub, "failed setup must not publish a startable hub")
		})
	}
}

func TestOutgoingAttemptBridgeSetupInstallsGateBeforeRuntime(t *testing.T) {
	sut := newOutgoingAttemptBridgeService(
		newOutgoingAttemptTestConfiguration(t),
		&legacyServiceReaderRecorder{},
		&outgoingAttemptGateStub{},
		&outgoingAttemptSinkRecorder{},
	)

	require.NoError(t, sut.Setup())
	require.NotNil(t, sut.connectionsHub)
	_, ok := sut.connectionsHub.(shipapi.OutgoingAttemptGateSetter)
	assert.True(t, ok, "configured hub must expose the reviewed gate setter")
}

func TestOutgoingAttemptBridgeForwardsExactCallbacksWithoutLegacyTranslation(t *testing.T) {
	legacy := &legacyServiceReaderRecorder{}
	sink := &outgoingAttemptSinkRecorder{}
	sut := newOutgoingAttemptBridgeService(nil, legacy, &outgoingAttemptGateStub{}, sink)
	metadata := shipapi.OutgoingAttemptMetadata{
		AttemptID:    "attempt-73",
		Scope:        "remote-scope",
		ControlEpoch: 41,
	}
	state := shipmodel.ShipState{State: shipmodel.SmeStateError}

	sut.OutgoingAttemptConnectionClosed("remote-ski", true, metadata)
	sut.OutgoingAttemptHandshakeStateUpdate("remote-ski", state, metadata)

	assert.Equal(t, []outgoingAttemptClosedEvent{{"remote-ski", true, metadata}}, sink.closed)
	assert.Equal(t, []outgoingAttemptHandshakeEvent{{"remote-ski", state, metadata}}, sink.handshake)
	assert.Zero(t, legacy.connectedCalls)
	assert.Zero(t, legacy.disconnected)
	assert.Zero(t, legacy.pairingCalls)
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

func newOutgoingAttemptTestConfiguration(t *testing.T) *api.Configuration {
	t.Helper()

	certificate, err := cert.CreateCertificate("unit", "org", "de", "cn")
	require.NoError(t, err)
	configuration, err := api.NewConfiguration(
		"vendor",
		"brand",
		"model",
		"serial",
		spinemodel.DeviceTypeTypeEnergyManagementSystem,
		[]spinemodel.EntityTypeType{spinemodel.EntityTypeTypeCEM},
		4729,
		certificate,
		4*time.Second,
	)
	require.NoError(t, err)
	return configuration
}
