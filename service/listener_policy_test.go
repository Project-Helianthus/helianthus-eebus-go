package service

import (
	"net"
	"net/netip"
	"reflect"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/Project-Helianthus/helianthus-eebus-go/api"
	shipapi "github.com/Project-Helianthus/helianthus-ship-go/api"
	"github.com/Project-Helianthus/helianthus-ship-go/cert"
	spinemodel "github.com/Project-Helianthus/helianthus-spine-go/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestListenerPolicyHasExactDependencyFreeShape(t *testing.T) {
	policyType := reflect.TypeOf(ListenerPolicy{})
	want := map[string]reflect.Type{
		"ListenAddress":    reflect.TypeOf(netip.AddrPort{}),
		"DiscoveryEnabled": reflect.TypeOf(false),
	}

	require.Equal(t, len(want), policyType.NumField(), "ListenerPolicy must remain a minimal value product")
	for name, wantType := range want {
		field, ok := policyType.FieldByName(name)
		require.True(t, ok, "ListenerPolicy is missing %s", name)
		assert.Equal(t, wantType, field.Type, "ListenerPolicy.%s leaks or changes its value type", name)
		assert.Empty(t, field.PkgPath, "ListenerPolicy.%s must be exported", name)
	}
}

func TestServiceOptionsComposeListenerPolicyAndOutgoingAttemptBridge(t *testing.T) {
	optionsType := reflect.TypeOf(ServiceOptions{})
	policyField, ok := optionsType.FieldByName("ListenerPolicy")
	require.True(t, ok, "ServiceOptions must carry the optional listener policy")
	assert.Equal(t, reflect.PointerTo(reflect.TypeOf(ListenerPolicy{})), policyField.Type)
	bridgeField, ok := optionsType.FieldByName("OutgoingAttemptBridge")
	require.True(t, ok, "ServiceOptions must carry the existing outgoing-attempt bridge")
	assert.Equal(t, reflect.PointerTo(reflect.TypeOf(OutgoingAttemptBridgeConfiguration{})), bridgeField.Type)

	endpoint := listenerPolicyAvailableEndpoint(t, netip.MustParseAddr("127.0.0.1"))
	configuration := newListenerPolicyConfiguration(t, endpoint.Port())
	configuration.SetInterfaces([]string{listenerPolicyMissingInterface})
	gate := &outgoingAttemptGateStub{}
	sink := &outgoingAttemptSinkRecorder{}
	policy := ListenerPolicy{ListenAddress: endpoint, DiscoveryEnabled: false}
	bridge := OutgoingAttemptBridgeConfiguration{Gate: gate, Sink: sink}

	sut := NewServiceWithOptions(configuration, &legacyServiceReaderRecorder{}, ServiceOptions{
		ListenerPolicy:        &policy,
		OutgoingAttemptBridge: &bridge,
	})
	require.NoError(t, sut.Setup())
	t.Cleanup(sut.Shutdown)
	require.True(t, sut.bridgeEnabled)
	assert.Same(t, gate, sut.outgoingAttemptGate)
	assert.Same(t, sink, sut.outgoingAttemptSink)

	require.NoError(t, startScopedService(t, sut))
	listenerPolicyDial(t, endpoint)
	metadata := shipapi.OutgoingAttemptMetadata{AttemptID: "listener-policy-composition"}
	sut.OutgoingAttemptConnectionClosed("remote-ski", false, metadata)
	require.Equal(t, []outgoingAttemptClosedEvent{{"remote-ski", false, metadata}}, sink.closed)

	listenerPolicyConcurrentShutdown(t, sut, 8)
	listenerPolicyRequireRebind(t, endpoint)
}

func TestScopedStartBindsExactLoopbackAndReleasesIdempotently(t *testing.T) {
	t.Run("IPv4", func(t *testing.T) {
		endpoint := listenerPolicyAvailableEndpoint(t, netip.MustParseAddr("127.0.0.1"))
		sut := newListenerPolicyService(t, endpoint, false)
		require.NoError(t, sut.Setup())
		t.Cleanup(sut.Shutdown)
		require.NoError(t, startScopedService(t, sut))

		listenerPolicyDial(t, endpoint)
		alternate := netip.AddrPortFrom(netip.MustParseAddr("127.0.0.2"), endpoint.Port())
		listenerPolicyRequireUnreachable(t, alternate)
		listenerPolicyConcurrentShutdown(t, sut, 8)
		listenerPolicyRequireRebind(t, endpoint)
	})

	t.Run("IPv6", func(t *testing.T) {
		endpoint, ok := listenerPolicyAvailableIPv6Endpoint(t)
		if !ok {
			return
		}
		sut := newListenerPolicyService(t, endpoint, false)
		require.NoError(t, sut.Setup())
		t.Cleanup(sut.Shutdown)
		require.NoError(t, startScopedService(t, sut))

		listenerPolicyDial(t, endpoint)
		listenerPolicyConcurrentShutdown(t, sut, 8)
		listenerPolicyRequireRebind(t, endpoint)
	})
}

func TestDiscoveryPolicyIsIndependentFromPairingAndListenerStartup(t *testing.T) {
	tests := []struct {
		name      string
		discovery bool
		pairing   bool
	}{
		{name: "disabled pairing closed", discovery: false, pairing: false},
		{name: "disabled pairing possible", discovery: false, pairing: true},
		{name: "enabled pairing closed", discovery: true, pairing: false},
		{name: "enabled pairing possible", discovery: true, pairing: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			endpoint := listenerPolicyAvailableEndpoint(t, netip.MustParseAddr("127.0.0.1"))
			configuration := newListenerPolicyConfiguration(t, endpoint.Port())
			configuration.SetInterfaces([]string{listenerPolicyMissingInterface})
			policy := ListenerPolicy{ListenAddress: endpoint, DiscoveryEnabled: test.discovery}
			sut := NewServiceWithOptions(configuration, &legacyServiceReaderRecorder{}, ServiceOptions{
				ListenerPolicy: &policy,
			})
			sut.UserIsAbleToApproveOrCancelPairingRequests(test.pairing)
			require.NoError(t, sut.Setup())
			t.Cleanup(sut.Shutdown)

			err := startScopedService(t, sut)
			if !test.discovery {
				require.NoError(t, err, "disabled discovery must not inspect or start mDNS")
				listenerPolicyDial(t, endpoint)
				listenerPolicyConcurrentShutdown(t, sut, 2)
				listenerPolicyRequireRebind(t, endpoint)
				return
			}

			require.Error(t, err)
			assert.ErrorContains(t, err, listenerPolicyMissingInterface)
			retryErr := startScopedService(t, sut)
			require.Error(t, retryErr, "an initial discovery failure must terminalize scoped startup")
			assert.Contains(t, strings.ToLower(retryErr.Error()), "terminal")
			sut.Start()
			listenerPolicyConcurrentShutdown(t, sut, 2)
			listenerPolicyRequireRebind(t, endpoint)
		})
	}
}

func TestScopedBindFailurePropagatesAndTerminalizesRetries(t *testing.T) {
	held := listenerPolicyListen(t, netip.AddrPortFrom(netip.MustParseAddr("127.0.0.1"), 0))
	endpoint := held.Addr().(*net.TCPAddr).AddrPort()
	sut := newListenerPolicyService(t, endpoint, false)
	require.NoError(t, sut.Setup())
	t.Cleanup(sut.Shutdown)

	err := startScopedService(t, sut)
	require.Error(t, err)
	assert.ErrorIs(t, err, syscall.EADDRINUSE)
	require.NoError(t, held.Close())

	retryErr := startScopedService(t, sut)
	require.Error(t, retryErr, "releasing the address must not revive a failed service")
	assert.Contains(t, strings.ToLower(retryErr.Error()), "terminal")
	sut.Start()
	listenerPolicyConcurrentShutdown(t, sut, 2)
	listenerPolicyRequireRebind(t, endpoint)
}

func TestScopedSetupRejectsPortMismatchWithoutReconstructingPolicy(t *testing.T) {
	endpoint := listenerPolicyAvailableEndpoint(t, netip.MustParseAddr("127.0.0.1"))
	configurationPort := endpoint.Port() - 1
	if configurationPort == 0 {
		configurationPort = endpoint.Port() + 1
	}
	policy := ListenerPolicy{ListenAddress: endpoint, DiscoveryEnabled: false}
	sut := NewServiceWithOptions(
		newListenerPolicyConfiguration(t, configurationPort),
		&legacyServiceReaderRecorder{},
		ServiceOptions{ListenerPolicy: &policy},
	)

	err := sut.Setup()
	require.Error(t, err)
	assert.Contains(t, strings.ToLower(err.Error()), "port")
	assertBridgeRuntimeUnpublished(t, sut)
	require.NotPanics(t, func() {
		sut.Start()
		sut.Shutdown()
		sut.Shutdown()
	})
}

func TestServiceExposesOneSynchronousScopedStartMethod(t *testing.T) {
	serviceType := reflect.TypeOf((*Service)(nil))
	errorType := reflect.TypeOf((*error)(nil)).Elem()
	var found []string
	for _, name := range []string{"StartScoped", "StartWithPolicy"} {
		method, ok := serviceType.MethodByName(name)
		if !ok {
			continue
		}
		found = append(found, name)
		assert.Equal(t, 1, method.Type.NumIn(), "%s must accept only its receiver", name)
		if assert.Equal(t, 1, method.Type.NumOut(), "%s must return its synchronous startup error", name) {
			assert.Equal(t, errorType, method.Type.Out(0), "%s must return error", name)
		}
	}
	require.Len(t, found, 1, "Service must expose one clear additive scoped-start method")
}

const listenerPolicyMissingInterface = "helianthus-listener-policy-missing-interface"

func newListenerPolicyService(t *testing.T, endpoint netip.AddrPort, discovery bool) *Service {
	t.Helper()
	policy := ListenerPolicy{ListenAddress: endpoint, DiscoveryEnabled: discovery}
	return NewServiceWithOptions(
		newListenerPolicyConfiguration(t, endpoint.Port()),
		&legacyServiceReaderRecorder{},
		ServiceOptions{ListenerPolicy: &policy},
	)
}

func newListenerPolicyConfiguration(t *testing.T, port uint16) *api.Configuration {
	t.Helper()
	certificate, err := cert.CreateCertificate("listener-policy", "helianthus", "ro", "localhost")
	require.NoError(t, err)
	configuration, err := api.NewConfiguration(
		"vendor",
		"brand",
		"model",
		"serial",
		spinemodel.DeviceTypeTypeEnergyManagementSystem,
		[]spinemodel.EntityTypeType{spinemodel.EntityTypeTypeCEM},
		int(port),
		certificate,
		4*time.Second,
	)
	require.NoError(t, err)
	return configuration
}

func startScopedService(t *testing.T, sut *Service) error {
	t.Helper()
	switch starter := any(sut).(type) {
	case interface{ StartScoped() error }:
		return starter.StartScoped()
	case interface{ StartWithPolicy() error }:
		return starter.StartWithPolicy()
	default:
		t.Fatal("Service lacks a synchronous scoped-start method")
		return nil
	}
}

func listenerPolicyAvailableEndpoint(t *testing.T, address netip.Addr) netip.AddrPort {
	t.Helper()
	listener := listenerPolicyListen(t, netip.AddrPortFrom(address, 0))
	endpoint := listener.Addr().(*net.TCPAddr).AddrPort()
	require.NoError(t, listener.Close())
	return endpoint
}

func listenerPolicyAvailableIPv6Endpoint(t *testing.T) (netip.AddrPort, bool) {
	t.Helper()
	listener, err := net.ListenTCP("tcp6", net.TCPAddrFromAddrPort(netip.MustParseAddrPort("[::1]:0")))
	if err != nil {
		t.Skipf("IPv6 loopback unavailable: %v", err)
		return netip.AddrPort{}, false
	}
	endpoint := listener.Addr().(*net.TCPAddr).AddrPort()
	require.NoError(t, listener.Close())
	return endpoint, true
}

func listenerPolicyListen(t *testing.T, endpoint netip.AddrPort) *net.TCPListener {
	t.Helper()
	network := "tcp6"
	if endpoint.Addr().Is4() {
		network = "tcp4"
	}
	listener, err := net.ListenTCP(network, net.TCPAddrFromAddrPort(endpoint))
	require.NoError(t, err)
	return listener
}

func listenerPolicyDial(t *testing.T, endpoint netip.AddrPort) {
	t.Helper()
	connection, err := net.DialTimeout("tcp", endpoint.String(), time.Second)
	require.NoError(t, err)
	require.NoError(t, connection.Close())
}

func listenerPolicyRequireUnreachable(t *testing.T, endpoint netip.AddrPort) {
	t.Helper()
	connection, err := net.DialTimeout("tcp4", endpoint.String(), 200*time.Millisecond)
	if connection != nil {
		_ = connection.Close()
	}
	require.Error(t, err, "listener unexpectedly widened to %s", endpoint)
}

func listenerPolicyConcurrentShutdown(t *testing.T, sut *Service, calls int) {
	t.Helper()
	var wait sync.WaitGroup
	for call := 0; call < calls; call++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			sut.Shutdown()
		}()
	}
	wait.Wait()
	sut.Shutdown()
}

func listenerPolicyRequireRebind(t *testing.T, endpoint netip.AddrPort) {
	t.Helper()
	listener := listenerPolicyListen(t, endpoint)
	require.NoError(t, listener.Close())
}
