package service_test

import (
	"reflect"
	"testing"

	"github.com/Project-Helianthus/helianthus-eebus-go/api"
	"github.com/Project-Helianthus/helianthus-eebus-go/service"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var legacyServiceConstructor func(
	*api.Configuration,
	api.ServiceReaderInterface,
) *service.Service = service.NewService

var legacyOutgoingBridgeConstructor func(
	*api.Configuration,
	api.ServiceReaderInterface,
	service.OutgoingAttemptBridgeConfiguration,
) *service.Service = service.NewServiceWithOutgoingAttemptBridge

var optionsServiceConstructor func(
	*api.Configuration,
	api.ServiceReaderInterface,
	service.ServiceOptions,
) *service.Service = service.NewServiceWithOptions

var _ api.ServiceInterface = (*service.Service)(nil)

type legacyServiceLifecycle interface {
	Setup() error
	Start()
	Shutdown()
}

var _ legacyServiceLifecycle = (*service.Service)(nil)

var _ = service.ServiceOptions{
	ListenerPolicy:        &service.ListenerPolicy{},
	OutgoingAttemptBridge: &service.OutgoingAttemptBridgeConfiguration{},
}

func TestScopedListenerAdditionsDoNotChangeLegacyPublicSignatures(t *testing.T) {
	require.NotNil(t, legacyServiceConstructor)
	require.NotNil(t, legacyOutgoingBridgeConstructor)
	require.NotNil(t, optionsServiceConstructor)

	serviceType := reflect.TypeOf((*service.Service)(nil))
	setup, ok := serviceType.MethodByName("Setup")
	require.True(t, ok)
	assert.Equal(t, 1, setup.Type.NumOut())
	assert.Equal(t, reflect.TypeOf((*error)(nil)).Elem(), setup.Type.Out(0))
	start, ok := serviceType.MethodByName("Start")
	require.True(t, ok)
	assert.Zero(t, start.Type.NumOut(), "legacy Start must remain void")
}
