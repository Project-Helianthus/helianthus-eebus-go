package mgcp

import (
	"testing"

	"github.com/Project-Helianthus/helianthus-eebus-go/api"
	spineapi "github.com/Project-Helianthus/helianthus-spine-go/api"
	"github.com/Project-Helianthus/helianthus-spine-go/model"
	"github.com/Project-Helianthus/helianthus-spine-go/spine"
	"github.com/Project-Helianthus/helianthus-spine-go/util"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestScope(t *testing.T) {
	a := newScopeClient(t, "scope-a", 1)
	b := newScopeClient(t, "scope-b", 2)
	// The two connections represent the same peer, but have distinct remote objects.
	added := spineapi.EventPayload{
		Ski: b.peer.Ski(), Device: b.peer, Entity: b.entity,
		EventType: spineapi.EventTypeEntityChange, ChangeType: spineapi.ElementChangeAdd,
		Data: &model.NodeManagementDetailedDiscoveryDataType{},
	}
	a.client.HandleEvent(added)
	a.client.UseCaseBase.HandleEvent(added)
	assert.Empty(t, a.sender.calls, "A must not subscribe in response to B's event")
	assert.Empty(t, b.sender.calls, "A must not send a read on B's connection")
	assert.Empty(t, a.client.RemoteEntitiesScenarios())
	assert.Empty(t, a.updates)

	a.sender.calls, b.sender.calls = nil, nil
	b.client.HandleEvent(added)
	b.client.UseCaseBase.HandleEvent(added)
	src := b.client.LocalEntity.FeatureOfTypeAndRole(model.FeatureTypeTypeDeviceConfiguration, model.RoleTypeClient).Address()
	dst := b.entity.FeatureOfTypeAndRole(model.FeatureTypeTypeDeviceConfiguration, model.RoleTypeServer).Address()
	assert.Equal(t, []scopeCall{
		{class: model.CmdClassifierTypeCall, source: src, destination: dst, feature: model.FeatureTypeTypeDeviceConfiguration},
		{class: model.CmdClassifierTypeRead, source: src, destination: dst, readsConfigDescription: true},
	}, b.sender.calls)
	assert.Empty(t, a.sender.calls)
	assert.Equal(t, []api.RemoteEntityScenarios{{Entity: b.entity, Scenarios: []uint{1}}}, b.client.RemoteEntitiesScenarios())
	assert.Equal(t, []api.EventType{UseCaseSupportUpdate}, b.updates)

	addedA := added
	addedA.Device, addedA.Entity = a.peer, a.entity
	a.client.UseCaseBase.HandleEvent(addedA)
	aAvailable := []api.RemoteEntityScenarios{{Entity: a.entity, Scenarios: []uint{1}}}
	assert.Equal(t, aAvailable, a.client.RemoteEntitiesScenarios())

	// Connection removal has no Entity and arrives after deletion from the local map.
	b.local.RemoveRemoteDevice(b.peer.Ski())
	removed := spineapi.EventPayload{
		Ski: b.peer.Ski(), Device: b.peer,
		EventType: spineapi.EventTypeDeviceChange, ChangeType: spineapi.ElementChangeRemove,
	}
	b.client.HandleEvent(removed)
	b.client.UseCaseBase.HandleEvent(removed)
	assert.Empty(t, b.client.RemoteEntitiesScenarios())
	assert.Equal(t, []api.EventType{UseCaseSupportUpdate, UseCaseSupportUpdate}, b.updates)
	a.client.UseCaseBase.HandleEvent(removed)
	assert.Equal(t, aAvailable, a.client.RemoteEntitiesScenarios())
	assert.False(t, b.client.EventOwned(removed), "handled removals must not retain the remote")

	b.client.Stop()
	b.local.AddRemoteDeviceForSki(b.peer.Ski(), b.peer)
	b.sender.calls, b.updates = nil, nil
	b.client.HandleEvent(added)
	b.client.UseCaseBase.HandleEvent(added)
	assert.Empty(t, b.sender.calls)
	assert.Empty(t, b.client.RemoteEntitiesScenarios())
	assert.Empty(t, b.updates)
}

type scopeClient struct {
	local   *spine.DeviceLocal
	client  *MGCP
	peer    *spine.DeviceRemote
	entity  *spine.EntityRemote
	sender  *scopeSender
	updates []api.EventType
}

func newScopeClient(t *testing.T, address string, entityID model.AddressEntityType) *scopeClient {
	t.Helper()
	f := &scopeClient{sender: &scopeSender{}}
	f.local = spine.NewDeviceLocal("", "", "", "", address, model.DeviceTypeTypeEnergyManagementSystem, "")
	localEntity := spine.NewEntityLocal(f.local, model.EntityTypeTypeCEM, []model.AddressEntityType{entityID}, 0)
	f.local.AddEntity(localEntity)
	f.client = NewMGCP(localEntity, func(_ string, _ spineapi.DeviceRemoteInterface, _ spineapi.EntityRemoteInterface, event api.EventType) {
		f.updates = append(f.updates, event)
	})
	t.Cleanup(func() {
		_ = spine.Events.Unsubscribe(f.client)
		_ = spine.Events.Unsubscribe(f.client.UseCaseBase)
	})
	f.client.AddFeatures()

	f.peer = spine.NewDeviceRemote(f.local, "scope-peer-ski", f.sender)
	peerAddress := util.Ptr(model.AddressDeviceType("scope-peer"))
	f.peer.UpdateDevice(&model.NetworkManagementDeviceDescriptionDataType{DeviceAddress: &model.DeviceAddressType{Device: peerAddress}})
	f.entity = spine.NewEntityRemote(f.peer, model.EntityTypeTypeGridConnectionPointOfPremises, []model.AddressEntityType{3})
	f.peer.AddEntity(f.entity)
	f.local.AddRemoteDeviceForSki(f.peer.Ski(), f.peer)
	server := spine.NewFeatureRemote(5, f.entity, model.FeatureTypeTypeDeviceConfiguration, model.RoleTypeServer)
	server.SetOperations([]model.FunctionPropertyType{{
		Function:           util.Ptr(model.FunctionTypeDeviceConfigurationKeyValueDescriptionListData),
		PossibleOperations: &model.PossibleOperationsType{Read: &model.PossibleOperationsReadType{}},
	}})
	f.entity.AddFeature(server)

	data := &model.NodeManagementUseCaseDataType{}
	data.AddUseCaseSupport(*server.Address(), model.UseCaseActorTypeGridConnectionPoint,
		model.UseCaseNameTypeMonitoringOfGridConnectionPoint, "1.0.0", "release", true, []model.UseCaseScenarioSupportType{1})
	node := f.peer.Entity(spine.DeviceInformationAddressEntity).FeatureOfTypeAndRole(model.FeatureTypeTypeNodeManagement, model.RoleTypeSpecial)
	_, err := node.UpdateData(true, model.FunctionTypeNodeManagementUseCaseData, data, nil, nil)
	require.Nil(t, err)
	return f
}

type scopeCall struct {
	class                  model.CmdClassifierType
	source, destination    *model.FeatureAddressType
	feature                model.FeatureTypeType
	readsConfigDescription bool
}

type scopeSender struct {
	spineapi.SenderInterface
	calls []scopeCall
}

func (s *scopeSender) Request(class model.CmdClassifierType, source, destination *model.FeatureAddressType, _ bool, cmd []model.CmdType) (*model.MsgCounterType, error) {
	s.calls = append(s.calls, scopeCall{class: class, source: source, destination: destination, readsConfigDescription: len(cmd) == 1 && cmd[0].DeviceConfigurationKeyValueDescriptionListData != nil})
	return util.Ptr(model.MsgCounterType(len(s.calls))), nil
}

func (s *scopeSender) Subscribe(source, destination *model.FeatureAddressType, feature model.FeatureTypeType) (*model.MsgCounterType, error) {
	s.calls = append(s.calls, scopeCall{class: model.CmdClassifierTypeCall, source: source, destination: destination, feature: feature})
	return util.Ptr(model.MsgCounterType(len(s.calls))), nil
}
