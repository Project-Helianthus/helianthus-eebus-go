package lpc

import (
	"testing"

	"github.com/Project-Helianthus/helianthus-eebus-go/api"
	"github.com/Project-Helianthus/helianthus-eebus-go/usecases/cem/cevc"
	"github.com/Project-Helianthus/helianthus-eebus-go/usecases/cem/opev"
	"github.com/Project-Helianthus/helianthus-eebus-go/usecases/cem/oscev"
	"github.com/Project-Helianthus/helianthus-eebus-go/usecases/cs/lpp"
	eglpc "github.com/Project-Helianthus/helianthus-eebus-go/usecases/eg/lpc"
	eglpp "github.com/Project-Helianthus/helianthus-eebus-go/usecases/eg/lpp"
	spineapi "github.com/Project-Helianthus/helianthus-spine-go/api"
	"github.com/Project-Helianthus/helianthus-spine-go/model"
	"github.com/Project-Helianthus/helianthus-spine-go/spine"
	"github.com/Project-Helianthus/helianthus-spine-go/util"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestScopeHandlers(t *testing.T) {
	a, b := newScopePeer("scope-a"), newScopePeer("scope-b")
	csLPC, csLPP := NewLPC(a.localEntity, nil), lpp.NewLPP(a.localEntity, nil)
	egLPC, egLPP := eglpc.NewLPC(a.localEntity, nil), eglpp.NewLPP(a.localEntity, nil)
	cemCEVC, cemOPEV, cemOSCEV := cevc.NewCEVC(a.localEntity, nil), opev.NewOPEV(a.localEntity, nil), oscev.NewOSCEV(a.localEntity, nil)
	for _, tc := range []struct {
		name    string
		handler scopeHandler
		base    spineapi.EventHandlerInterface
	}{
		{"cs/lpc", csLPC, csLPC.UseCaseBase},
		{"cs/lpp", csLPP, csLPP.UseCaseBase},
		{"eg/lpc", egLPC, egLPC.UseCaseBase},
		{"eg/lpp", egLPP, egLPP.UseCaseBase},
		{"cem/cevc", cemCEVC, cemCEVC.UseCaseBase},
		{"cem/opev", cemOPEV, cemOPEV.UseCaseBase},
		{"cem/oscev", cemOSCEV, cemOSCEV.UseCaseBase},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Dispatch directly so the assertions do not depend on asynchronous delivery.
			require.NoError(t, spine.Events.Unsubscribe(tc.handler))
			require.NoError(t, spine.Events.Unsubscribe(tc.base))
			a.entity.typeChecks, b.entity.typeChecks = 0, 0
			h := tc.handler

			// Observe whether the handler reaches its existing entity-type check.
			event := spineapi.EventPayload{Ski: b.peer.Ski(), Entity: b.entity}
			h.HandleEvent(event)
			assert.Zero(t, b.entity.typeChecks, "foreign peer must not reach entity type filtering")

			event.Entity = a.entity
			h.HandleEvent(event)
			assert.Equal(t, 1, a.entity.typeChecks, "owned peer must reach entity type filtering")

			h.Stop()
			h.HandleEvent(event)
			assert.Equal(t, 1, a.entity.typeChecks, "stopped handler must not inspect the event")
		})
	}
}

func TestScopeLPC(t *testing.T) {
	a, b := newScopeLPC(t, "scope-a"), newScopeLPC(t, "scope-b")
	connected := spineapi.EventPayload{
		Ski: b.peer.Ski(), Device: b.peer,
		EventType: spineapi.EventTypeDeviceChange, ChangeType: spineapi.ElementChangeAdd,
	}
	a.uc.HandleEvent(connected)
	assert.Nil(t, a.uc.heartbeatDiag, "foreign connect must not change heartbeat state")
	assert.Empty(t, a.sender.calls)
	assert.Empty(t, b.sender.calls, "foreign connect must not send on the other instance")
	a.sender.calls, b.sender.calls = nil, nil
	b.uc.HandleEvent(connected)
	assert.NotNil(t, b.uc.heartbeatDiag)
	assert.Equal(t, []model.CmdClassifierType{model.CmdClassifierTypeCall, model.CmdClassifierTypeRead}, b.sender.calls)

	heartbeat := spineapi.EventPayload{
		Ski: b.peer.Ski(), Device: b.peer, Entity: b.entity,
		LocalFeature: b.diagClient, EventType: spineapi.EventTypeDataChange,
		ChangeType: spineapi.ElementChangeUpdate, CmdClassifier: util.Ptr(model.CmdClassifierTypeNotify),
		Function: model.FunctionTypeDeviceDiagnosisHeartbeatData, Data: &model.DeviceDiagnosisHeartbeatDataType{},
	}
	for _, tc := range []struct {
		name   string
		mutate func(*spineapi.EventPayload)
	}{
		{"foreign local feature", func(p *spineapi.EventPayload) { p.LocalFeature = a.diagClient }},
		{"foreign device", func(p *spineapi.EventPayload) { p.Device = a.peer }},
		{"foreign entity", func(p *spineapi.EventPayload) { p.Entity = a.entity }},
		{"wrong SKI", func(p *spineapi.EventPayload) { p.Ski = "different-ski" }},
		{"missing SKI", func(p *spineapi.EventPayload) { p.Ski = "" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			changed := heartbeat
			tc.mutate(&changed)
			b.updates = nil
			b.uc.HandleEvent(changed)
			assert.Empty(t, b.updates)
		})
	}
	b.updates = nil
	b.uc.HandleEvent(heartbeat)
	assert.Equal(t, []api.EventType{DataUpdateHeartbeat}, b.updates)

	for _, f := range []*scopeLPC{a, b} {
		f.uc.UseCaseBase.HandleEvent(spineapi.EventPayload{
			Ski: f.peer.Ski(), Device: f.peer,
			EventType: spineapi.EventTypeDataChange, ChangeType: spineapi.ElementChangeUpdate,
			Data: &model.NodeManagementUseCaseDataType{},
		})
		require.Len(t, f.uc.RemoteEntitiesScenarios(), 1)
		f.updates = nil
	}

	b.local.RemoveRemoteDevice(b.peer.Ski())
	removed := connected
	removed.ChangeType = spineapi.ElementChangeRemove
	a.uc.HandleEvent(removed)
	a.uc.UseCaseBase.HandleEvent(removed)
	b.uc.HandleEvent(removed)
	b.uc.UseCaseBase.HandleEvent(removed)
	assert.Len(t, a.uc.RemoteEntitiesScenarios(), 1)
	assert.Empty(t, a.updates)
	assert.Empty(t, b.uc.RemoteEntitiesScenarios(), "client must leave removal for the base handler")
	assert.Equal(t, []api.EventType{UseCaseSupportUpdate}, b.updates)
	assert.False(t, b.uc.EventOwned(removed), "base must retire the detached peer")

	b.uc.Stop()
	b.local.AddRemoteDeviceForSki(b.peer.Ski(), b.peer)
	b.sender.calls, b.updates = nil, nil
	b.uc.HandleEvent(connected)
	b.uc.HandleEvent(heartbeat)
	assert.Empty(t, b.sender.calls, "stopped handler must not initiate heartbeat traffic")
	assert.Empty(t, b.updates, "stopped handler must not call back")

	heartbeat.Device, heartbeat.Entity, heartbeat.LocalFeature = a.peer, a.entity, a.diagClient
	a.uc.HandleEvent(heartbeat)
	assert.Equal(t, []api.EventType{DataUpdateHeartbeat}, a.updates, "stopping B must not stop A")
}

type scopeHandler interface {
	spineapi.EventHandlerInterface
	Stop()
}

type scopeEntity struct {
	*spine.EntityRemote
	typeChecks int
}

func (e *scopeEntity) EntityType() model.EntityTypeType {
	e.typeChecks++
	return e.EntityRemote.EntityType()
}

type scopePeer struct {
	local       *spine.DeviceLocal
	localEntity *spine.EntityLocal
	peer        *spine.DeviceRemote
	entity      *scopeEntity
	sender      *scopeSender
}

func newScopePeer(address string) scopePeer {
	local := spine.NewDeviceLocal("", "", "", "", address, model.DeviceTypeTypeEnergyManagementSystem, "")
	localEntity := spine.NewEntityLocal(local, model.EntityTypeTypeCEM, []model.AddressEntityType{1}, 0)
	local.AddEntity(localEntity)
	sender := &scopeSender{}
	peer := spine.NewDeviceRemote(local, "scope-peer-ski", sender)
	peer.UpdateDevice(&model.NetworkManagementDeviceDescriptionDataType{
		DeviceAddress: &model.DeviceAddressType{Device: util.Ptr(model.AddressDeviceType("scope-peer"))},
	})
	entity := &scopeEntity{EntityRemote: spine.NewEntityRemote(peer, model.EntityTypeTypeGridGuard, []model.AddressEntityType{2})}
	peer.AddEntity(entity)
	local.AddRemoteDeviceForSki(peer.Ski(), peer)
	return scopePeer{local: local, localEntity: localEntity, peer: peer, entity: entity, sender: sender}
}

type scopeLPC struct {
	scopePeer
	uc         *LPC
	diagClient spineapi.FeatureLocalInterface
	updates    []api.EventType
}

func newScopeLPC(t *testing.T, address string) *scopeLPC {
	t.Helper()
	f := &scopeLPC{scopePeer: newScopePeer(address)}
	f.uc = NewLPC(f.localEntity, func(_ string, _ spineapi.DeviceRemoteInterface, _ spineapi.EntityRemoteInterface, event api.EventType) {
		f.updates = append(f.updates, event)
	})
	require.NoError(t, spine.Events.Unsubscribe(f.uc))
	require.NoError(t, spine.Events.Unsubscribe(f.uc.UseCaseBase))
	f.diagClient = f.localEntity.GetOrAddFeature(model.FeatureTypeTypeDeviceDiagnosis, model.RoleTypeClient)

	server := spine.NewFeatureRemote(3, f.entity, model.FeatureTypeTypeDeviceDiagnosis, model.RoleTypeServer)
	server.SetOperations([]model.FunctionPropertyType{{
		Function:           util.Ptr(model.FunctionTypeDeviceDiagnosisHeartbeatData),
		PossibleOperations: &model.PossibleOperationsType{Read: &model.PossibleOperationsReadType{}},
	}})
	f.entity.AddFeature(server)

	data := &model.NodeManagementUseCaseDataType{}
	data.AddUseCaseSupport(*server.Address(), model.UseCaseActorTypeEnergyGuard,
		model.UseCaseNameTypeLimitationOfPowerConsumption, "1.0.0", "release", true, []model.UseCaseScenarioSupportType{3})
	node := f.peer.Entity(spine.DeviceInformationAddressEntity).FeatureOfTypeAndRole(model.FeatureTypeTypeNodeManagement, model.RoleTypeSpecial)
	_, err := node.UpdateData(true, model.FunctionTypeNodeManagementUseCaseData, data, nil, nil)
	require.Nil(t, err)
	return f
}

type scopeSender struct {
	spineapi.SenderInterface
	calls []model.CmdClassifierType
}

func (s *scopeSender) Subscribe(_, _ *model.FeatureAddressType, _ model.FeatureTypeType) (*model.MsgCounterType, error) {
	return s.record(model.CmdClassifierTypeCall), nil
}

func (s *scopeSender) Request(class model.CmdClassifierType, _, _ *model.FeatureAddressType, _ bool, _ []model.CmdType) (*model.MsgCounterType, error) {
	return s.record(class), nil
}

func (s *scopeSender) record(class model.CmdClassifierType) *model.MsgCounterType {
	s.calls = append(s.calls, class)
	return util.Ptr(model.MsgCounterType(len(s.calls)))
}
