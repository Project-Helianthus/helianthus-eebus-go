package usecase

import (
	"testing"

	spineapi "github.com/Project-Helianthus/helianthus-spine-go/api"
	"github.com/Project-Helianthus/helianthus-spine-go/model"
	"github.com/Project-Helianthus/helianthus-spine-go/spine"
	"github.com/Project-Helianthus/helianthus-spine-go/util"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEventOwnedDeviceRemoval(t *testing.T) {
	local := spine.NewDeviceLocal("", "", "", "", "local", model.DeviceTypeTypeEnergyManagementSystem, "")
	base := &UseCaseBase{LocalEntity: spine.NewEntityLocal(local, model.EntityTypeTypeCEM, []model.AddressEntityType{1}, 0)}
	old := spine.NewDeviceRemote(local, "departing-ski", nil)
	other := spine.NewDeviceRemote(local, "remaining-ski", nil)
	for _, peer := range []*spine.DeviceRemote{old, other} {
		peer.UpdateDevice(&model.NetworkManagementDeviceDescriptionDataType{
			DeviceAddress: &model.DeviceAddressType{Device: util.Ptr(model.AddressDeviceType("same-address"))},
		})
		local.AddRemoteDeviceForSki(peer.Ski(), peer)
		require.True(t, base.EventOwned(spineapi.EventPayload{Device: peer, Ski: peer.Ski()}))
	}
	for i, peer := range []*spine.DeviceRemote{old, other, old} {
		entity := spine.NewEntityRemote(peer, model.EntityTypeTypeEV, []model.AddressEntityType{model.AddressEntityType(i + 1)})
		peer.AddEntity(entity)
		base.updateRemoteEntityScenarios(entity, []model.UseCaseScenarioSupportType{1})
	}
	require.Len(t, base.RemoteEntitiesScenarios(), 3)

	local.RemoveRemoteDevice(old.Ski())
	base.HandleEvent(spineapi.EventPayload{
		Device: old, Ski: old.Ski(), EventType: spineapi.EventTypeDeviceChange, ChangeType: spineapi.ElementChangeRemove,
	})
	remaining := base.RemoteEntitiesScenarios()
	require.Len(t, remaining, 1)
	assert.Same(t, other, remaining[0].Entity.Device())
}

func (s *UseCaseSuite) Test_HandleEvent() {
	payload := spineapi.EventPayload{}
	s.uc.HandleEvent(payload)

	payload.Device = s.remoteDevice
	payload.EventType = spineapi.EventTypeDeviceChange
	payload.ChangeType = spineapi.ElementChangeRemove
	s.uc.HandleEvent(payload)

	payload.Entity = s.remoteDevice.Entities()[0]
	payload.EventType = spineapi.EventTypeDataChange
	payload.ChangeType = spineapi.ElementChangeUpdate
	payload.Data = &model.NodeManagementUseCaseDataType{}
	s.uc.HandleEvent(payload)

	payload.Data = &model.NodeManagementDetailedDiscoveryDataType{}
	s.uc.HandleEvent(payload)
}

func (s *UseCaseSuite) Test_useCaseDataUpdate() {
	payload := spineapi.EventPayload{
		Device:     s.remoteDevice,
		Entity:     s.remoteDevice.Entities()[0],
		EventType:  spineapi.EventTypeDataChange,
		ChangeType: spineapi.ElementChangeUpdate,
		Data:       &model.NodeManagementUseCaseDataType{},
	}
	s.uc.useCaseDataUpdate(payload)

	result := s.uc.IsScenarioAvailableAtEntity(s.monitoredEntity, 1)
	assert.False(s.T(), result)

	feature := s.monitoredEntity.FeatureOfTypeAndRole(model.FeatureTypeTypeLoadControl, model.RoleTypeServer)
	address := &model.FeatureAddressType{
		Device:  s.monitoredEntity.Device().Address(),
		Entity:  []model.AddressEntityType{0},
		Feature: util.Ptr(model.AddressFeatureType(0)),
	}
	nodeFeature := s.remoteDevice.FeatureByAddress(address)
	data := &model.NodeManagementUseCaseDataType{}
	data.AddUseCaseSupport(
		*feature.Address(),
		model.UseCaseActorTypeHeatPump,
		useCaseName,
		"1.0.0",
		"release",
		false,
		[]model.UseCaseScenarioSupportType{1, 2, 3})
	_, _ = nodeFeature.UpdateData(true, model.FunctionTypeNodeManagementUseCaseData, data, nil, nil)

	s.uc.useCaseDataUpdate(payload)

	result = s.uc.IsScenarioAvailableAtEntity(s.monitoredEntity, 1)
	assert.False(s.T(), result)

	data = &model.NodeManagementUseCaseDataType{}
	data.AddUseCaseSupport(
		*feature.Address(),
		model.UseCaseActorTypeEV,
		useCaseName,
		"1.0.0",
		"release",
		false,
		[]model.UseCaseScenarioSupportType{1, 2, 3})
	_, _ = nodeFeature.UpdateData(true, model.FunctionTypeNodeManagementUseCaseData, data, nil, nil)

	s.uc.useCaseDataUpdate(payload)

	result = s.uc.IsScenarioAvailableAtEntity(s.monitoredEntity, 1)
	assert.False(s.T(), result)

	data = &model.NodeManagementUseCaseDataType{}
	data.AddUseCaseSupport(
		model.FeatureAddressType{},
		model.UseCaseActorTypeEV,
		useCaseName,
		"1.0.0",
		"release",
		true,
		[]model.UseCaseScenarioSupportType{1, 2, 3})
	_, _ = nodeFeature.UpdateData(true, model.FunctionTypeNodeManagementUseCaseData, data, nil, nil)

	s.uc.useCaseDataUpdate(payload)

	result = s.uc.IsScenarioAvailableAtEntity(s.monitoredEntity, 1)
	assert.True(s.T(), result)

	data = &model.NodeManagementUseCaseDataType{}
	data.AddUseCaseSupport(
		*feature.Address(),
		model.UseCaseActorTypeEV,
		useCaseName,
		"1.0.0",
		"release",
		true,
		[]model.UseCaseScenarioSupportType{1, 2, 3})
	_, _ = nodeFeature.UpdateData(true, model.FunctionTypeNodeManagementUseCaseData, data, nil, nil)

	s.uc.useCaseDataUpdate(payload)

	result = s.uc.IsScenarioAvailableAtEntity(s.monitoredEntity, 1)
	assert.True(s.T(), result)

	data = &model.NodeManagementUseCaseDataType{}
	data.AddUseCaseSupport(
		*feature.Address(),
		model.UseCaseActorTypeEV,
		useCaseName,
		"1.0.0",
		"release",
		true,
		[]model.UseCaseScenarioSupportType{2, 3})
	_, _ = nodeFeature.UpdateData(true, model.FunctionTypeNodeManagementUseCaseData, data, nil, nil)

	s.uc.useCaseDataUpdate(payload)

	result = s.uc.IsScenarioAvailableAtEntity(s.monitoredEntity, 1)
	assert.False(s.T(), result)

	data = &model.NodeManagementUseCaseDataType{}
	data.AddUseCaseSupport(
		*feature.Address(),
		model.UseCaseActorTypeEV,
		useCaseName,
		"1.0.0",
		"release",
		true,
		[]model.UseCaseScenarioSupportType{1, 2, 3})
	_, _ = nodeFeature.UpdateData(true, model.FunctionTypeNodeManagementUseCaseData, data, nil, nil)

	s.uc.useCaseScenarios[0].ServerFeatures = []model.FeatureTypeType{model.FeatureTypeTypeSmartEnergyManagementPs}

	s.uc.useCaseDataUpdate(payload)

	result = s.uc.IsScenarioAvailableAtEntity(s.monitoredEntity, 1)
	assert.False(s.T(), result)
}

func (s *UseCaseSuite) Test_useCaseDataUpdate_PMCP() {
	payload := spineapi.EventPayload{
		Device:     s.remoteDevice,
		Entity:     s.remoteDevice.Entities()[0],
		EventType:  spineapi.EventTypeDataChange,
		ChangeType: spineapi.ElementChangeUpdate,
		Data:       &model.NodeManagementUseCaseDataType{},
	}
	s.uc.useCaseDataUpdate(payload)

	result := s.uc.IsScenarioAvailableAtEntity(s.monitoredEntity, 1)
	assert.False(s.T(), result)

	address := &model.FeatureAddressType{
		Device:  s.monitoredEntity.Device().Address(),
		Entity:  []model.AddressEntityType{0},
		Feature: util.Ptr(model.AddressFeatureType(0)),
	}
	nodeFeature := s.remoteDevice.FeatureByAddress(address)

	// the PMCP device reports the wrong entity address for the EV use cases :(
	data := &model.NodeManagementUseCaseDataType{
		UseCaseInformation: []model.UseCaseInformationDataType{
			{
				Address: &model.FeatureAddressType{
					Device: util.Ptr(model.AddressDeviceType("d:_i:19667_PorscheEVSE_0012345")),
					Entity: []model.AddressEntityType{1},
				},
				Actor: util.Ptr(model.UseCaseActorTypeEVSE),
				UseCaseSupport: []model.UseCaseSupportType{
					{
						UseCaseName:     util.Ptr(model.UseCaseNameType("evseCommissioningAndConfiguration")),
						UseCaseVersion:  util.Ptr(model.SpecificationVersionType("1.0.0")),
						ScenarioSupport: []model.UseCaseScenarioSupportType{1, 2},
					},
				},
			},
			{
				Address: &model.FeatureAddressType{
					Device: util.Ptr(model.AddressDeviceType("d:_i:19667_PorscheEVSE_0012345")),
					Entity: []model.AddressEntityType{1}, // this should include the subentity 1 to point to the EV entity
				},
				Actor: util.Ptr(model.UseCaseActorTypeEV),
				UseCaseSupport: []model.UseCaseSupportType{
					{
						UseCaseName:     util.Ptr(model.UseCaseNameType("evCommissioningAndConfiguration")),
						UseCaseVersion:  util.Ptr(model.SpecificationVersionType("1.0.0")),
						ScenarioSupport: []model.UseCaseScenarioSupportType{1, 2, 3, 6, 7, 8},
					},
					{
						UseCaseName:     util.Ptr(model.UseCaseNameType("overloadProtectionByEvChargingCurrentCurtailment")),
						UseCaseVersion:  util.Ptr(model.SpecificationVersionType("1.0.0")),
						ScenarioSupport: []model.UseCaseScenarioSupportType{1, 2, 3},
					},
					{
						UseCaseName:     util.Ptr(model.UseCaseNameType("measurementOfElectricityDuringEvCharging")),
						UseCaseVersion:  util.Ptr(model.SpecificationVersionType("1.0.0")),
						ScenarioSupport: []model.UseCaseScenarioSupportType{1},
					},
				},
			},
		},
	}
	_, _ = nodeFeature.UpdateData(true, model.FunctionTypeNodeManagementUseCaseData, data, nil, nil)

	s.uc.useCaseDataUpdate(payload)

	result = s.uc.IsScenarioAvailableAtEntity(s.monitoredEntity, 1)
	assert.True(s.T(), result)
}
