package usecase

import (
	"slices"

	"github.com/Project-Helianthus/helianthus-eebus-go/api"
	"github.com/Project-Helianthus/helianthus-eebus-go/usecases/internal"
	spineapi "github.com/Project-Helianthus/helianthus-spine-go/api"
	"github.com/Project-Helianthus/helianthus-spine-go/model"
)

// EventOwned reports whether an event belongs to this running use case.
func (u *UseCaseBase) EventOwned(payload spineapi.EventPayload) bool {
	return u.eventScope.Owns(u.LocalEntity, payload)
}

// Stop makes the client and base ignore subsequent events, including queued dispatches.
func (u *UseCaseBase) Stop() {
	u.eventScope.Stop()
}

// handle SPINE events
func (u *UseCaseBase) HandleEvent(payload spineapi.EventPayload) {
	if !u.EventOwned(payload) {
		return
	}

	if u.deviceOrEntityRemoved(payload) {
		return
	}

	switch payload.Data.(type) {
	case *model.NodeManagementUseCaseDataType,
		*model.NodeManagementDetailedDiscoveryDataType:
		u.useCaseDataUpdate(payload)
	default:
		return
	}
}

func (u *UseCaseBase) deviceOrEntityRemoved(payload spineapi.EventPayload) bool {
	if internal.IsDeviceDisconnected(payload) {
		// Only the base consumes device removals. The client may see the same
		// dispatch first, so retire the remembered remote here, not in the gate.
		u.eventScope.ForgetRemote(payload.Device)
		u.removeDeviceFromAvailableEntityScenarios(payload.Device)
		return true
	}
	if internal.IsEntityDisconnected(payload) {
		u.removeEntityFromAvailableEntityScenarios(payload.Entity)
		return true
	}

	return false
}

func (u *UseCaseBase) removeDeviceFromAvailableEntityScenarios(device spineapi.DeviceRemoteInterface) {
	var removed []spineapi.EntityRemoteInterface
	u.mux.Lock()
	u.availableEntityScenarios = slices.DeleteFunc(u.availableEntityScenarios, func(item api.RemoteEntityScenarios) bool {
		if item.Entity.Device() != device {
			return false
		}
		removed = append(removed, item.Entity)
		return true
	})
	u.mux.Unlock()

	if u.EventCB != nil {
		for _, entity := range removed {
			u.EventCB(device.Ski(), device, entity, u.useCaseUpdateEvent)
		}
	}
}

func (u *UseCaseBase) useCaseDataUpdate(
	payload spineapi.EventPayload,
) {
	remoteDevice := payload.Device
	if remoteDevice == nil && payload.Entity != nil {
		remoteDevice = payload.Entity.Device()
	}
	if remoteDevice == nil {
		return
	}

	// go over the use cases and check which entity of the remote device supports the usecase
	ucs := remoteDevice.UseCases()
	for _, uc := range ucs {
		if uc.Actor == nil || !slices.Contains(u.validActorTypes, *uc.Actor) {
			continue
		}

		for _, support := range uc.UseCaseSupport {
			// UseCaseAvailable should only be checkd for client functionality
			// of a use acse. But as there are devices (e.g. Porsche PMCC) that
			// also use it for the server side, we need to check it always.
			if support.UseCaseName == nil ||
				*support.UseCaseName != u.UseCaseName ||
				support.ScenarioSupport == nil &&
					len(support.ScenarioSupport) == 0 ||
				(support.UseCaseAvailable != nil && !*support.UseCaseAvailable) {
				continue
			}

			// TODO: check the version and compare them for compatibility, it may be required to provide a minimmal compatible version
			// but then also the question is what to do if the version is newer

			entitiesToCheck := []spineapi.EntityRemoteInterface{}

			// if the address is given, use that for further checks
			if uc.Address != nil {
				ucEntity := remoteDevice.Entity(uc.Address.Entity)
				// the PMCP EVSE reports EV use cases with the address of the EVSE
				if *uc.Actor == model.UseCaseActorTypeEV && len(uc.Address.Entity) == 1 {
					// add the EV subentity to the address
					evAddress := append(uc.Address.Entity, 1)
					ucEntity = remoteDevice.Entity(evAddress)
				}
				if ucEntity != nil {
					entitiesToCheck = append(entitiesToCheck, ucEntity)
				}
			}

			// if no entity was defined/found, go over all known entities and search for one or multiple
			if len(entitiesToCheck) == 0 {
				entitiesToCheck = remoteDevice.Entities()
			}

			for _, entity := range entitiesToCheck {
				if !slices.Contains(u.validEntityTypes, entity.EntityType()) {
					continue
				}

				supportedScenarios := []model.UseCaseScenarioSupportType{}

				// go over each scenario this use case supports and check if the required server features are available
				for _, scenario := range u.useCaseScenarios {
					if !slices.Contains(support.ScenarioSupport, scenario.Scenario) {
						continue
					}

					// check if the required server features are available
					requiredServerFeatures := u.requiredServerFeaturesForScenario(scenario.Scenario)
					foundMatchingServerFeatures := []model.FeatureTypeType{}
					for _, feature := range entity.Features() {
						if feature.Role() != model.RoleTypeServer ||
							!slices.Contains(requiredServerFeatures, feature.Type()) {
							continue
						}

						foundMatchingServerFeatures = append(foundMatchingServerFeatures, feature.Type())
					}

					// check if the minimum required server features are available
					slices.Sort(foundMatchingServerFeatures)
					slices.Sort(requiredServerFeatures)

					if slices.Compare(requiredServerFeatures, foundMatchingServerFeatures) != 0 {
						continue
					}

					supportedScenarios = append(supportedScenarios, scenario.Scenario)
				}

				u.updateRemoteEntityScenarios(entity, supportedScenarios)
			}
		}
	}
}
