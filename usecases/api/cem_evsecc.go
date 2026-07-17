package api

import (
	"github.com/Project-Helianthus/helianthus-eebus-go/api"
	spineapi "github.com/Project-Helianthus/helianthus-spine-go/api"
	"github.com/Project-Helianthus/helianthus-spine-go/model"
)

// Actor: Customer Energy Management
// UseCase: EVSE Commissioning and Configuration
type CemEVSECCInterface interface {
	api.UseCaseInterface

	// the manufacturer data of an EVSE
	//
	// parameters:
	//   - entity: the entity of the EV
	//
	// returns deviceName, serialNumber, error
	ManufacturerData(entity spineapi.EntityRemoteInterface) (api.ManufacturerData, error)

	// the operating state data of an EVSE
	//
	// parameters:
	//   - entity: the entity of the EV
	//
	// returns operatingState, lastErrorCode, error
	OperatingState(entity spineapi.EntityRemoteInterface) (model.DeviceDiagnosisOperatingStateType, string, error)
}
