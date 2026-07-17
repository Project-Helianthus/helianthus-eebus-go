package evsoc

import (
	"github.com/Project-Helianthus/helianthus-eebus-go/api"
	"github.com/Project-Helianthus/helianthus-eebus-go/features/client"
	spineapi "github.com/Project-Helianthus/helianthus-spine-go/api"
	"github.com/Project-Helianthus/helianthus-spine-go/model"
	"github.com/Project-Helianthus/helianthus-spine-go/util"
)

// return the last known SoC of the connected EV
//
// only works with a current ISO15118-2 with VAS or ISO15118-20
// communication between EVSE and EV
//
// possible errors:
//   - ErrDataNotAvailable if no such measurement is (yet) available
//   - and others
func (e *EVSOC) StateOfCharge(entity spineapi.EntityRemoteInterface) (float64, error) {
	if !e.IsCompatibleEntityType(entity) {
		return 0, api.ErrNoCompatibleEntity
	}

	evMeasurement, err := client.NewMeasurement(e.LocalEntity, entity)
	if err != nil || evMeasurement == nil {
		return 0, err
	}

	filter := model.MeasurementDescriptionDataType{
		MeasurementType: util.Ptr(model.MeasurementTypeTypePercentage),
		CommodityType:   util.Ptr(model.CommodityTypeTypeElectricity),
		ScopeType:       util.Ptr(model.ScopeTypeTypeStateOfCharge),
	}
	result, err := evMeasurement.GetDataForFilter(filter)
	if err != nil || len(result) == 0 || result[0].Value == nil {
		return 0, api.ErrDataNotAvailable
	}
	return result[0].Value.GetValue(), nil
}
