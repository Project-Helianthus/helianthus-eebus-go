package evsecc

import (
	"github.com/Project-Helianthus/helianthus-eebus-go/api"
	ucapi "github.com/Project-Helianthus/helianthus-eebus-go/usecases/api"
	"github.com/Project-Helianthus/helianthus-eebus-go/usecases/usecase"
	spineapi "github.com/Project-Helianthus/helianthus-spine-go/api"
	"github.com/Project-Helianthus/helianthus-spine-go/model"
	"github.com/Project-Helianthus/helianthus-spine-go/spine"
)

type EVSECC struct {
	*usecase.UseCaseBase
}

var _ ucapi.CemEVSECCInterface = (*EVSECC)(nil)

func NewEVSECC(localEntity spineapi.EntityLocalInterface, eventCB api.EntityEventCallback) *EVSECC {
	validActorTypes := []model.UseCaseActorType{
		model.UseCaseActorTypeEVSE,
		model.UseCaseActorTypeEV, // The Porsche PMCC devices use this actor for this use case incorrectly
	}
	validEntityTypes := []model.EntityTypeType{
		model.EntityTypeTypeEVSE,
	}
	useCaseScenarios := []api.UseCaseScenario{
		{
			Scenario:       model.UseCaseScenarioSupportType(1),
			ServerFeatures: []model.FeatureTypeType{model.FeatureTypeTypeDeviceClassification},
		},
		{
			Scenario:       model.UseCaseScenarioSupportType(2),
			Mandatory:      true,
			ServerFeatures: []model.FeatureTypeType{model.FeatureTypeTypeDeviceDiagnosis},
		},
	}

	usecase := usecase.NewUseCaseBase(
		localEntity,
		model.UseCaseActorTypeCEM,
		model.UseCaseNameTypeEVSECommissioningAndConfiguration,
		"1.0.1",
		"release",
		useCaseScenarios,
		eventCB,
		UseCaseSupportUpdate,
		validActorTypes,
		validEntityTypes)

	uc := &EVSECC{
		UseCaseBase: usecase,
	}

	_ = spine.Events.Subscribe(uc)

	return uc
}

func (e *EVSECC) AddFeatures() {
	// client features
	var clientFeatures = []model.FeatureTypeType{
		model.FeatureTypeTypeDeviceClassification,
		model.FeatureTypeTypeDeviceDiagnosis,
	}

	for _, feature := range clientFeatures {
		_ = e.LocalEntity.GetOrAddFeature(feature, model.RoleTypeClient)
	}
}
