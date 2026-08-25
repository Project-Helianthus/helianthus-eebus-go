package server_test

import (
	"testing"

	"github.com/Project-Helianthus/helianthus-eebus-go/features/server"
	spineapi "github.com/Project-Helianthus/helianthus-spine-go/api"
	"github.com/Project-Helianthus/helianthus-spine-go/model"
	"github.com/Project-Helianthus/helianthus-spine-go/util"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

// TestNorthboundMeasurementNativeSPINEConformance is a small in-memory
// conformance seed for a native SPINE Measurement server. It deliberately
// stops at the local CEM/service boundary: no SHIP or network runtime is
// needed to prove the read-only description/data contract.
func TestNorthboundMeasurementNativeSPINEConformance(t *testing.T) {
	suite.Run(t, new(NorthboundMeasurementPOCSuite))
}

type NorthboundMeasurementPOCSuite struct {
	suite.Suite

	measurement *server.Measurement
	feature     spineapi.FeatureInterface
}

func (s *NorthboundMeasurementPOCSuite) BeforeTest(_, _ string) {
	fixture := new(MeasurementSuite)
	fixture.SetT(s.T())
	fixture.BeforeTest("NorthboundMeasurementPOCSuite", "Test")
	s.measurement = fixture.sut
	s.feature = fixture.localEntity.FeatureOfTypeAndRole(model.FeatureTypeTypeMeasurement, model.RoleTypeServer)
}

func (s *NorthboundMeasurementPOCSuite) TestDescriptionAndDataAreReadOnly() {
	feature := s.feature
	require.NotNil(s.T(), feature)

	description := model.MeasurementDescriptionDataType{
		MeasurementType: util.Ptr(model.MeasurementTypeTypeTemperature),
		CommodityType:   util.Ptr(model.CommodityTypeTypeHeatingwater),
		Unit:            util.Ptr(model.UnitOfMeasurementTypedegC),
		ScopeType:       util.Ptr(model.ScopeTypeTypeFlowTemperature),
	}
	measurementID := s.measurement.AddDescription(description)
	require.NotNil(s.T(), measurementID)

	gotDescription, err := s.measurement.GetDescriptionForId(*measurementID)
	require.NoError(s.T(), err)
	require.NotNil(s.T(), gotDescription)
	assert.Equal(s.T(), description.MeasurementType, gotDescription.MeasurementType)
	assert.Equal(s.T(), description.CommodityType, gotDescription.CommodityType)
	assert.Equal(s.T(), description.Unit, gotDescription.Unit)
	assert.Equal(s.T(), description.ScopeType, gotDescription.ScopeType)

	data := model.MeasurementDataType{
		ValueType: util.Ptr(model.MeasurementValueTypeTypeValue),
		Value:     model.NewScaledNumberType(625),
	}
	require.NoError(s.T(), s.measurement.UpdateDataForId(data, nil, *measurementID))

	gotData, err := s.measurement.GetDataForId(*measurementID)
	require.NoError(s.T(), err)
	require.NotNil(s.T(), gotData)
	assert.Equal(s.T(), measurementID, gotData.MeasurementId)
	assert.Equal(s.T(), data.ValueType, gotData.ValueType)
	assert.Equal(s.T(), data.Value, gotData.Value)

	operations := feature.Operations()
	descriptionOperations, ok := operations[model.FunctionTypeMeasurementDescriptionListData]
	require.True(s.T(), ok)
	assert.True(s.T(), descriptionOperations.Read())
	assert.False(s.T(), descriptionOperations.Write())
	assert.False(s.T(), descriptionOperations.WritePartial())
	dataOperations, ok := operations[model.FunctionTypeMeasurementListData]
	require.True(s.T(), ok)
	assert.True(s.T(), dataOperations.Read())
	assert.False(s.T(), dataOperations.Write())
	assert.False(s.T(), dataOperations.WritePartial())
}
