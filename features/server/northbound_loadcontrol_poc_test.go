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

func TestNorthboundLoadControlNativeSPINEConformance(t *testing.T) {
	suite.Run(t, new(NorthboundLoadControlPOCSuite))
}

type NorthboundLoadControlPOCSuite struct {
	suite.Suite

	loadControl *server.LoadControl
	feature     spineapi.FeatureInterface
}

func (s *NorthboundLoadControlPOCSuite) BeforeTest(_, _ string) {
	fixture := new(LoadControlSuite)
	fixture.SetT(s.T())
	fixture.BeforeTest("NorthboundLoadControlPOCSuite", "Test")
	s.loadControl = fixture.sut
	s.feature = fixture.localEntity.FeatureOfTypeAndRole(model.FeatureTypeTypeLoadControl, model.RoleTypeServer)
}

func (s *NorthboundLoadControlPOCSuite) TestLimitDescriptorRetentionAndReadOnlyOperations() {
	require.NotNil(s.T(), s.feature)

	description := model.LoadControlLimitDescriptionDataType{
		LimitType:      util.Ptr(model.LoadControlLimitTypeTypeSignDependentAbsValueLimit),
		LimitCategory:  util.Ptr(model.LoadControlCategoryTypeObligation),
		LimitDirection: util.Ptr(model.EnergyDirectionTypeConsume),
		ScopeType:      util.Ptr(model.ScopeTypeTypeActivePowerLimit),
	}
	limitID := s.loadControl.AddLimitDescription(description)
	require.NotNil(s.T(), limitID)

	got, err := s.loadControl.GetLimitDescriptionForId(*limitID)
	require.NoError(s.T(), err)
	require.NotNil(s.T(), got)
	assert.Equal(s.T(), limitID, got.LimitId)
	assert.Equal(s.T(), description.LimitType, got.LimitType)
	assert.Equal(s.T(), description.LimitCategory, got.LimitCategory)
	assert.Equal(s.T(), description.LimitDirection, got.LimitDirection)
	assert.Equal(s.T(), description.ScopeType, got.ScopeType)

	operations := s.feature.Operations()
	descriptionOperations, ok := operations[model.FunctionTypeLoadControlLimitDescriptionListData]
	require.True(s.T(), ok)
	assert.True(s.T(), descriptionOperations.Read())
	assert.False(s.T(), descriptionOperations.Write())
	assert.False(s.T(), descriptionOperations.WritePartial())
}
