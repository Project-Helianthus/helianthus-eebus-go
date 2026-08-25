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

func TestNorthboundDeviceConfigurationNativeSPINEConformance(t *testing.T) {
	suite.Run(t, new(NorthboundDeviceConfigurationPOCSuite))
}

type NorthboundDeviceConfigurationPOCSuite struct {
	suite.Suite

	configuration *server.DeviceConfiguration
	feature       spineapi.FeatureInterface
}

func (s *NorthboundDeviceConfigurationPOCSuite) BeforeTest(_, _ string) {
	fixture := new(DeviceConfigurationSuite)
	fixture.SetT(s.T())
	fixture.BeforeTest("NorthboundDeviceConfigurationPOCSuite", "Test")
	s.configuration = fixture.sut
	s.feature = fixture.localEntity.FeatureOfTypeAndRole(model.FeatureTypeTypeDeviceConfiguration, model.RoleTypeServer)
}

func (s *NorthboundDeviceConfigurationPOCSuite) TestDescriptorRetentionAndReadOnlyOperations() {
	require.NotNil(s.T(), s.feature)

	description := model.DeviceConfigurationKeyValueDescriptionDataType{
		KeyName:   util.Ptr(model.DeviceConfigurationKeyNameTypeFailsafeConsumptionActivePowerLimit),
		ValueType: util.Ptr(model.DeviceConfigurationKeyValueTypeTypeScaledNumber),
		Unit:      util.Ptr(model.UnitOfMeasurementTypeW),
	}
	keyID := s.configuration.AddKeyValueDescription(description)
	require.NotNil(s.T(), keyID)

	got, err := s.configuration.GetKeyValueDescriptionFoKeyId(*keyID)
	require.NoError(s.T(), err)
	require.NotNil(s.T(), got)
	assert.Equal(s.T(), keyID, got.KeyId)
	assert.Equal(s.T(), description.KeyName, got.KeyName)
	assert.Equal(s.T(), description.ValueType, got.ValueType)
	assert.Equal(s.T(), description.Unit, got.Unit)

	operations := s.feature.Operations()
	descriptionOperations, ok := operations[model.FunctionTypeDeviceConfigurationKeyValueDescriptionListData]
	require.True(s.T(), ok)
	assert.True(s.T(), descriptionOperations.Read())
	assert.False(s.T(), descriptionOperations.Write())
	assert.False(s.T(), descriptionOperations.WritePartial())
}
