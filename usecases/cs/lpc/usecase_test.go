package lpc

import (
	"time"

	spineapi "github.com/Project-Helianthus/helianthus-spine-go/api"
	"github.com/Project-Helianthus/helianthus-spine-go/model"
	"github.com/Project-Helianthus/helianthus-spine-go/util"
	"github.com/stretchr/testify/assert"
)

func (s *CsLPCSuite) Test_loadControlServerAndLimitId() {
	lc, _, err := s.sut.loadControlServerAndLimitId()
	assert.NotNil(s.T(), lc)
	assert.Nil(s.T(), err)

	f := s.sut.LocalEntity.FeatureOfTypeAndRole(model.FeatureTypeTypeLoadControl, model.RoleTypeServer)
	f.UpdateData(model.FunctionTypeLoadControlLimitDescriptionListData, &model.LoadControlLimitDescriptionListDataType{}, nil, nil)
	lc, _, err = s.sut.loadControlServerAndLimitId()
	assert.NotNil(s.T(), lc)
	assert.NotNil(s.T(), err)

	s.sut.LocalEntity = nil
	lc, _, err = s.sut.loadControlServerAndLimitId()
	assert.Nil(s.T(), lc)
	assert.NotNil(s.T(), err)
}

func (s *CsLPCSuite) Test_loadControlWriteCB() {
	msg := spineapi.Message{}

	s.sut.loadControlWriteCB(&msg)
	assert.False(s.T(), s.eventCalled)

	msg = spineapi.Message{
		RequestHeader: &model.HeaderType{
			MsgCounter: util.Ptr(model.MsgCounterType(500)),
		},
		Cmd: model.CmdType{
			LoadControlLimitListData: &model.LoadControlLimitListDataType{},
		},
		DeviceRemote: s.remoteDevice,
		EntityRemote: s.monitoredEntity,
	}
	withMsgCounter := func(source spineapi.Message, counter model.MsgCounterType) spineapi.Message {
		source.RequestHeader = &model.HeaderType{MsgCounter: util.Ptr(counter)}
		return source
	}

	msg0 := withMsgCounter(msg, 500)
	s.sut.loadControlWriteCB(&msg0)

	msg1 := withMsgCounter(msg, 501)
	msg1.Cmd = model.CmdType{
		LoadControlLimitListData: &model.LoadControlLimitListDataType{
			LoadControlLimitData: []model.LoadControlLimitDataType{},
		},
	}

	s.sut.loadControlWriteCB(&msg1)
	assert.False(s.T(), s.eventCalled)

	msg2 := withMsgCounter(msg, 502)
	msg2.Cmd = model.CmdType{
		LoadControlLimitListData: &model.LoadControlLimitListDataType{
			LoadControlLimitData: []model.LoadControlLimitDataType{
				{},
			},
		},
	}

	s.sut.loadControlWriteCB(&msg2)
	assert.False(s.T(), s.eventCalled)

	msg3 := withMsgCounter(msg, 503)
	msg3.Cmd = model.CmdType{
		LoadControlLimitListData: &model.LoadControlLimitListDataType{
			LoadControlLimitData: []model.LoadControlLimitDataType{
				{
					LimitId:       util.Ptr(model.LoadControlLimitIdType(0)),
					IsLimitActive: util.Ptr(true),
					Value:         model.NewScaledNumberType(1000),
					TimePeriod:    model.NewTimePeriodTypeWithRelativeEndTime(time.Minute * 2),
				},
			},
		},
	}

	s.sut.loadControlWriteCB(&msg3)
	assert.True(s.T(), s.eventCalled)

	msg4 := withMsgCounter(msg, 504)
	msg4.Cmd = model.CmdType{
		Filter: []model.FilterType{
			{
				CmdControl: &model.CmdControlType{
					Partial: util.Ptr(model.ElementTagType{}),
				},
			},
		},
		LoadControlLimitListData: &model.LoadControlLimitListDataType{
			LoadControlLimitData: []model.LoadControlLimitDataType{
				{
					LimitId:       util.Ptr(model.LoadControlLimitIdType(0)),
					IsLimitActive: util.Ptr(true),
					Value:         model.NewScaledNumberType(5000),
					TimePeriod:    model.NewTimePeriodTypeWithRelativeEndTime(time.Hour * 3),
				},
			},
		},
	}

	s.sut.loadControlWriteCB(&msg4)
	assert.True(s.T(), s.eventCalled)
}

func (s *CsLPCSuite) Test_UpdateUseCaseAvailability() {
	s.sut.UpdateUseCaseAvailability(true)
}
