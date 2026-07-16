package api

import "github.com/Project-Helianthus/helianthus-spine-go/model"

//go:generate mockery

var PhaseNameMapping = []model.ElectricalConnectionPhaseNameType{model.ElectricalConnectionPhaseNameTypeA, model.ElectricalConnectionPhaseNameTypeB, model.ElectricalConnectionPhaseNameTypeC}
