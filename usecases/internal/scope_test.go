package internal

import (
	"testing"

	spineapi "github.com/Project-Helianthus/helianthus-spine-go/api"
	"github.com/Project-Helianthus/helianthus-spine-go/model"
	"github.com/Project-Helianthus/helianthus-spine-go/spine"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestScopeOwnership(t *testing.T) {
	local := newScopeLocalEntity("local", 1)
	sameAddress := newScopeLocalEntity("local", 2)
	other := newScopeLocalEntity("other", 1)
	matchingFeature := spine.NewFeatureLocal(3, sameAddress, model.FeatureTypeTypeMeasurement, model.RoleTypeClient)
	foreignFeature := spine.NewFeatureLocal(3, other, model.FeatureTypeTypeMeasurement, model.RoleTypeClient)

	remote := spine.NewDeviceRemote(local.Device(), "shared-ski", nil)
	foreign := spine.NewDeviceRemote(other.Device(), remote.Ski(), nil)
	local.Device().AddRemoteDeviceForSki(remote.Ski(), remote)
	other.Device().AddRemoteDeviceForSki(foreign.Ski(), foreign)
	entity := spine.NewEntityRemote(remote, model.EntityTypeTypeEV, []model.AddressEntityType{1})
	foreignEntity := spine.NewEntityRemote(foreign, model.EntityTypeTypeEV, []model.AddressEntityType{1})

	for _, tt := range []struct {
		name    string
		payload spineapi.EventPayload
		owned   bool
	}{
		{"no owner evidence", spineapi.EventPayload{Ski: remote.Ski()}, false},
		{"local feature address value", spineapi.EventPayload{LocalFeature: matchingFeature}, true},
		{"foreign local feature address", spineapi.EventPayload{LocalFeature: foreignFeature}, false},
		{"missing remote SKI", spineapi.EventPayload{Device: remote}, false},
		{"attached remote device", spineapi.EventPayload{Device: remote, Ski: remote.Ski()}, true},
		{"foreign remote with same SKI", spineapi.EventPayload{Device: foreign, Ski: foreign.Ski()}, false},
		{"attached remote entity", spineapi.EventPayload{Entity: entity, Ski: remote.Ski()}, true},
		{"foreign remote entity with same SKI", spineapi.EventPayload{Entity: foreignEntity, Ski: foreign.Ski()}, false},
		{"unrelated local feature with attached remote", spineapi.EventPayload{LocalFeature: foreignFeature, Device: remote, Ski: remote.Ski()}, false},
		{"matching local feature with foreign remote", spineapi.EventPayload{LocalFeature: matchingFeature, Device: foreign, Ski: foreign.Ski()}, false},
		{"device and entity match", spineapi.EventPayload{Device: remote, Entity: entity, Ski: remote.Ski()}, true},
		{"device and entity disagree", spineapi.EventPayload{Device: remote, Entity: foreignEntity, Ski: remote.Ski()}, false},
		{"payload SKI disagrees", spineapi.EventPayload{Device: remote, Ski: "different-ski"}, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var scope EventScope
			assert.Equal(t, tt.owned, scope.Owns(local, tt.payload))
		})
	}
}

func TestScopeRemoval(t *testing.T) {
	local := newScopeLocalEntity("local", 1)
	remote := spine.NewDeviceRemote(local.Device(), "reused-ski", nil)
	local.Device().AddRemoteDeviceForSki(remote.Ski(), remote)

	var scope EventScope
	require.True(t, scope.Owns(local, spineapi.EventPayload{Device: remote, Ski: remote.Ski()}))
	local.Device().RemoveRemoteDevice(remote.Ski())
	replacement := spine.NewDeviceRemote(local.Device(), remote.Ski(), nil)
	local.Device().AddRemoteDeviceForSki(replacement.Ski(), replacement)

	remove := spineapi.EventPayload{Device: remote, Ski: remote.Ski(), EventType: spineapi.EventTypeDeviceChange, ChangeType: spineapi.ElementChangeRemove}
	assert.True(t, scope.Owns(local, remove), "the old remote is still recognized for its removal")
	assert.True(t, scope.Owns(local, remove), "the client must not consume the base handler's removal")
	assert.False(t, scope.Owns(local, spineapi.EventPayload{Device: remote, Ski: remote.Ski()}), "a detached remote cannot re-add itself")
	remove.Ski = "different-ski"
	assert.False(t, scope.Owns(local, remove), "a removal must use the accepted SKI")
	remove.Ski = remote.Ski()
	remove.EventType = spineapi.EventTypeDataChange
	assert.False(t, scope.Owns(local, remove), "other event types cannot use the removal fallback")
	remove.EventType = spineapi.EventTypeDeviceChange
	remove.Device = spine.NewDeviceRemote(local.Device(), remote.Ski(), nil)
	assert.False(t, scope.Owns(local, remove), "a never-accepted pointer cannot use the same SKI's removal fallback")

	scope.Stop()
	remove.Device = remote
	assert.False(t, scope.Owns(local, remove), "stop also rejects a previously owned removal")
	assert.False(t, scope.Owns(local, spineapi.EventPayload{Device: replacement, Ski: replacement.Ski()}), "stop rejects an attached remote")
	assert.False(t, scope.Owns(local, spineapi.EventPayload{
		LocalFeature: spine.NewFeatureLocal(3, local, model.FeatureTypeTypeMeasurement, model.RoleTypeClient),
	}), "stop rejects a local feature")
}

func newScopeLocalEntity(address string, entityID model.AddressEntityType) *spine.EntityLocal {
	device := spine.NewDeviceLocal("", "", "", "", address, model.DeviceTypeTypeEnergyManagementSystem, "")
	return spine.NewEntityLocal(device, model.EntityTypeTypeCEM, []model.AddressEntityType{entityID}, 0)
}
