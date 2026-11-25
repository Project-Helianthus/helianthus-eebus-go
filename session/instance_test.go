package session

import (
	"crypto/tls"
	"testing"
	"time"

	"github.com/Project-Helianthus/helianthus-eebus-go/api"
	shipapi "github.com/Project-Helianthus/helianthus-ship-go/api"
	"github.com/Project-Helianthus/helianthus-ship-go/cert"
	spineapi "github.com/Project-Helianthus/helianthus-spine-go/api"
	"github.com/Project-Helianthus/helianthus-spine-go/model"
)

const (
	skiRemoteA = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	skiRemoteB = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
)

type discardWriter struct{}

func (discardWriter) WriteShipMessageWithPayload([]byte) {}

func testCertificate(t *testing.T) tls.Certificate {
	t.Helper()
	certificate, err := cert.CreateCertificate("unit", "org", "de", "cn")
	if err != nil {
		t.Fatal(err)
	}
	return certificate
}

func openAt(t *testing.T, root string, port int, serial string, transport Transport) *Instance {
	t.Helper()
	instance, err := Open(Config{
		VendorCode:  "test",
		Brand:       "helianthus",
		Model:       "stack",
		Serial:      serial,
		DeviceType:  model.DeviceTypeTypeEnergyManagementSystem,
		EntityTypes: []model.EntityTypeType{model.EntityTypeTypeCEM},
		Port:        port,
		Certificate: testCertificate(t),
		Heartbeat:   time.Second,
		StateRoot:   root,
		Host:        "local",
		Transport:   transport,
	})
	if err != nil {
		t.Fatal(err)
	}
	return instance
}

func TestTwoInstancesKeepSeparateRemotes(t *testing.T) {
	leftRoot := t.TempDir()
	rightRoot := t.TempDir()
	left := openAt(t, leftRoot, 4711, "left", nil)
	right := openAt(t, rightRoot, 4712, "right", nil)
	t.Cleanup(func() { _ = left.Close() })
	t.Cleanup(func() { _ = right.Close() })

	if left.SKI() == "" || left.SKI() == right.SKI() || left.Port() == right.Port() {
		t.Fatalf("identity left=%s:%d right=%s:%d", left.SKI(), left.Port(), right.SKI(), right.Port())
	}
	if err := left.AttachRemote(skiRemoteA, discardWriter{}); err != nil {
		t.Fatal(err)
	}
	if err := right.AttachRemote(skiRemoteB, discardWriter{}); err != nil {
		t.Fatal(err)
	}
	if left.LocalDevice().RemoteDeviceForSki(skiRemoteA) == nil || left.LocalDevice().RemoteDeviceForSki(skiRemoteB) != nil {
		t.Fatal("left remote set is not its own")
	}
	if right.LocalDevice().RemoteDeviceForSki(skiRemoteB) == nil || right.LocalDevice().RemoteDeviceForSki(skiRemoteA) != nil {
		t.Fatal("right remote set is not its own")
	}
	if err := left.Close(); err != nil {
		t.Fatal(err)
	}
	if left.Running() || !right.Running() {
		t.Fatalf("running left=%t right=%t", left.Running(), right.Running())
	}
	if right.LocalDevice().RemoteDeviceForSki(skiRemoteB) == nil {
		t.Fatal("closing the first instance stopped the second")
	}
}

func TestNotifyReachesOnlyTheOwningInstance(t *testing.T) {
	transport := &offlinePeer{heldAnswered: true}
	root := t.TempDir()
	owner := openAt(t, root, 4721, "owner", transport)
	other := openAt(t, t.TempDir(), 4722, "other", &offlinePeer{heldAnswered: true})
	t.Cleanup(func() { _ = owner.Close() })
	t.Cleanup(func() { _ = other.Close() })

	var ownerHits, otherHits int
	owner.SetNotifyHandler(func(string, Pair, model.FunctionType, any, time.Time) { ownerHits++ })
	other.SetNotifyHandler(func(string, Pair, model.FunctionType, any, time.Time) { otherHits++ })

	pair := measurementPair(11)
	if err := owner.Trust(skiRemoteA, true); err != nil {
		t.Fatal(err)
	}
	if err := owner.Connect(skiRemoteA, true); err != nil {
		t.Fatal(err)
	}
	if err := owner.Select(skiRemoteA, []Pair{pair}); err != nil {
		t.Fatal(err)
	}
	if len(transport.subscribes) != 1 {
		t.Fatalf("subscribes = %d", len(transport.subscribes))
	}
	if err := owner.NoteResult(skiRemoteA, transport.subscribes[0].counter, true); err != nil {
		t.Fatal(err)
	}
	payload := spineapi.EventPayload{
		Ski:       skiRemoteA,
		EventType: spineapi.EventTypeDataChange,
		Function:  model.FunctionTypeMeasurementListData,
		Data:      "notified-value",
	}
	owner.HandleEvent(payload)
	other.HandleEvent(payload)
	if ownerHits != 1 || otherHits != 0 {
		t.Fatalf("hits owner=%d other=%d", ownerHits, otherHits)
	}
	value, at, ok := owner.LastNotified(skiRemoteA, pair, model.FunctionTypeMeasurementListData)
	if !ok || value != "notified-value" || at.IsZero() {
		t.Fatalf("cache value=%v at=%s ok=%t", value, at, ok)
	}
	if _, _, ok := other.LastNotified(skiRemoteA, pair, model.FunctionTypeMeasurementListData); ok {
		t.Fatal("other instance stored the notify")
	}
}

func TestUnpairedDiscoveryKeepsSHIPState(t *testing.T) {
	instance := openAt(t, t.TempDir(), 4723, "view", nil)
	t.Cleanup(func() { _ = instance.Close() })
	instance.VisiblePairingCandidateDiscoverySnapshotUpdated(nil, shipapi.PairingCandidateDiscoverySnapshotV1{
		Candidates: []shipapi.PairingCandidateDiscoveryObservationV1{{
			SKI:        skiRemoteA,
			Name:       "remote",
			Identifier: "remote-id",
			Brand:      "helianthus",
			Type:       "EnergyManagementSystem",
			Model:      "stack",
			Path:       "/ship/",
			Host:       "local",
			Port:       4800,
			Register:   true,
		}},
	})
	instance.ServicePairingDetailUpdate(skiRemoteA, shipapi.NewConnectionStateDetail(shipapi.ConnectionStateInProgress, nil))
	remote, ok := instance.Discovery().Remote(skiRemoteA)
	if !ok || remote.Paired || remote.Connection != shipapi.ConnectionStateInProgress {
		t.Fatalf("remote = %+v ok=%t", remote, ok)
	}
	if remote.Announcement.TXT.SKI != skiRemoteA || remote.Announcement.TXT.Path != "/ship/" || !remote.Announcement.TXT.Register {
		t.Fatalf("txt = %+v", remote.Announcement.TXT)
	}
}

func measurementPair(feature uint) Pair {
	return Pair{
		Client:            featureAddress("d:_n:test_local-a", 1, 1),
		Server:            featureAddress("d:_n:test_remote-a", 1, feature),
		ServerFeatureType: model.FeatureTypeTypeMeasurement,
	}
}

func featureAddress(device string, entity uint, feature uint) model.FeatureAddressType {
	deviceValue := model.AddressDeviceType(device)
	featureValue := model.AddressFeatureType(feature)
	return model.FeatureAddressType{
		Device:  &deviceValue,
		Entity:  []model.AddressEntityType{model.AddressEntityType(entity)},
		Feature: &featureValue,
	}
}

// Compile-time check that the service reader used above matches the constructor.
var _ api.ServiceReaderInterface = (*Instance)(nil)
