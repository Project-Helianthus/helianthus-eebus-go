package session

import (
	"bytes"
	"crypto/tls"
	"net"
	"testing"
	"time"

	"github.com/Project-Helianthus/helianthus-eebus-go/api"
	"github.com/Project-Helianthus/helianthus-eebus-go/discovery"
	shipapi "github.com/Project-Helianthus/helianthus-ship-go/api"
	"github.com/Project-Helianthus/helianthus-ship-go/cert"
	"github.com/Project-Helianthus/helianthus-ship-go/mdns"
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
		Ski:          skiRemoteA,
		EventType:    spineapi.EventTypeDataChange,
		Function:     model.FunctionTypeMeasurementListData,
		Data:         "notified-value",
		LocalFeature: localFeature(t, owner, model.FeatureTypeTypeMeasurement, model.RoleTypeClient),
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

func TestSharedRemoteSKIEventsStayWithTheOwningInstance(t *testing.T) {
	leftTransport := &offlinePeer{heldAnswered: true}
	rightTransport := &offlinePeer{heldAnswered: true}
	leftRoot := t.TempDir()
	rightRoot := t.TempDir()
	left := openAt(t, leftRoot, 4791, "shared-left", leftTransport)
	right := openAt(t, rightRoot, 4792, "shared-right", rightTransport)
	t.Cleanup(func() { _ = left.Close() })
	t.Cleanup(func() { _ = right.Close() })

	var leftHits, rightHits int
	left.SetNotifyHandler(func(string, Pair, model.FunctionType, any, time.Time) { leftHits++ })
	right.SetNotifyHandler(func(string, Pair, model.FunctionType, any, time.Time) { rightHits++ })

	pair := measurementPair(11)
	server := pair.Server
	for _, instance := range []*Instance{left, right} {
		if err := instance.Trust(skiRemoteA, true); err != nil {
			t.Fatal(err)
		}
		if err := instance.Connect(skiRemoteA, true); err != nil {
			t.Fatal(err)
		}
		if err := instance.Select(skiRemoteA, []Pair{pair}); err != nil {
			t.Fatal(err)
		}
	}
	if len(leftTransport.subscribes) != 1 || len(rightTransport.subscribes) != 1 {
		t.Fatalf("subscribes left=%d right=%d", len(leftTransport.subscribes), len(rightTransport.subscribes))
	}
	if err := left.NoteResult(skiRemoteA, leftTransport.subscribes[0].counter, true); err != nil {
		t.Fatal(err)
	}
	if err := right.NoteResult(skiRemoteA, rightTransport.subscribes[0].counter, true); err != nil {
		t.Fatal(err)
	}

	leftFeature := localFeature(t, left, model.FeatureTypeTypeMeasurement, model.RoleTypeClient)
	rightFeature := localFeature(t, right, model.FeatureTypeTypeMeasurement, model.RoleTypeClient)
	notify := spineapi.EventPayload{
		Ski:          skiRemoteA,
		EventType:    spineapi.EventTypeDataChange,
		Function:     model.FunctionTypeMeasurementListData,
		Data:         "from-left-connection",
		Feature:      remoteFeatureAddress{address: &server},
		LocalFeature: leftFeature,
	}
	left.HandleEvent(notify)
	right.HandleEvent(notify)
	notify.Data = "from-right-connection"
	notify.LocalFeature = rightFeature
	left.HandleEvent(notify)
	right.HandleEvent(notify)
	if leftHits != 1 || rightHits != 1 {
		t.Fatalf("hits left=%d right=%d", leftHits, rightHits)
	}
	leftValue, leftAt, leftOK := left.LastNotified(skiRemoteA, pair, model.FunctionTypeMeasurementListData)
	rightValue, rightAt, rightOK := right.LastNotified(skiRemoteA, pair, model.FunctionTypeMeasurementListData)
	if !leftOK || leftValue != "from-left-connection" || leftAt.IsZero() {
		t.Fatalf("left cache value=%v at=%s ok=%t", leftValue, leftAt, leftOK)
	}
	if !rightOK || rightValue != "from-right-connection" || rightAt.IsZero() {
		t.Fatalf("right cache value=%v at=%s ok=%t", rightValue, rightAt, rightOK)
	}

	leftServer := localFeature(t, left, model.FeatureTypeTypeMeasurement, model.RoleTypeServer)
	kept := Pair{
		Client:            featureAddress("d:_n:test_remote-a", 1, 21),
		Server:            *leftServer.Address(),
		ServerFeatureType: model.FeatureTypeTypeMeasurement,
	}
	if err := left.AcceptRemoteSubscribe(skiRemoteA, kept); err != nil {
		t.Fatal(err)
	}
	if err := right.AcceptRemoteSubscribe(skiRemoteA, kept); err != nil {
		t.Fatal(err)
	}
	remove := spineapi.EventPayload{
		Ski:          skiRemoteA,
		EventType:    spineapi.EventTypeSubscriptionChange,
		ChangeType:   spineapi.ElementChangeRemove,
		LocalFeature: leftServer,
		Data: model.SubscriptionManagementDeleteCallType{
			ClientAddress: &kept.Client,
			ServerAddress: &kept.Server,
		},
	}
	left.HandleEvent(remove)
	right.HandleEvent(remove)
	if pairEstablished(left, skiRemoteA, kept) || !pairEstablished(right, skiRemoteA, kept) {
		t.Fatalf("delete crossed instances left=%t right=%t", pairEstablished(left, skiRemoteA, kept), pairEstablished(right, skiRemoteA, kept))
	}

	added := Pair{
		Client:            featureAddress("d:_n:test_remote-a", 1, 22),
		Server:            *leftServer.Address(),
		ServerFeatureType: model.FeatureTypeTypeMeasurement,
	}
	add := spineapi.EventPayload{
		Ski:          skiRemoteA,
		EventType:    spineapi.EventTypeSubscriptionChange,
		ChangeType:   spineapi.ElementChangeAdd,
		LocalFeature: leftServer,
		Data: model.SubscriptionManagementRequestCallType{
			ClientAddress:     &added.Client,
			ServerAddress:     &added.Server,
			ServerFeatureType: &added.ServerFeatureType,
		},
	}
	left.HandleEvent(add)
	right.HandleEvent(add)
	if !pairEstablished(left, skiRemoteA, added) || pairEstablished(right, skiRemoteA, added) {
		t.Fatalf("add crossed instances left=%t right=%t", pairEstablished(left, skiRemoteA, added), pairEstablished(right, skiRemoteA, added))
	}
	if !pairEstablished(left, skiRemoteA, pair) || !pairEstablished(right, skiRemoteA, pair) {
		t.Fatal("client pair was dropped")
	}

	if err := right.Close(); err != nil {
		t.Fatal(err)
	}
	reopened := openAt(t, rightRoot, 4794, "shared-right", &offlinePeer{heldAnswered: true})
	t.Cleanup(func() { _ = reopened.Close() })
	if !pairEstablished(reopened, skiRemoteA, kept) || pairEstablished(reopened, skiRemoteA, added) {
		t.Fatalf("reopened right kept=%t added=%t records=%+v", pairEstablished(reopened, skiRemoteA, kept), pairEstablished(reopened, skiRemoteA, added), reopened.Established())
	}
}

type remoteFeatureAddress struct {
	spineapi.FeatureRemoteInterface
	address *model.FeatureAddressType
}

func (f remoteFeatureAddress) Address() *model.FeatureAddressType { return f.address }

func localFeature(t *testing.T, instance *Instance, kind model.FeatureTypeType, role model.RoleType) spineapi.FeatureLocalInterface {
	t.Helper()
	device := instance.LocalDevice()
	if device == nil {
		t.Fatal("local device is missing")
	}
	entity := device.Entity([]model.AddressEntityType{1})
	if entity == nil {
		t.Fatal("local entity is missing")
	}
	feature := entity.GetOrAddFeature(kind, role)
	if feature == nil || feature.Device() == nil || feature.Device().Address() == nil || feature.Address() == nil {
		t.Fatal("local feature is missing")
	}
	return feature
}

func TestOpenRunsEachSHIPListener(t *testing.T) {
	left := openAt(t, t.TempDir(), 4761, "listen-left", nil)
	right := openAt(t, t.TempDir(), 4762, "listen-right", nil)
	t.Cleanup(func() { _ = left.Close() })
	t.Cleanup(func() { _ = right.Close() })

	if !left.Running() || !right.Running() {
		t.Fatalf("running left=%t right=%t", left.Running(), right.Running())
	}
	if !shipPortOpen(4761) || !shipPortOpen(4762) {
		t.Fatal("SHIP listener is not accepting")
	}
	if !shipAnnouncementSeen(left.Discovery().Announcement().Name, left.SKI(), 2*time.Second) {
		t.Fatal("left SHIP service was not announced")
	}
	if !shipAnnouncementSeen(right.Discovery().Announcement().Name, right.SKI(), 2*time.Second) {
		t.Fatal("right SHIP service was not announced")
	}

	left.mu.Lock()
	left.port = 0
	left.mu.Unlock()
	if left.Running() {
		t.Fatal("running ignores the SHIP listener")
	}
	left.mu.Lock()
	left.port = 4761
	left.mu.Unlock()
	if !left.Running() {
		t.Fatal("SHIP listener was dropped")
	}

	if err := left.Close(); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for shipPortOpen(4761) {
		if time.Now().After(deadline) {
			t.Fatal("closed instance is still listening")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if left.Running() {
		t.Fatal("closed instance reports running")
	}
	if !right.Running() || !shipPortOpen(4762) {
		t.Fatal("closing one instance stopped the other SHIP listener")
	}
	if shipAnnouncementSeen(left.Discovery().Announcement().Name, left.SKI(), time.Second) {
		t.Fatal("closed instance is still announced")
	}
	if !shipAnnouncementSeen(right.Discovery().Announcement().Name, right.SKI(), 2*time.Second) {
		t.Fatal("closing one instance stopped the other SHIP announcement")
	}
}

func shipAnnouncementSeen(name, ski string, wait time.Duration) bool {
	conn, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4zero, Port: 0})
	if err != nil {
		return false
	}
	defer func() { _ = conn.Close() }()
	dst := &net.UDPAddr{IP: net.ParseIP("224.0.0.251"), Port: 5353}
	query := shipServiceQuery()
	deadline := time.Now().Add(wait)
	buf := make([]byte, 2048)
	markers := [][]byte{
		[]byte(name),
		[]byte("path=/ship/"),
		[]byte("ski=" + ski),
		[]byte("register=false"),
	}
	for time.Now().Before(deadline) {
		_ = conn.SetWriteDeadline(time.Now().Add(200 * time.Millisecond))
		_, _ = conn.WriteToUDP(query, dst)
		_ = conn.SetReadDeadline(time.Now().Add(200 * time.Millisecond))
		for {
			n, _, err := conn.ReadFromUDP(buf)
			if err != nil {
				break
			}
			packet := buf[:n]
			matched := true
			for _, marker := range markers {
				if !bytes.Contains(packet, marker) {
					matched = false
					break
				}
			}
			if matched {
				return true
			}
		}
	}
	return false
}

// shipServiceQuery is a unicast-response PTR query for the SHIP mDNS service.
func shipServiceQuery() []byte {
	name := []byte{
		5, '_', 's', 'h', 'i', 'p',
		4, '_', 't', 'c', 'p',
		5, 'l', 'o', 'c', 'a', 'l',
		0,
	}
	msg := make([]byte, 12+len(name)+4)
	msg[0], msg[1] = 0, 1
	msg[5] = 1
	copy(msg[12:], name)
	off := 12 + len(name)
	msg[off], msg[off+1] = 0, 12
	msg[off+2], msg[off+3] = 0x80, 1
	return msg
}

func TestInstanceDiscoversRemoteSHIPService(t *testing.T) {
	const thirdSKI = "cccccccccccccccccccccccccccccccccccccccc"
	provider := mdns.NewZeroconfProvider(nil)
	t.Cleanup(provider.Shutdown)
	third := discovery.TXT{
		ID:       "id-third",
		SKI:      thirdSKI,
		Register: true,
		Brand:    "helianthus",
		Type:     "EnergyManagementSystem",
		Model:    "stack",
	}
	if err := provider.Announce("offline-peer", 4773, third.Records()); err != nil {
		t.Fatal(err)
	}

	left := openAt(t, t.TempDir(), 4771, "browse-left", nil)
	right := openAt(t, t.TempDir(), 4772, "browse-right", nil)
	t.Cleanup(func() { _ = left.Close() })
	t.Cleanup(func() { _ = right.Close() })

	waitDiscovered(t, left, right.SKI(), discoveredAnnouncement(right, "browse-right", false))
	waitDiscovered(t, left, thirdSKI, discovery.Announcement{
		Name: "offline-peer",
		Port: 4773,
		TXT:  announcedTXT(third),
	})
	waitDiscovered(t, right, left.SKI(), discoveredAnnouncement(left, "browse-left", false))
	waitDiscovered(t, right, thirdSKI, discovery.Announcement{
		Name: "offline-peer",
		Port: 4773,
		TXT:  announcedTXT(third),
	})
	if _, ok := left.Discovery().Remote(left.SKI()); ok {
		t.Fatal("instance listed its own SHIP service")
	}

	if err := left.Close(); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for shipPortOpen(4771) {
		if time.Now().After(deadline) {
			t.Fatal("closed instance is still listening")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if shipAnnouncementSeen(left.Discovery().Announcement().Name, left.SKI(), time.Second) {
		t.Fatal("closed instance is still announced")
	}
	if !right.Running() || !shipPortOpen(4772) {
		t.Fatal("closing one instance stopped the other SHIP listener")
	}
	if !shipAnnouncementSeen(right.Discovery().Announcement().Name, right.SKI(), 2*time.Second) {
		t.Fatal("closing one instance stopped the other SHIP announcement")
	}
}

func discoveredAnnouncement(instance *Instance, serial string, register bool) discovery.Announcement {
	return discovery.Announcement{
		Name: "helianthus-stack-" + serial,
		Port: instance.Port(),
		TXT: discovery.TXT{
			TXTVers:  "1",
			ID:       "helianthus-stack-" + serial,
			Path:     discovery.WebsocketPath,
			SKI:      instance.SKI(),
			Register: register,
			Brand:    "helianthus",
			Type:     "EnergyManagementSystem",
			Model:    "stack",
		},
	}
}

func announcedTXT(txt discovery.TXT) discovery.TXT {
	txt.TXTVers = "1"
	if txt.Path == "" {
		txt.Path = discovery.WebsocketPath
	}
	return txt
}

func waitDiscovered(t *testing.T, instance *Instance, ski string, want discovery.Announcement) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	var last discovery.Remote
	var ok bool
	for time.Now().Before(deadline) {
		last, ok = instance.Discovery().Remote(ski)
		got := last.Announcement
		if ok && got.Name == want.Name && got.Host != "" && got.Port == want.Port && got.TXT == want.TXT &&
			!last.Paired && !last.Trusted && last.Connection == shipapi.ConnectionStateNone {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("remote %s = %+v ok=%t remotes=%+v", ski, last, ok, instance.Discovery().Remotes())
}

func TestCloseReturnsWhilePeerRecordsAreInFlight(t *testing.T) {
	const thirdSKI = "cccccccccccccccccccccccccccccccccccccccc"
	provider := mdns.NewZeroconfProvider(nil)
	t.Cleanup(func() { provider.Shutdown() })
	third := discovery.TXT{
		ID:       "id-third",
		SKI:      thirdSKI,
		Register: true,
		Brand:    "helianthus",
		Type:     "EnergyManagementSystem",
		Model:    "stack",
	}
	if err := provider.Announce("offline-peer", 4783, third.Records()); err != nil {
		t.Fatal(err)
	}
	left := openAt(t, t.TempDir(), 4781, "close-left", nil)
	right := openAt(t, t.TempDir(), 4782, "close-right", nil)
	t.Cleanup(func() { _ = right.Close() })

	waitDiscovered(t, left, right.SKI(), discoveredAnnouncement(right, "close-right", false))
	waitDiscovered(t, right, left.SKI(), discoveredAnnouncement(left, "close-left", false))
	waitDiscovered(t, left, thirdSKI, discovery.Announcement{
		Name: "offline-peer",
		Port: 4783,
		TXT:  announcedTXT(third),
	})

	finished := make(chan error, 1)
	go func() {
		finished <- left.Close()
	}()
	select {
	case err := <-finished:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Close did not return while a peer SHIP record was in flight")
	}
	if left.Running() || shipPortOpen(4781) {
		t.Fatal("closed instance is still listening")
	}
	if shipAnnouncementSeen(left.Discovery().Announcement().Name, left.SKI(), time.Second) {
		t.Fatal("closed instance is still announced")
	}
	if !right.Running() || !shipPortOpen(4782) {
		t.Fatal("closing one instance stopped the other SHIP listener")
	}
	if !shipAnnouncementSeen(right.Discovery().Announcement().Name, right.SKI(), 2*time.Second) {
		t.Fatal("closing one instance stopped the other SHIP announcement")
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
