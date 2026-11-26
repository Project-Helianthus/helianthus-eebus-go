package session

import (
	"encoding/json"
	"reflect"
	"testing"
	"time"

	spineapi "github.com/Project-Helianthus/helianthus-spine-go/api"
	"github.com/Project-Helianthus/helianthus-spine-go/model"
	"github.com/Project-Helianthus/helianthus-spine-go/spine"
	"github.com/Project-Helianthus/helianthus-spine-go/util"
)

func newAttachDevice(address string) *spine.DeviceLocal {
	return spine.NewDeviceLocal("fixture", "", "", "", address, model.DeviceTypeTypeEnergyManagementSystem, "")
}

func mustAttach(t *testing.T, cfg AttachConfig) *Instance {
	t.Helper()
	s, err := Attach(cfg)
	attachCheck(t, err)
	t.Cleanup(func() { attachCheck(t, s.Close()) })
	return s
}

func attachCheck(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func assertAttachedRecords(t *testing.T, s *Instance, want []Record) {
	t.Helper()
	got := s.Established()
	if len(got) != len(want) || len(want) != 0 && !reflect.DeepEqual(got, want) {
		t.Fatalf("retained records = %+v, want %+v", got, want)
	}
}

func TestAttachRetainsAndReconcilesWithoutOwningSHIP(t *testing.T) {
	pair := measurementPair(31)
	device := newAttachDevice(string(*pair.Client.Device))
	peer := &offlinePeer{heldAnswered: true}
	cfg := AttachConfig{Device: device, StateRoot: t.TempDir(), Transport: peer}
	s := mustAttach(t, cfg)
	if s.LocalDevice() != device || s.Running() || s.hub != nil || s.browser != nil || s.registration != nil {
		t.Fatal("attachment did not reuse the device without a listener, browse or announcement")
	}

	// Use a second device with the same SPINE address and the same peer and
	// pair. Neither the address nor the pair should tie the state roots together.
	otherPeer := &offlinePeer{heldAnswered: true}
	otherCfg := AttachConfig{Device: newAttachDevice(string(*pair.Client.Device)), StateRoot: t.TempDir(), Transport: otherPeer}
	other := mustAttach(t, otherCfg)
	attachCheck(t, s.Trust(skiRemoteA, true))
	attachCheck(t, s.Connect(skiRemoteA, true))
	attachCheck(t, s.Select(skiRemoteA, []Pair{pair}))
	if len(peer.subscribes) != 1 || len(otherPeer.subscribes) != 0 {
		t.Fatal("selection did not send exactly one subscribe on its own transport")
	}
	assertAttachedRecords(t, s, nil) // The driver has not reported a result yet.
	attachCheck(t, s.NoteResult(skiRemoteA, peer.lastCounter(), true))
	want := []Record{{Peer: skiRemoteA, Role: RoleLocalClient, Pair: pair, ID: 1}}
	assertAttachedRecords(t, s, want)
	assertAttachedRecords(t, other, nil)
	peer.retain(pair)

	// A notification delivered by the process-wide bus must not enter the
	// attached session. Only the notification forwarded by its driver does.
	notifies := make(chan any, 2)
	s.SetNotifyHandler(func(_ string, _ Pair, _ model.FunctionType, data any, _ time.Time) { notifies <- data })
	fn := model.FunctionTypeMeasurementListData
	spine.Events.Publish(spineapi.EventPayload{
		EventType: spineapi.EventTypeDataChange, Ski: skiRemoteA, LocalFeature: device.NodeManagement(),
		CmdClassifier: util.Ptr(model.CmdClassifierTypeNotify), Function: fn, Data: "global",
	})
	select {
	case data := <-notifies:
		t.Fatalf("attachment accepted process-wide notification %v", data)
	case <-time.After(100 * time.Millisecond):
	}
	at := time.Unix(1, 0)
	if !s.ReceiveNotify(skiRemoteA, pair, fn, "driver", at) {
		t.Fatal("driver notification not accepted")
	}
	select {
	case data := <-notifies:
		if data != "driver" {
			t.Fatalf("received %v, want driver notification", data)
		}
	default:
		t.Fatal("driver notification not delivered")
	}

	attachCheck(t, s.Connect(skiRemoteA, false))
	assertAttachedRecords(t, s, want)
	if s.ReceiveNotify(skiRemoteA, pair, fn, "offline", at) || len(peer.deletes) != 0 {
		t.Fatal("connection loss did not quiesce the pair without deleting it")
	}
	attachCheck(t, s.Connect(skiRemoteA, true))
	if len(peer.subscribes) != 1 {
		t.Fatal("reconnecting duplicated an intact subscribe")
	}
	attachCheck(t, s.Close())
	s = mustAttach(t, cfg)
	assertAttachedRecords(t, s, want)
	attachCheck(t, s.Trust(skiRemoteA, true))
	attachCheck(t, s.Connect(skiRemoteA, true))
	if len(peer.subscribes) != 1 {
		t.Fatal("reopening and reconnecting duplicated an intact subscribe")
	}

	attachCheck(t, other.Close())
	other = mustAttach(t, otherCfg)
	assertAttachedRecords(t, other, nil)
	attachCheck(t, other.Trust(skiRemoteA, true))
	attachCheck(t, other.Connect(skiRemoteA, true))
	attachCheck(t, other.Select(skiRemoteA, []Pair{pair}))
	if len(otherPeer.subscribes) != 1 {
		t.Fatal("the second session did not subscribe independently")
	}
	attachCheck(t, other.NoteResult(skiRemoteA, otherPeer.lastCounter(), true))
	attachCheck(t, s.Trust(skiRemoteA, false))
	assertAttachedRecords(t, s, nil)
	assertAttachedRecords(t, other, want)
	if len(peer.deletes) != 1 || len(otherPeer.deletes) != 0 {
		t.Fatal("revocation did not send a delete on only the revoked session's transport")
	}
	attachCheck(t, s.Close())
	assertAttachedRecords(t, mustAttach(t, cfg), nil)

	attachCheck(t, other.Connect(skiRemoteA, false))
	attachCheck(t, other.Release(skiRemoteA, pair))
	attachCheck(t, other.Close())
	assertAttachedRecords(t, mustAttach(t, otherCfg), nil)
}

func TestAttachKeepsServiceNodeManagementWorking(t *testing.T) {
	pair := measurementPair(47)
	device := newAttachDevice(string(*pair.Client.Device))
	pinned := device.NodeManagement()
	writer := &captureWriter{}
	remote := spine.NewDeviceRemote(device, skiRemoteA, spine.NewSender(writer))
	remote.UpdateDevice(&model.NetworkManagementDeviceDescriptionDataType{
		DeviceAddress: &model.DeviceAddressType{Device: pair.Server.Device},
	})
	device.AddRemoteDeviceForSki(skiRemoteA, remote)
	s := mustAttach(t, AttachConfig{Device: device, StateRoot: t.TempDir()})
	attachCheck(t, s.Trust(skiRemoteA, true))
	attachCheck(t, s.Connect(skiRemoteA, true))
	if device.NodeManagement() != pinned {
		t.Fatal("attachment replaced the service's node management object")
	}
	destination := s.PrimaryNodeManagement()
	var counter model.MsgCounterType
	exchange := func(classifier model.CmdClassifierType, cmd model.CmdType) model.CmdType {
		t.Helper()
		before := len(writer.snapshot())
		counter++
		attachCheck(t, device.ProcessCmd(model.DatagramType{
			Header: model.HeaderType{
				AddressSource: spine.NodeManagementAddress(remote.Address()), AddressDestination: &destination,
				MsgCounter: &counter, CmdClassifier: &classifier,
			},
			Payload: model.PayloadType{Cmd: []model.CmdType{cmd}},
		}, remote))
		sent := writer.snapshot()[before:]
		if len(sent) != 1 {
			t.Fatalf("node management sent %d datagrams, want one reply", len(sent))
		}
		var reply model.Datagram
		attachCheck(t, json.Unmarshal(sent[0], &reply))
		header := reply.Datagram.Header
		if header.MsgCounterReference == nil || *header.MsgCounterReference != counter ||
			header.CmdClassifier == nil || *header.CmdClassifier != model.CmdClassifierTypeReply || len(reply.Datagram.Payload.Cmd) != 1 {
			t.Fatalf("unexpected node management reply: %s", sent[0])
		}
		return reply.Datagram.Payload.Cmd[0]
	}

	// With no custom transport, Connect uses the existing SPINE sender.
	if sent := writer.snapshot(); len(sent) != 1 {
		t.Fatalf("Connect sent %d datagrams, want one subscription-data request", len(sent))
	} else {
		var request model.Datagram
		attachCheck(t, json.Unmarshal(sent[0], &request))
		if cmds := request.Datagram.Payload.Cmd; len(cmds) != 1 || cmds[0].NodeManagementSubscriptionData == nil {
			t.Fatalf("unexpected reconcile request: %s", sent[0])
		}
	}
	// The service's existing native handler still answers its own read.
	useCaseRead := model.CmdType{NodeManagementUseCaseData: &model.NodeManagementUseCaseDataType{}}
	if exchange(model.CmdClassifierTypeRead, useCaseRead).NodeManagementUseCaseData == nil {
		t.Fatal("native node management read was not delegated")
	}
	// The attached handler answers from retained state, not the native manager.
	served := Pair{Client: pair.Server, Server: pair.Client, ServerFeatureType: pair.ServerFeatureType}
	attachCheck(t, s.AcceptRemoteSubscribe(skiRemoteA, served))
	reply := exchange(model.CmdClassifierTypeCall, model.CmdType{NodeManagementSubscriptionData: &model.NodeManagementSubscriptionDataType{}})
	data := reply.NodeManagementSubscriptionData
	if data == nil || len(data.SubscriptionEntry) != 1 ||
		!reflect.DeepEqual(data.SubscriptionEntry[0].ClientAddress, &served.Client) ||
		!reflect.DeepEqual(data.SubscriptionEntry[0].ServerAddress, &served.Server) {
		t.Fatalf("unexpected retained subscription reply: %+v", data)
	}

	attachCheck(t, s.Close())
	if device.FeatureByAddress(&destination) != pinned || device.RemoteDeviceForSki(skiRemoteA) != remote {
		t.Fatal("Close did not restore native dispatch or keep the service's connection")
	}
	if exchange(model.CmdClassifierTypeRead, useCaseRead).NodeManagementUseCaseData == nil {
		t.Fatal("service could not answer its own read after Close")
	}
}

func TestAttachRejectsInvalidInputsBeforeChangingDevice(t *testing.T) {
	device := newAttachDevice("fixture-local")
	destination := spine.NodeManagementAddress(device.Address())
	pinned := device.FeatureByAddress(destination)
	for _, cfg := range []AttachConfig{
		{StateRoot: t.TempDir()},
		{Device: device},
	} {
		if s, err := Attach(cfg); err == nil || s != nil {
			t.Fatalf("Attach returned %v, %v for invalid configuration", s, err)
		}
	}
	if device.FeatureByAddress(destination) != pinned {
		t.Fatal("failed attachment changed the device")
	}
}
