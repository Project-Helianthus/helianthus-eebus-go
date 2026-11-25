package session

import (
	"encoding/json"
	"sync"
	"testing"
	"time"

	spineapi "github.com/Project-Helianthus/helianthus-spine-go/api"
	"github.com/Project-Helianthus/helianthus-spine-go/model"
	"github.com/Project-Helianthus/helianthus-spine-go/spine"
)

type sentSubscribe struct {
	pair    Pair
	counter uint64
}

type sentDelete struct {
	del     Delete
	counter uint64
}

type offlinePeer struct {
	next          uint64
	failSubscribe int
	failDelete    int
	replay        bool
	lastSubscribe map[string]uint64
	subscribes    []sentSubscribe
	deletes       []sentDelete
	notifies      int
	held          []Pair
	heldAnswered  bool
}

func (o *offlinePeer) SendSubscribe(_ string, pair Pair) SendResult {
	key := pair.Client.String() + "|" + pair.Server.String()
	if o.lastSubscribe == nil {
		o.lastSubscribe = make(map[string]uint64)
	}
	if o.replay {
		if counter, ok := o.lastSubscribe[key]; ok {
			return SendResult{Counter: counter, Sent: true, Replay: true}
		}
	}
	if o.failSubscribe > 0 {
		o.failSubscribe--
		return SendResult{}
	}
	o.next++
	o.lastSubscribe[key] = o.next
	o.subscribes = append(o.subscribes, sentSubscribe{pair: pair, counter: o.next})
	return SendResult{Counter: o.next, Sent: true}
}

func (o *offlinePeer) SendDelete(_ string, del Delete) SendResult {
	if o.failDelete > 0 {
		o.failDelete--
		return SendResult{}
	}
	o.next++
	o.deletes = append(o.deletes, sentDelete{del: del, counter: o.next})
	return SendResult{Counter: o.next, Sent: true}
}

func (o *offlinePeer) SendNotify(string, model.FeatureAddressType, model.FunctionType, any) error {
	o.notifies++
	return nil
}

func (o *offlinePeer) PeerHeld(string) ([]Pair, bool) {
	return append([]Pair(nil), o.held...), o.heldAnswered
}

func (o *offlinePeer) retain(pairs ...Pair) {
	o.held = append([]Pair(nil), pairs...)
	o.heldAnswered = true
}

func TestSessionRetainsReconcilesAndReleasesSubscriptions(t *testing.T) {
	peer := &offlinePeer{heldAnswered: true}
	root := t.TempDir()
	instance := openAt(t, root, 4730, "replay", peer)
	defer func() { _ = instance.Close() }()

	first := measurementPair(11)
	if err := instance.Trust(skiRemoteA, true); err != nil || instance.Connect(skiRemoteA, true) != nil {
		t.Fatal("trust")
	}
	if err := instance.Select(skiRemoteA, []Pair{first}); err != nil {
		t.Fatal(err)
	}
	if len(peer.subscribes) != 1 {
		t.Fatalf("subscribes = %d", len(peer.subscribes))
	}
	if err := instance.NoteResult(skiRemoteA, peer.subscribes[0].counter, true); err != nil {
		t.Fatal(err)
	}
	peer.retain(first)
	if len(instance.Established()) != 1 {
		t.Fatal("pair was not established")
	}

	notifiedAt := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	if !instance.ReceiveNotify(skiRemoteA, first, model.FunctionTypeMeasurementListData, "value", notifiedAt) {
		t.Fatal("notify was not delivered")
	}
	value, at, ok := instance.LastNotified(skiRemoteA, first, model.FunctionTypeMeasurementListData)
	if !ok || value != "value" || !at.Equal(notifiedAt) {
		t.Fatalf("notified value=%v at=%s ok=%t", value, at, ok)
	}
	if _, _, ok := instance.LastNotified(skiRemoteA, first, model.FunctionTypeMeasurementDescriptionListData); ok {
		t.Fatal("missing function was available")
	}

	if err := instance.Suspend(); err != nil {
		t.Fatal(err)
	}
	before := len(peer.subscribes)
	if err := instance.Run(skiRemoteA); err != nil {
		t.Fatal(err)
	}
	if len(peer.subscribes) != before || instance.Publish(first.Server, model.FunctionTypeMeasurementListData, "x") != 0 {
		t.Fatal("suspended instance exchanged data")
	}
	if instance.ReceiveNotify(skiRemoteA, first, model.FunctionTypeMeasurementListData, "during-suspend", notifiedAt) {
		t.Fatal("suspended instance accepted a notify")
	}
	if len(instance.Established()) != 1 {
		t.Fatal("suspend dropped the retained pair")
	}
	if value, _, ok := instance.LastNotified(skiRemoteA, first, model.FunctionTypeMeasurementListData); !ok || value != "value" {
		t.Fatal("suspend replaced the last notified value")
	}
	if err := instance.Resume(); err != nil {
		t.Fatal(err)
	}
	if len(peer.subscribes) != before {
		t.Fatal("resume subscribed again")
	}

	if err := instance.Connect(skiRemoteA, false); err != nil {
		t.Fatal(err)
	}
	if len(instance.Established()) != 1 {
		t.Fatal("disconnect dropped the retained pair")
	}
	_ = instance.Close()
	instance = openAt(t, root, 4730, "replay", peer)
	if err := instance.Trust(skiRemoteA, true); err != nil || instance.Connect(skiRemoteA, true) != nil {
		t.Fatal(err)
	}
	if len(instance.Established()) != 1 || len(peer.subscribes) != before {
		t.Fatalf("reopen established=%d subscribes=%d", len(instance.Established()), len(peer.subscribes))
	}

	peer.failDelete = 1
	if err := instance.Select(skiRemoteA, nil); err != nil {
		t.Fatal(err)
	}
	if len(peer.deletes) != 0 || len(instance.Established()) != 1 {
		t.Fatalf("failed delete emitted %d and left %d pairs", len(peer.deletes), len(instance.Established()))
	}
	if err := instance.Select(skiRemoteA, []Pair{first}); err != nil {
		t.Fatal(err)
	}
	if len(peer.subscribes) != before || len(peer.deletes) != 0 || len(instance.Established()) != 1 {
		t.Fatalf("reselect subscribes=%d deletes=%d established=%d", len(peer.subscribes), len(peer.deletes), len(instance.Established()))
	}
	if !instance.ReceiveNotify(skiRemoteA, first, model.FunctionTypeMeasurementListData, "still", notifiedAt) {
		t.Fatal("reselected pair stopped notifying")
	}
	peer.failDelete = 0

	peer.failDelete = 1
	if err := instance.Release(skiRemoteA, first); err != nil {
		t.Fatal(err)
	}
	if len(peer.deletes) != 0 || len(instance.Established()) != 1 {
		t.Fatal("one-shot failed delete removed the pair")
	}
	if err := instance.Run(skiRemoteA); err != nil {
		t.Fatal(err)
	}
	if len(peer.deletes) != 1 || len(instance.Established()) != 0 {
		t.Fatalf("retry deletes=%d established=%d", len(peer.deletes), len(instance.Established()))
	}
	if err := instance.NoteResult(skiRemoteA, peer.deletes[0].counter, false); err != nil {
		t.Fatal(err)
	}
	if len(peer.deletes) != 2 {
		t.Fatalf("negative delete was not sent again: %d", len(peer.deletes))
	}
	if err := instance.NoteResult(skiRemoteA, peer.deletes[1].counter, true); err != nil {
		t.Fatal(err)
	}
	if instance.ReceiveNotify(skiRemoteA, first, model.FunctionTypeMeasurementListData, "after", notifiedAt) {
		t.Fatal("released pair still notified")
	}

	if err := instance.Select(skiRemoteA, []Pair{first}); err != nil {
		t.Fatal(err)
	}
	if err := instance.NoteResult(skiRemoteA, peer.subscribes[len(peer.subscribes)-1].counter, true); err != nil {
		t.Fatal(err)
	}
	peer.retain(first)
	if err := instance.Connect(skiRemoteA, false); err != nil {
		t.Fatal(err)
	}
	if err := instance.Release(skiRemoteA, first); err != nil {
		t.Fatal(err)
	}
	if len(instance.Established()) != 0 {
		t.Fatal("unavailable release kept the pair")
	}
	_ = instance.Close()
	instance = openAt(t, root, 4730, "replay", peer)
	if err := instance.Trust(skiRemoteA, true); err != nil || instance.Connect(skiRemoteA, true) != nil {
		t.Fatal(err)
	}
	if len(instance.Established()) != 0 {
		t.Fatal("unavailable release was restored")
	}

	server := localServerPair(1, 1, 1, 21)
	if err := instance.AcceptRemoteSubscribe(skiRemoteA, server); err != nil {
		t.Fatal(err)
	}
	peer.notifies = 0
	_ = instance.Close()
	instance = openAt(t, root, 4730, "replay", peer)
	subscribesBefore := len(peer.subscribes)
	if err := instance.Trust(skiRemoteA, true); err != nil || instance.Connect(skiRemoteA, true) != nil {
		t.Fatal(err)
	}
	if len(peer.subscribes) != subscribesBefore {
		t.Fatal("reconnect asked the remote client to subscribe again")
	}
	if instance.Publish(server.Server, model.FunctionTypeMeasurementListData, "server") != 1 || peer.notifies != 1 {
		t.Fatalf("server notify count publishes, peer=%d", peer.notifies)
	}
	if err := instance.ApplyRemoteDelete(skiRemoteA, Delete{Client: server.Client, Server: server.Server}); err != nil {
		t.Fatal(err)
	}
	_ = instance.Close()
	instance = openAt(t, root, 4730, "replay", peer)
	if err := instance.Trust(skiRemoteA, true); err != nil || instance.Connect(skiRemoteA, true) != nil {
		t.Fatal(err)
	}
	if instance.Publish(server.Server, model.FunctionTypeMeasurementListData, "gone") != 0 {
		t.Fatal("deleted server pair notified after reopen")
	}

	client := measurementPair(31)
	if err := instance.Select(skiRemoteA, []Pair{client}); err != nil {
		t.Fatal(err)
	}
	if err := instance.NoteResult(skiRemoteA, peer.lastCounter(), true); err != nil {
		t.Fatal(err)
	}
	if err := instance.ApplyRemoteDelete(skiRemoteA, Delete{Client: client.Client, Server: client.Server}); err != nil {
		t.Fatal(err)
	}
	subscribesBefore = len(peer.subscribes)
	_ = instance.Close()
	instance = openAt(t, root, 4730, "replay", peer)
	if err := instance.Trust(skiRemoteA, true); err != nil || instance.Connect(skiRemoteA, true) != nil {
		t.Fatal(err)
	}
	if len(instance.Established()) != 0 || len(peer.subscribes) != subscribesBefore {
		t.Fatal("remote server delete was reconciled")
	}

	kept := localServerPair(1, 1, 2, 41)
	insideA := localServerPair(1, 2, 1, 42)
	insideB := localServerPair(1, 3, 1, 43)
	for _, pair := range []Pair{kept, insideA, insideB} {
		if err := instance.AcceptRemoteSubscribe(skiRemoteA, pair); err != nil {
			t.Fatal(err)
		}
	}
	entityDelete := Delete{
		Client: featureAddress("d:_n:test_remote-a", 1, 0),
		Server: featureAddress("d:_n:test_local-a", 1, 0),
	}
	entityDelete.Client.Feature = nil
	entityDelete.Server.Feature = nil
	if err := instance.ApplyRemoteDelete(skiRemoteA, entityDelete); err != nil {
		t.Fatal(err)
	}
	_ = instance.Close()
	instance = openAt(t, root, 4730, "replay", peer)
	if err := instance.Trust(skiRemoteA, true); err != nil || instance.Connect(skiRemoteA, true) != nil {
		t.Fatal(err)
	}
	if instance.Publish(insideA.Server, model.FunctionTypeMeasurementListData, "scoped") != 0 {
		t.Fatal("entity delete left an inside pair")
	}
	if instance.Publish(kept.Server, model.FunctionTypeMeasurementListData, "outside") != 1 {
		t.Fatal("entity delete removed the pair outside the entity")
	}
	deviceDelete := Delete{
		Client: featureAddress("d:_n:test_remote-a", 0, 0),
		Server: featureAddress("d:_n:test_local-a", 0, 0),
	}
	deviceDelete.Client.Entity = nil
	deviceDelete.Client.Feature = nil
	deviceDelete.Server.Entity = nil
	deviceDelete.Server.Feature = nil
	if err := instance.ApplyRemoteDelete(skiRemoteA, deviceDelete); err != nil {
		t.Fatal(err)
	}
	if instance.Publish(kept.Server, model.FunctionTypeMeasurementListData, "device") != 0 {
		t.Fatal("device delete left the remaining pair")
	}

	rejected := measurementPair(51)
	if err := instance.Select(skiRemoteA, []Pair{rejected}); err != nil {
		t.Fatal(err)
	}
	rejectedCounter := peer.lastCounter()
	if err := instance.NoteResult(skiRemoteA, rejectedCounter, false); err != nil {
		t.Fatal(err)
	}
	subscribesBefore = len(peer.subscribes)
	_ = instance.Close()
	instance = openAt(t, root, 4730, "replay", peer)
	if err := instance.Trust(skiRemoteA, true); err != nil || instance.Connect(skiRemoteA, true) != nil {
		t.Fatal(err)
	}
	if len(instance.Established()) != 0 || len(peer.subscribes) != subscribesBefore {
		t.Fatal("rejected pair was restored")
	}
	if err := instance.Select(skiRemoteA, []Pair{rejected}); err != nil {
		t.Fatal(err)
	}
	if len(peer.subscribes) != subscribesBefore+1 || peer.lastCounter() == rejectedCounter {
		t.Fatal("next discovery did not request the rejected pair")
	}

	pending := measurementPair(61)
	if err := instance.Select(skiRemoteA, []Pair{pending}); err != nil {
		t.Fatal(err)
	}
	original := peer.lastCounter()
	peer.replay = true
	if err := instance.Select(skiRemoteA, nil); err != nil {
		t.Fatal(err)
	}
	if len(peer.deletes) == 0 {
		t.Fatal("pending release sent no delete")
	}
	deleteCount := len(peer.deletes)
	subscribeCount := len(peer.subscribes)
	if err := instance.Select(skiRemoteA, []Pair{pending}); err != nil {
		t.Fatal(err)
	}
	if len(peer.subscribes) != subscribeCount || len(instance.Established()) != 0 {
		t.Fatalf("replay established a pair subscribes=%d established=%d", len(peer.subscribes), len(instance.Established()))
	}
	if err := instance.NoteResult(skiRemoteA, original, true); err != nil {
		t.Fatal(err)
	}
	if err := instance.NoteResult(skiRemoteA, original, true); err != nil {
		t.Fatal(err)
	}
	if len(instance.Established()) != 0 {
		t.Fatal("late result restored the cancelled pair")
	}
	peer.replay = false
	if err := instance.Run(skiRemoteA); err != nil {
		t.Fatal(err)
	}
	if len(peer.subscribes) != subscribeCount+1 || len(peer.deletes) != deleteCount {
		t.Fatalf("fresh subscribe subscribes=%d deletes=%d", len(peer.subscribes), len(peer.deletes))
	}

	if err := instance.NoteResult(skiRemoteA, peer.lastCounter(), true); err != nil {
		t.Fatal(err)
	}
	readServer := localServerPair(4, 4, 4, 44)
	if err := instance.AcceptRemoteSubscribe(skiRemoteA, readServer); err != nil {
		t.Fatal(err)
	}
	if err := instance.Trust(skiRemoteB, true); err != nil || instance.Connect(skiRemoteB, true) != nil {
		t.Fatal(err)
	}
	other := localServerPair(5, 5, 5, 55)
	other.Client = featureAddress("d:_n:test_remote-b", 5, 5)
	if err := instance.AcceptRemoteSubscribe(skiRemoteB, other); err != nil {
		t.Fatal(err)
	}
	_ = instance.Close()
	instance = openAt(t, root, 4730, "replay", peer)
	if err := instance.Trust(skiRemoteA, true); err != nil || instance.Connect(skiRemoteA, true) != nil {
		t.Fatal(err)
	}
	unanswered := measurementPair(62)
	if err := instance.Select(skiRemoteA, []Pair{pending, unanswered}); err != nil {
		t.Fatal(err)
	}
	entries, err := instance.SubscriptionData(skiRemoteA, instance.PrimaryNodeManagement())
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 2 {
		t.Fatalf("subscription data = %d, want the requesting peer's two established roles", len(entries))
	}
	if err := instance.Release(skiRemoteA, pending); err != nil {
		t.Fatal(err)
	}
	if len(peer.deletes) == deleteCount {
		t.Fatal("release sent no delete")
	}
	if err := instance.NoteResult(skiRemoteA, peer.deletes[len(peer.deletes)-1].counter, true); err != nil {
		t.Fatal(err)
	}
	entries, err = instance.SubscriptionData(skiRemoteA, instance.PrimaryNodeManagement())
	if err != nil || len(entries) != 1 {
		t.Fatalf("subscription data after release = %d err=%v", len(entries), err)
	}
	wrong := instance.PrimaryNodeManagement()
	wrong.Feature = nil
	if _, err := instance.SubscriptionData(skiRemoteA, wrong); err == nil {
		t.Fatal("read of a non-primary destination was answered")
	}

	owned := localServerPair(6, 6, 6, 66)
	if err := instance.AcceptRemoteSubscribe(skiRemoteA, owned); err != nil {
		t.Fatal(err)
	}
	notifiesBefore := peer.notifies
	deletesBefore := len(peer.deletes)
	if err := instance.ReleaseServer(skiRemoteA, owned); err != nil {
		t.Fatal(err)
	}
	if len(peer.deletes) != deletesBefore+1 {
		t.Fatal("local server release did not send a delete")
	}
	if err := instance.NoteResult(skiRemoteA, peer.deletes[len(peer.deletes)-1].counter, true); err != nil {
		t.Fatal(err)
	}
	if instance.Publish(owned.Server, model.FunctionTypeMeasurementListData, "released") != 0 || peer.notifies != notifiesBefore {
		t.Fatal("released local server pair still notified")
	}
	retired := localServerPair(7, 7, 7, 77)
	if err := instance.AcceptRemoteSubscribe(skiRemoteA, retired); err != nil {
		t.Fatal(err)
	}
	if err := instance.Connect(skiRemoteA, false); err != nil {
		t.Fatal(err)
	}
	if err := instance.ReleaseServer(skiRemoteA, retired); err != nil {
		t.Fatal(err)
	}
	_ = instance.Close()
	instance = openAt(t, root, 4730, "replay", peer)
	if err := instance.Trust(skiRemoteA, true); err != nil || instance.Connect(skiRemoteA, true) != nil {
		t.Fatal(err)
	}
	if instance.Publish(retired.Server, model.FunctionTypeMeasurementListData, "retired") != 0 {
		t.Fatal("unavailable server release was reconciled")
	}

	lost := measurementPair(71)
	intact := measurementPair(72)
	if err := instance.Select(skiRemoteA, []Pair{lost, intact}); err != nil {
		t.Fatal(err)
	}
	if err := instance.NoteResult(skiRemoteA, peer.subscribes[len(peer.subscribes)-2].counter, true); err != nil {
		t.Fatal(err)
	}
	if err := instance.NoteResult(skiRemoteA, peer.subscribes[len(peer.subscribes)-1].counter, true); err != nil {
		t.Fatal(err)
	}
	peer.retain(intact)
	subscribesBefore = len(peer.subscribes)
	if err := instance.Connect(skiRemoteA, false); err != nil || instance.Connect(skiRemoteA, true) != nil {
		t.Fatal(err)
	}
	if len(peer.subscribes) != subscribesBefore+1 {
		t.Fatalf("lost pair renewals = %d", len(peer.subscribes)-subscribesBefore)
	}
	if peer.subscribes[len(peer.subscribes)-1].pair.Server.String() != lost.Server.String() {
		t.Fatal("renewed the intact pair")
	}
	if err := instance.NoteResult(skiRemoteA, peer.lastCounter(), true); err != nil {
		t.Fatal(err)
	}
	if !instance.ReceiveNotify(skiRemoteA, lost, model.FunctionTypeMeasurementListData, "resumed", notifiedAt) {
		t.Fatal("renewed pair did not notify")
	}
}

func TestTrustRevocationRemovesPairs(t *testing.T) {
	peer := &offlinePeer{heldAnswered: true}
	root := t.TempDir()
	instance := openAt(t, root, 4733, "revoke", peer)
	pair := measurementPair(91)
	if err := instance.Trust(skiRemoteA, true); err != nil || instance.Connect(skiRemoteA, true) != nil {
		t.Fatal(err)
	}
	if err := instance.Select(skiRemoteA, []Pair{pair}); err != nil {
		t.Fatal(err)
	}
	if err := instance.NoteResult(skiRemoteA, peer.lastCounter(), true); err != nil {
		t.Fatal(err)
	}
	if err := instance.Trust(skiRemoteA, false); err != nil {
		t.Fatal(err)
	}
	if len(peer.deletes) != 1 || len(instance.Established()) != 0 {
		t.Fatalf("revocation deletes=%d established=%d", len(peer.deletes), len(instance.Established()))
	}
	if instance.ReceiveNotify(skiRemoteA, pair, model.FunctionTypeMeasurementListData, "revoked", time.Unix(1, 0).UTC()) {
		t.Fatal("revoked pair still notified")
	}
	_ = instance.Close()
	instance = openAt(t, root, 4733, "revoke", peer)
	t.Cleanup(func() { _ = instance.Close() })
	subscribes := len(peer.subscribes)
	if err := instance.Trust(skiRemoteA, true); err != nil || instance.Connect(skiRemoteA, true) != nil {
		t.Fatal(err)
	}
	if len(instance.Established()) != 0 || len(peer.subscribes) != subscribes {
		t.Fatal("revoked pair was restored")
	}
}

func (o *offlinePeer) lastCounter() uint64 {
	if len(o.subscribes) == 0 {
		return 0
	}
	return o.subscribes[len(o.subscribes)-1].counter
}

func localServerPair(remoteEntity, remoteFeature, localEntity, localFeature uint) Pair {
	return Pair{
		Client:            featureAddress("d:_n:test_remote-a", remoteEntity, remoteFeature),
		Server:            featureAddress("d:_n:test_local-a", localEntity, localFeature),
		ServerFeatureType: model.FeatureTypeTypeMeasurement,
	}
}

func TestLastNotifiedKeyVariesEveryField(t *testing.T) {
	peer := &offlinePeer{heldAnswered: true}
	instance := openAt(t, t.TempDir(), 4731, "cache", peer)
	t.Cleanup(func() { _ = instance.Close() })
	base := Pair{
		Client:            featureAddress("d:_n:test_local-a", 1, 2),
		Server:            featureAddress("d:_n:test_remote-a", 3, 4),
		ServerFeatureType: model.FeatureTypeTypeMeasurement,
	}
	if err := instance.Trust(skiRemoteA, true); err != nil || instance.Connect(skiRemoteA, true) != nil {
		t.Fatal(err)
	}
	if err := instance.Select(skiRemoteA, []Pair{base}); err != nil {
		t.Fatal(err)
	}
	if err := instance.NoteResult(skiRemoteA, peer.lastCounter(), true); err != nil {
		t.Fatal(err)
	}
	at := time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC)
	if !instance.ReceiveNotify(skiRemoteA, base, model.FunctionTypeMeasurementListData, "base", at) {
		t.Fatal("base notify")
	}
	mutations := []struct {
		name   string
		ski    string
		pair   Pair
		change func(*Pair)
	}{
		{name: "peer", ski: skiRemoteB, pair: base},
		{name: "client-device", ski: skiRemoteA, pair: base, change: func(p *Pair) { p.Client = featureAddress("d:_n:test_local-b", 1, 2) }},
		{name: "client-entity", ski: skiRemoteA, pair: base, change: func(p *Pair) { p.Client = featureAddress("d:_n:test_local-a", 9, 2) }},
		{name: "client-feature", ski: skiRemoteA, pair: base, change: func(p *Pair) { p.Client = featureAddress("d:_n:test_local-a", 1, 9) }},
		{name: "server-device", ski: skiRemoteA, pair: base, change: func(p *Pair) { p.Server = featureAddress("d:_n:test_remote-b", 3, 4) }},
		{name: "server-entity", ski: skiRemoteA, pair: base, change: func(p *Pair) { p.Server = featureAddress("d:_n:test_remote-a", 8, 4) }},
		{name: "server-feature", ski: skiRemoteA, pair: base, change: func(p *Pair) { p.Server = featureAddress("d:_n:test_remote-a", 3, 8) }},
	}
	for _, mutation := range mutations {
		pair := mutation.pair
		if mutation.change != nil {
			mutation.change(&pair)
		}
		if _, _, ok := instance.LastNotified(mutation.ski, pair, model.FunctionTypeMeasurementListData); ok {
			t.Fatalf("%s still matched the base notify", mutation.name)
		}
	}
	if _, _, ok := instance.LastNotified(skiRemoteA, base, model.FunctionTypeLoadControlLimitListData); ok {
		t.Fatal("function still matched the base notify")
	}
}

func TestConnectionGoneStopsDeleteRetry(t *testing.T) {
	peer := &offlinePeer{heldAnswered: true}
	root := t.TempDir()
	instance := openAt(t, root, 4732, "gone", peer)
	t.Cleanup(func() { _ = instance.Close() })
	pair := measurementPair(81)
	if err := instance.Trust(skiRemoteA, true); err != nil || instance.Connect(skiRemoteA, true) != nil {
		t.Fatal(err)
	}
	if err := instance.Select(skiRemoteA, []Pair{pair}); err != nil {
		t.Fatal(err)
	}
	if err := instance.NoteResult(skiRemoteA, peer.lastCounter(), true); err != nil {
		t.Fatal(err)
	}
	peer.failDelete = 1
	if err := instance.Release(skiRemoteA, pair); err != nil {
		t.Fatal(err)
	}
	if len(peer.deletes) != 0 {
		t.Fatal("delete left the host")
	}
	if err := instance.Connect(skiRemoteA, false); err != nil {
		t.Fatal(err)
	}
	peer.failDelete = 0
	if err := instance.Run(skiRemoteA); err != nil {
		t.Fatal(err)
	}
	if len(peer.deletes) != 0 || len(instance.Established()) != 0 {
		t.Fatalf("retry after disconnect deletes=%d established=%d", len(peer.deletes), len(instance.Established()))
	}
}

// coincidentCounters numbers datagrams per peer. Two remotes both start at 1.
type coincidentCounters struct {
	next       map[string]uint64
	subscribes map[string][]sentSubscribe
	deletes    map[string][]sentDelete
}

func newCoincidentCounters() *coincidentCounters {
	return &coincidentCounters{
		next:       map[string]uint64{},
		subscribes: map[string][]sentSubscribe{},
		deletes:    map[string][]sentDelete{},
	}
}

func (c *coincidentCounters) SendSubscribe(peer string, pair Pair) SendResult {
	counter := c.bump(peer)
	c.subscribes[peer] = append(c.subscribes[peer], sentSubscribe{pair: pair, counter: counter})
	return SendResult{Counter: counter, Sent: true}
}

func (c *coincidentCounters) SendDelete(peer string, del Delete) SendResult {
	counter := c.bump(peer)
	c.deletes[peer] = append(c.deletes[peer], sentDelete{del: del, counter: counter})
	return SendResult{Counter: counter, Sent: true}
}

func (c *coincidentCounters) SendNotify(string, model.FeatureAddressType, model.FunctionType, any) error {
	return nil
}

func (c *coincidentCounters) PeerHeld(string) ([]Pair, bool) {
	return nil, false
}

func (c *coincidentCounters) bump(peer string) uint64 {
	c.next[peer]++
	return c.next[peer]
}

func TestCoincidentPeerCountersDoNotCollide(t *testing.T) {
	pair := measurementPair(11)
	t.Run("subscribe", func(t *testing.T) {
		transport := newCoincidentCounters()
		root := t.TempDir()
		instance := openAt(t, root, 4741, "counters", transport)
		trustAndConnect(t, instance, skiRemoteA, skiRemoteB)
		if err := instance.Select(skiRemoteA, []Pair{pair}); err != nil {
			t.Fatal(err)
		}
		if err := instance.Select(skiRemoteB, []Pair{pair}); err != nil {
			t.Fatal(err)
		}
		counter := coincidingSubscribe(t, transport)
		if err := instance.NoteResult(skiRemoteA, counter, true); err != nil {
			t.Fatal(err)
		}
		if err := instance.NoteResult(skiRemoteB, counter, true); err != nil {
			t.Fatal(err)
		}
		if establishedCount(instance, skiRemoteA) != 1 || establishedCount(instance, skiRemoteB) != 1 {
			t.Fatalf("established = %+v", instance.Established())
		}
		if err := instance.Close(); err != nil {
			t.Fatal(err)
		}
		reopened := openAt(t, root, 4741, "counters", transport)
		t.Cleanup(func() { _ = reopened.Close() })
		if establishedCount(reopened, skiRemoteA) != 1 || establishedCount(reopened, skiRemoteB) != 1 {
			t.Fatalf("reopen established = %+v", reopened.Established())
		}
	})

	t.Run("cancelled", func(t *testing.T) {
		transport := newCoincidentCounters()
		instance := openAt(t, t.TempDir(), 4742, "cancelled", transport)
		t.Cleanup(func() { _ = instance.Close() })
		trustAndConnect(t, instance, skiRemoteA, skiRemoteB)
		if err := instance.Select(skiRemoteA, []Pair{pair}); err != nil {
			t.Fatal(err)
		}
		if err := instance.Select(skiRemoteA, nil); err != nil {
			t.Fatal(err)
		}
		if err := instance.Select(skiRemoteB, []Pair{pair}); err != nil {
			t.Fatal(err)
		}
		counter := coincidingSubscribe(t, transport)
		if err := instance.NoteResult(skiRemoteB, counter, true); err != nil {
			t.Fatal(err)
		}
		if establishedCount(instance, skiRemoteB) != 1 {
			t.Fatal("cancelled counter of the first peer swallowed the second peer's result")
		}
		if err := instance.NoteResult(skiRemoteA, counter, true); err != nil {
			t.Fatal(err)
		}
		if establishedCount(instance, skiRemoteA) != 0 || establishedCount(instance, skiRemoteB) != 1 {
			t.Fatalf("late result established = %+v", instance.Established())
		}
	})

	t.Run("release", func(t *testing.T) {
		transport := newCoincidentCounters()
		instance := openAt(t, t.TempDir(), 4743, "release", transport)
		t.Cleanup(func() { _ = instance.Close() })
		trustAndConnect(t, instance, skiRemoteA, skiRemoteB)
		if err := instance.Select(skiRemoteA, []Pair{pair}); err != nil {
			t.Fatal(err)
		}
		if err := instance.Select(skiRemoteB, []Pair{pair}); err != nil {
			t.Fatal(err)
		}
		if err := instance.NoteResult(skiRemoteA, coincidingSubscribe(t, transport), true); err != nil {
			t.Fatal(err)
		}
		if err := instance.NoteResult(skiRemoteB, transport.subscribes[skiRemoteB][0].counter, true); err != nil {
			t.Fatal(err)
		}
		if err := instance.Release(skiRemoteA, pair); err != nil {
			t.Fatal(err)
		}
		if err := instance.Release(skiRemoteB, pair); err != nil {
			t.Fatal(err)
		}
		if len(transport.deletes[skiRemoteA]) != 1 || len(transport.deletes[skiRemoteB]) != 1 {
			t.Fatalf("deletes A=%d B=%d", len(transport.deletes[skiRemoteA]), len(transport.deletes[skiRemoteB]))
		}
		shared := transport.deletes[skiRemoteA][0].counter
		if shared != transport.deletes[skiRemoteB][0].counter {
			t.Fatalf("delete counters A=%d B=%d", shared, transport.deletes[skiRemoteB][0].counter)
		}
		if err := instance.NoteResult(skiRemoteA, shared, false); err != nil {
			t.Fatal(err)
		}
		if len(transport.deletes[skiRemoteA]) != 2 || len(transport.deletes[skiRemoteB]) != 1 {
			t.Fatalf("after first negative deletes A=%d B=%d", len(transport.deletes[skiRemoteA]), len(transport.deletes[skiRemoteB]))
		}
		if err := instance.NoteResult(skiRemoteB, shared, false); err != nil {
			t.Fatal(err)
		}
		if len(transport.deletes[skiRemoteA]) != 2 || len(transport.deletes[skiRemoteB]) != 2 {
			t.Fatalf("after second negative deletes A=%d B=%d", len(transport.deletes[skiRemoteA]), len(transport.deletes[skiRemoteB]))
		}
		if err := instance.NoteResult(skiRemoteA, transport.deletes[skiRemoteA][1].counter, true); err != nil {
			t.Fatal(err)
		}
		if err := instance.NoteResult(skiRemoteB, transport.deletes[skiRemoteB][1].counter, true); err != nil {
			t.Fatal(err)
		}
		if len(instance.Established()) != 0 {
			t.Fatalf("accepted deletes left %+v", instance.Established())
		}
	})
}

type captureWriter struct {
	mu       sync.Mutex
	messages [][]byte
}

func (w *captureWriter) WriteShipMessageWithPayload(message []byte) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.messages = append(w.messages, append([]byte(nil), message...))
}

func (w *captureWriter) snapshot() [][]byte {
	w.mu.Lock()
	defer w.mu.Unlock()
	out := make([][]byte, len(w.messages))
	copy(out, w.messages)
	return out
}

type spineDeleteDatagram struct {
	header model.HeaderType
	del    model.SubscriptionManagementDeleteCallType
}

func spineSubscriptionDeletes(messages [][]byte) []spineDeleteDatagram {
	out := make([]spineDeleteDatagram, 0)
	for _, message := range messages {
		var datagram model.Datagram
		if err := json.Unmarshal(message, &datagram); err != nil {
			continue
		}
		for _, cmd := range datagram.Datagram.Payload.Cmd {
			call := cmd.NodeManagementSubscriptionDeleteCall
			if call == nil || call.SubscriptionDelete == nil {
				continue
			}
			out = append(out, spineDeleteDatagram{
				header: datagram.Datagram.Header,
				del:    *call.SubscriptionDelete,
			})
		}
	}
	return out
}

func TestReleaseServerSendsSpineDelete(t *testing.T) {
	writer := &captureWriter{}
	instance := openAt(t, t.TempDir(), 4755, "server-release", nil)
	t.Cleanup(func() { _ = instance.Close() })
	if err := instance.AttachRemote(skiRemoteA, writer); err != nil {
		t.Fatal(err)
	}
	if err := instance.Trust(skiRemoteA, true); err != nil || instance.Connect(skiRemoteA, true) != nil {
		t.Fatal(err)
	}
	pair := localServerPair(4, 4, 1, 2)
	if err := instance.AcceptRemoteSubscribe(skiRemoteA, pair); err != nil {
		t.Fatal(err)
	}
	before := len(writer.snapshot())
	if err := instance.ReleaseServer(skiRemoteA, pair); err != nil {
		t.Fatal(err)
	}
	deletes := spineSubscriptionDeletes(writer.snapshot()[before:])
	if len(deletes) != 1 {
		t.Fatalf("spine deletes = %d", len(deletes))
	}
	sent := deletes[0]
	if sent.header.CmdClassifier == nil || *sent.header.CmdClassifier != model.CmdClassifierTypeCall {
		t.Fatalf("classifier = %v", sent.header.CmdClassifier)
	}
	if sent.del.ClientAddress == nil || sent.del.ClientAddress.String() != pair.Client.String() {
		t.Fatalf("client = %v", sent.del.ClientAddress)
	}
	if sent.del.ServerAddress == nil || sent.del.ServerAddress.String() != pair.Server.String() {
		t.Fatalf("server = %v", sent.del.ServerAddress)
	}
	source := spine.NodeManagementAddress(&instance.localDevice)
	destination := spine.NodeManagementAddress(pair.Client.Device)
	if sent.header.AddressSource == nil || sent.header.AddressSource.String() != source.String() {
		t.Fatalf("source = %v", sent.header.AddressSource)
	}
	if sent.header.AddressDestination == nil || sent.header.AddressDestination.String() != destination.String() {
		t.Fatalf("destination = %v", sent.header.AddressDestination)
	}
	if len(instance.Established()) != 0 {
		t.Fatalf("established = %+v", instance.Established())
	}
	entries, err := instance.SubscriptionData(skiRemoteA, instance.PrimaryNodeManagement())
	if err != nil || len(entries) != 0 {
		t.Fatalf("subscription data = %d err=%v", len(entries), err)
	}
	if instance.Publish(pair.Server, model.FunctionTypeMeasurementListData, "released") != 0 {
		t.Fatal("released local server pair was still published")
	}
}

func trustAndConnect(t *testing.T, instance *Instance, skis ...string) {
	t.Helper()
	for _, ski := range skis {
		if err := instance.Trust(ski, true); err != nil {
			t.Fatal(err)
		}
		if err := instance.Connect(ski, true); err != nil {
			t.Fatal(err)
		}
	}
}

func coincidingSubscribe(t *testing.T, transport *coincidentCounters) uint64 {
	t.Helper()
	if len(transport.subscribes[skiRemoteA]) == 0 || len(transport.subscribes[skiRemoteB]) == 0 {
		t.Fatal("both peers did not subscribe")
	}
	counter := transport.subscribes[skiRemoteA][0].counter
	if counter != transport.subscribes[skiRemoteB][0].counter {
		t.Fatalf("subscribe counters A=%d B=%d", counter, transport.subscribes[skiRemoteB][0].counter)
	}
	return counter
}

func establishedCount(instance *Instance, peer string) int {
	count := 0
	for _, record := range instance.Established() {
		if record.Peer == peer {
			count++
		}
	}
	return count
}

func TestPendingSubscribeRetriesAfterReconnect(t *testing.T) {
	peer := &offlinePeer{heldAnswered: true}
	instance := openAt(t, t.TempDir(), 4782, "pending-retry", peer)
	t.Cleanup(func() { _ = instance.Close() })
	pair := measurementPair(11)
	if err := instance.Trust(skiRemoteA, true); err != nil || instance.Connect(skiRemoteA, true) != nil {
		t.Fatal(err)
	}
	if err := instance.Select(skiRemoteA, []Pair{pair}); err != nil {
		t.Fatal(err)
	}
	if len(peer.subscribes) != 1 || len(instance.Established()) != 0 {
		t.Fatalf("subscribes=%d established=%d", len(peer.subscribes), len(instance.Established()))
	}
	original := peer.subscribes[0].counter
	if err := instance.Connect(skiRemoteA, false); err != nil || instance.Connect(skiRemoteA, true) != nil {
		t.Fatal(err)
	}
	if err := instance.Select(skiRemoteA, []Pair{pair}); err != nil {
		t.Fatal(err)
	}
	if len(peer.subscribes) != 2 {
		t.Fatalf("pending subscribe never retried after reconnect: %d", len(peer.subscribes))
	}
	if err := instance.NoteResult(skiRemoteA, original, true); err != nil || instance.NoteResult(skiRemoteA, original, true) != nil {
		t.Fatal(err)
	}
	if len(instance.Established()) != 0 {
		t.Fatal("late result established the pair")
	}
	if err := instance.NoteResult(skiRemoteA, peer.subscribes[1].counter, true); err != nil {
		t.Fatal(err)
	}
	if len(instance.Established()) != 1 {
		t.Fatalf("retry was not established: %+v", instance.Established())
	}
}

func TestDefaultPathResolvesResultReconcilesAndAnswers(t *testing.T) {
	writer := &captureWriter{}
	instance := openAt(t, t.TempDir(), 4781, "default-path", nil)
	t.Cleanup(func() { _ = instance.Close() })
	local := instance.LocalDevice()
	entity := local.Entity([]model.AddressEntityType{1})
	if entity == nil {
		t.Fatal("local entity is missing")
	}
	measurement := entity.GetOrAddFeature(model.FeatureTypeTypeMeasurement, model.RoleTypeClient)
	loadControl := entity.GetOrAddFeature(model.FeatureTypeTypeLoadControl, model.RoleTypeClient)
	if measurement.Address() == nil || loadControl.Address() == nil {
		t.Fatal("local client feature has no address")
	}
	if err := instance.AttachRemote(skiRemoteA, writer); err != nil {
		t.Fatal(err)
	}
	remoteDevice := model.AddressDeviceType("d:_n:test_remote-a")
	remote := local.RemoteDeviceForSki(skiRemoteA)
	remote.UpdateDevice(&model.NetworkManagementDeviceDescriptionDataType{
		DeviceAddress: &model.DeviceAddressType{Device: &remoteDevice},
	})
	measurementPair := Pair{
		Client:            *measurement.Address(),
		Server:            featureAddress(string(remoteDevice), 1, 11),
		ServerFeatureType: model.FeatureTypeTypeMeasurement,
	}
	loadPair := Pair{
		Client:            *loadControl.Address(),
		Server:            featureAddress(string(remoteDevice), 1, 12),
		ServerFeatureType: model.FeatureTypeTypeLoadControl,
	}
	if err := instance.Trust(skiRemoteA, true); err != nil || instance.Connect(skiRemoteA, true) != nil {
		t.Fatal(err)
	}
	if err := instance.Select(skiRemoteA, []Pair{measurementPair, loadPair}); err != nil {
		t.Fatal(err)
	}
	subscribes := subscriptionRequests(writer.snapshot())
	measurementCounter, ok := subscribeCounter(subscribes, measurementPair)
	if !ok || len(subscribes) != 2 {
		t.Fatalf("subscribes=%d measurement=%t", len(subscribes), ok)
	}
	source := spine.NodeManagementAddress(&remoteDevice)
	destination := instance.PrimaryNodeManagement()
	feedSpine(t, remote, model.HeaderType{
		AddressSource:       source,
		AddressDestination:  &destination,
		MsgCounter:          counterPtr(9001),
		MsgCounterReference: counterPtr(measurementCounter),
		CmdClassifier:       classifierPtr(model.CmdClassifierTypeResult),
	}, model.CmdType{ResultData: &model.ResultDataType{ErrorNumber: errorNumberPtr(model.ErrorNumberTypeNoError)}})
	waitForPair(t, instance, skiRemoteA, measurementPair)
	if establishedCount(instance, skiRemoteA) != 1 {
		t.Fatalf("pending pair was established: %+v", instance.Established())
	}

	localDevice := local.Address()
	serverPair := Pair{
		Client:            featureAddress(string(remoteDevice), 1, 21),
		Server:            featureAddress(string(*localDevice), 1, 2),
		ServerFeatureType: model.FeatureTypeTypeMeasurement,
	}
	instance.HandleEvent(spineapi.EventPayload{
		Ski:        skiRemoteA,
		EventType:  spineapi.EventTypeSubscriptionChange,
		ChangeType: spineapi.ElementChangeAdd,
		Data: model.SubscriptionManagementRequestCallType{
			ClientAddress:     &serverPair.Client,
			ServerAddress:     &serverPair.Server,
			ServerFeatureType: &serverPair.ServerFeatureType,
		},
	})
	waitForPair(t, instance, skiRemoteA, serverPair)
	if err := instance.Trust(skiRemoteB, true); err != nil || instance.Connect(skiRemoteB, true) != nil {
		t.Fatal(err)
	}
	other := Pair{
		Client:            featureAddress("d:_n:test_remote-b", 1, 21),
		Server:            featureAddress(string(*localDevice), 1, 3),
		ServerFeatureType: model.FeatureTypeTypeMeasurement,
	}
	if err := instance.AcceptRemoteSubscribe(skiRemoteB, other); err != nil {
		t.Fatal(err)
	}

	beforeReply := len(writer.snapshot())
	feedSpine(t, remote, model.HeaderType{
		AddressSource:      source,
		AddressDestination: &destination,
		MsgCounter:         counterPtr(9002),
		CmdClassifier:      classifierPtr(model.CmdClassifierTypeCall),
	}, model.CmdType{NodeManagementSubscriptionData: &model.NodeManagementSubscriptionDataType{}})
	replies := subscriptionReplies(writer.snapshot()[beforeReply:])
	if len(replies) != 1 {
		t.Fatalf("subscription replies=%d", len(replies))
	}
	entries := replies[0].SubscriptionEntry
	if len(entries) != 2 || !entryHas(entries, measurementPair) || !entryHas(entries, serverPair) || entryHas(entries, loadPair) || entryHas(entries, other) {
		t.Fatalf("subscription data = %+v", entries)
	}

	readCounter, ok := subscriptionReadCounter(writer.snapshot())
	if !ok {
		t.Fatal("subscription data was not read from the peer")
	}
	beforeRenew := subscriptionRequests(writer.snapshot())
	feedSpine(t, remote, model.HeaderType{
		AddressSource:       source,
		AddressDestination:  &destination,
		MsgCounter:          counterPtr(9003),
		MsgCounterReference: counterPtr(readCounter),
		CmdClassifier:       classifierPtr(model.CmdClassifierTypeReply),
	}, model.CmdType{NodeManagementSubscriptionData: &model.NodeManagementSubscriptionDataType{}})
	renewed := subscriptionRequests(writer.snapshot())
	if len(renewed) != len(beforeRenew)+1 {
		t.Fatalf("lost pair renewals=%d", len(renewed)-len(beforeRenew))
	}
	if renewed[len(renewed)-1].server != measurementPair.Server.String() {
		t.Fatalf("renewed %s", renewed[len(renewed)-1].server)
	}

	feedSpine(t, remote, model.HeaderType{
		AddressSource:      source,
		AddressDestination: &destination,
		MsgCounter:         counterPtr(9004),
		CmdClassifier:      classifierPtr(model.CmdClassifierTypeCall),
	}, model.CmdType{NodeManagementSubscriptionDeleteCall: &model.NodeManagementSubscriptionDeleteCallType{
		SubscriptionDelete: &model.SubscriptionManagementDeleteCallType{
			ClientAddress: &measurementPair.Client,
			ServerAddress: &measurementPair.Server,
		},
	}})
	if pairEstablished(instance, skiRemoteA, measurementPair) || !pairEstablished(instance, skiRemoteA, serverPair) || !pairEstablished(instance, skiRemoteB, other) {
		t.Fatalf("after delete %+v", instance.Established())
	}
}

func waitForPair(t *testing.T, instance *Instance, peer string, pair Pair) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if pairEstablished(instance, peer, pair) {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("pair %s was not established: %+v", pair.Server.String(), instance.Established())
}

func pairEstablished(instance *Instance, peer string, pair Pair) bool {
	for _, record := range instance.Established() {
		if record.Peer == peer && sameAddress(record.Pair.Client, pair.Client) && sameAddress(record.Pair.Server, pair.Server) {
			return true
		}
	}
	return false
}

func feedSpine(t *testing.T, remote spineapi.DeviceRemoteInterface, header model.HeaderType, cmd model.CmdType) {
	t.Helper()
	buf, err := json.Marshal(model.Datagram{Datagram: model.DatagramType{
		Header:  header,
		Payload: model.PayloadType{Cmd: []model.CmdType{cmd}},
	}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := remote.HandleSpineMesssage(buf); err != nil {
		t.Fatalf("spine: %v", err)
	}
}

type sentSubscription struct {
	counter uint64
	server  string
}

func subscriptionRequests(messages [][]byte) []sentSubscription {
	return subscriptionCalls(messages, func(cmd model.CmdType) *model.FeatureAddressType {
		call := cmd.NodeManagementSubscriptionRequestCall
		if call == nil || call.SubscriptionRequest == nil {
			return nil
		}
		return call.SubscriptionRequest.ServerAddress
	})
}

func subscriptionReadCounter(messages [][]byte) (uint64, bool) {
	for _, message := range messages {
		var datagram model.Datagram
		if json.Unmarshal(message, &datagram) != nil || datagram.Datagram.Header.CmdClassifier == nil || datagram.Datagram.Header.MsgCounter == nil {
			continue
		}
		if *datagram.Datagram.Header.CmdClassifier != model.CmdClassifierTypeCall || len(datagram.Datagram.Payload.Cmd) == 0 {
			continue
		}
		if datagram.Datagram.Payload.Cmd[0].NodeManagementSubscriptionData == nil {
			continue
		}
		return uint64(*datagram.Datagram.Header.MsgCounter), true
	}
	return 0, false
}

func subscriptionReplies(messages [][]byte) []*model.NodeManagementSubscriptionDataType {
	out := make([]*model.NodeManagementSubscriptionDataType, 0)
	for _, message := range messages {
		var datagram model.Datagram
		if json.Unmarshal(message, &datagram) != nil || datagram.Datagram.Header.CmdClassifier == nil {
			continue
		}
		if *datagram.Datagram.Header.CmdClassifier != model.CmdClassifierTypeReply || len(datagram.Datagram.Payload.Cmd) == 0 {
			continue
		}
		data := datagram.Datagram.Payload.Cmd[0].NodeManagementSubscriptionData
		if data == nil {
			continue
		}
		out = append(out, data)
	}
	return out
}

func subscriptionCalls(messages [][]byte, server func(model.CmdType) *model.FeatureAddressType) []sentSubscription {
	out := make([]sentSubscription, 0)
	for _, message := range messages {
		var datagram model.Datagram
		if json.Unmarshal(message, &datagram) != nil || datagram.Datagram.Header.MsgCounter == nil {
			continue
		}
		if len(datagram.Datagram.Payload.Cmd) == 0 {
			continue
		}
		address := server(datagram.Datagram.Payload.Cmd[0])
		if address == nil {
			continue
		}
		out = append(out, sentSubscription{counter: uint64(*datagram.Datagram.Header.MsgCounter), server: address.String()})
	}
	return out
}

func subscribeCounter(requests []sentSubscription, pair Pair) (uint64, bool) {
	for _, request := range requests {
		if request.server == pair.Server.String() {
			return request.counter, true
		}
	}
	return 0, false
}

func entryHas(entries []model.SubscriptionManagementEntryDataType, pair Pair) bool {
	for _, entry := range entries {
		if entry.ClientAddress != nil && entry.ServerAddress != nil &&
			entry.ClientAddress.String() == pair.Client.String() && entry.ServerAddress.String() == pair.Server.String() {
			return true
		}
	}
	return false
}

func counterPtr(value uint64) *model.MsgCounterType {
	counter := model.MsgCounterType(value)
	return &counter
}

func classifierPtr(value model.CmdClassifierType) *model.CmdClassifierType {
	return &value
}

func errorNumberPtr(value model.ErrorNumberType) *model.ErrorNumberType {
	return &value
}
