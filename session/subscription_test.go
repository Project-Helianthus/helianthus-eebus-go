package session

import (
	"testing"
	"time"

	"github.com/Project-Helianthus/helianthus-spine-go/model"
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
