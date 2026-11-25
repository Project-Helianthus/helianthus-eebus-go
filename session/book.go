package session

import (
	"reflect"

	"github.com/Project-Helianthus/helianthus-spine-go/model"
	"github.com/Project-Helianthus/helianthus-spine-go/util"
)

type entry struct {
	peer             string
	role             Role
	pair             Pair
	id               uint
	established      bool
	wanted           bool
	pending          *uint64
	releaseQueued    bool
	releaseSent      bool
	releaseCounter   *uint64
	renewCounter     *uint64
	lastSubscribe    *uint64
	lastWasSubscribe bool
}

// peerCounter is one datagram number on one peer. Each remote numbers its own
// datagrams, so the same counter on two peers is two different datagrams.
type peerCounter struct {
	peer    string
	counter uint64
}

func counterKey(peer string, counter uint64) peerCounter {
	return peerCounter{peer: peer, counter: counter}
}

type book struct {
	nextID    uint
	byKey     map[string]*entry
	byCounter map[peerCounter]string
	cancelled map[peerCounter]struct{}
	releaseOf map[peerCounter]string
}

func newBook() *book {
	return &book{
		byKey:     make(map[string]*entry),
		byCounter: make(map[peerCounter]string),
		cancelled: make(map[peerCounter]struct{}),
		releaseOf: make(map[peerCounter]string),
	}
}

func keyOf(peer string, role Role, pair Pair) string {
	return peer + "\x00" + string(role) + "\x00" + pair.Client.String() + "\x00" + pair.Server.String()
}

func (b *book) get(peer string, role Role, pair Pair) *entry {
	return b.byKey[keyOf(peer, role, pair)]
}

func (b *book) ensure(peer string, role Role, pair Pair) *entry {
	key := keyOf(peer, role, pair)
	if current := b.byKey[key]; current != nil {
		current.pair = clonePair(pair)
		return current
	}
	b.nextID++
	created := &entry{
		peer: peer,
		role: role,
		pair: clonePair(pair),
		id:   b.nextID,
	}
	b.byKey[key] = created
	return created
}

func (b *book) drop(item *entry) {
	if item == nil {
		return
	}
	if item.pending != nil {
		delete(b.byCounter, counterKey(item.peer, *item.pending))
	}
	if item.releaseCounter != nil {
		delete(b.releaseOf, counterKey(item.peer, *item.releaseCounter))
	}
	delete(b.byKey, keyOf(item.peer, item.role, item.pair))
}

func (b *book) durable() []storedPair {
	out := make([]storedPair, 0)
	for _, item := range b.byKey {
		if !item.established {
			continue
		}
		out = append(out, storedFromEntry(item.peer, item.role, item.id, item.pair))
	}
	return out
}

func (b *book) adopt(pairs []storedPair) {
	for _, stored := range pairs {
		if stored.Role != RoleLocalClient && stored.Role != RoleLocalServer {
			continue
		}
		item := b.ensure(stored.Peer, stored.Role, stored.pair())
		item.established = true
		item.id = stored.ID
		if stored.ID > b.nextID {
			b.nextID = stored.ID
		}
		if stored.Role == RoleLocalClient {
			item.wanted = true
		}
	}
}

func (b *book) records() []Record {
	out := make([]Record, 0)
	for _, item := range b.byKey {
		if !item.established {
			continue
		}
		out = append(out, Record{
			Peer: item.peer,
			Role: item.role,
			Pair: clonePair(item.pair),
			ID:   item.id,
		})
	}
	return out
}

func (s *Instance) selectPairsLocked(peer string, pairs []Pair) {
	wanted := make(map[string]Pair, len(pairs))
	for _, pair := range pairs {
		pair = clonePair(pair)
		wanted[keyOf(peer, RoleLocalClient, pair)] = pair
		item := s.book.ensure(peer, RoleLocalClient, pair)
		item.wanted = true
		if item.releaseQueued && !item.releaseSent {
			// The delete has not left the host. Selecting the pair again drops it
			// so a stale delete cannot remove the subscription the peer still has.
			item.releaseQueued = false
		}
		if item.releaseSent {
			// A delete that already left stays with the peer. The new selection is
			// a new subscribe; a later negative result must not send that delete again.
			item.releaseQueued = false
		}
	}
	for _, item := range s.entriesOf(peer, RoleLocalClient) {
		if _, keep := wanted[keyOf(peer, RoleLocalClient, item.pair)]; keep {
			continue
		}
		item.wanted = false
		if item.established || item.pending != nil {
			item.releaseQueued = true
		}
	}
	s.flushLocked(peer)
}

func (s *Instance) entriesOf(peer string, role Role) []*entry {
	out := make([]*entry, 0)
	for _, item := range s.book.byKey {
		if item.peer == peer && item.role == role {
			out = append(out, item)
		}
	}
	return out
}

func (s *Instance) flushLocked(peer string) {
	if !s.connected[peer] {
		s.dropUnsentReleasesLocked(peer)
		return
	}
	if !s.trusted[peer] || s.suspended {
		return
	}
	for _, item := range s.entriesOf(peer, RoleLocalClient) {
		s.flushReleaseLocked(item)
	}
	for _, item := range s.entriesOf(peer, RoleLocalServer) {
		s.flushReleaseLocked(item)
	}
	for _, item := range s.entriesOf(peer, RoleLocalClient) {
		if !item.wanted || item.established || item.pending != nil || item.releaseQueued {
			continue
		}
		s.sendSubscribeLocked(item)
	}
}

func (s *Instance) canExchangeLocked(peer string) bool {
	return s.trusted[peer] && s.connected[peer] && !s.suspended
}

func (s *Instance) dropUnsentReleasesLocked(peer string) {
	for _, item := range s.entriesOf(peer, RoleLocalClient) {
		s.dropUnsentReleaseLocked(item)
	}
	for _, item := range s.entriesOf(peer, RoleLocalServer) {
		s.dropUnsentReleaseLocked(item)
	}
}

func (s *Instance) dropUnsentReleaseLocked(item *entry) {
	if !item.releaseQueued || item.releaseSent {
		return
	}
	s.cancelPendingLocked(item)
	item.established = false
	item.wanted = false
	item.releaseQueued = false
	s.book.drop(item)
}

func (s *Instance) flushReleaseLocked(item *entry) {
	if !item.releaseQueued || item.releaseSent {
		return
	}
	result := s.sendDeleteLocked(item)
	if !result.Sent || result.Replay {
		return
	}
	s.cancelPendingLocked(item)
	item.releaseSent = true
	counter := result.Counter
	item.releaseCounter = &counter
	s.book.releaseOf[counterKey(item.peer, counter)] = keyOf(item.peer, item.role, item.pair)
	item.established = false
	item.wanted = false
	item.renewCounter = nil
}

func (s *Instance) sendDeleteLocked(item *entry) SendResult {
	del := Delete{Client: cloneAddress(item.pair.Client), Server: cloneAddress(item.pair.Server)}
	var result SendResult
	switch {
	case s.transport != nil:
		result = s.transport.SendDelete(item.peer, del)
	case item.role == RoleLocalServer:
		// The client address is the remote feature. The local-client delete looks
		// that address up on this device and sends nothing (SPINE 1.3.0 §7.4.4).
		result = s.spineDeleteServer(item.peer, del)
	default:
		result = s.spineDelete(item.peer, del)
	}
	if result.Sent && !result.Replay {
		item.lastWasSubscribe = false
	}
	return result
}

func (s *Instance) sendSubscribeLocked(item *entry) {
	var result SendResult
	if s.transport != nil {
		result = s.transport.SendSubscribe(item.peer, clonePair(item.pair))
	} else {
		result = s.spineSubscribe(item.peer, item.pair)
	}
	if !result.Sent {
		return
	}
	if result.Replay || (item.lastSubscribe != nil && *item.lastSubscribe == result.Counter && !item.lastWasSubscribe) {
		// A replay of a subscribe that a later delete cancelled does not establish the pair.
		return
	}
	if item.lastSubscribe != nil && *item.lastSubscribe == result.Counter && item.pending != nil {
		return
	}
	counter := result.Counter
	item.lastSubscribe = &counter
	item.lastWasSubscribe = true
	item.pending = &counter
	s.book.byCounter[counterKey(item.peer, counter)] = keyOf(item.peer, item.role, item.pair)
}

func (s *Instance) cancelPendingLocked(item *entry) {
	if item.pending == nil {
		return
	}
	ref := counterKey(item.peer, *item.pending)
	s.book.cancelled[ref] = struct{}{}
	delete(s.book.byCounter, ref)
	item.pending = nil
	item.lastWasSubscribe = false
}

func (s *Instance) noteResultLocked(peer string, counter uint64, accepted bool) {
	ref := counterKey(peer, counter)
	if _, cancelled := s.book.cancelled[ref]; cancelled {
		return
	}
	if key, ok := s.book.byCounter[ref]; ok {
		item := s.book.byKey[key]
		if item == nil || item.peer != peer {
			return
		}
		delete(s.book.byCounter, ref)
		item.pending = nil
		if item.renewCounter != nil && *item.renewCounter == counter {
			item.renewCounter = nil
		}
		if !accepted || !item.wanted {
			item.established = false
			if !item.wanted {
				s.book.drop(item)
			}
			return
		}
		item.established = true
		return
	}
	if key, ok := s.book.releaseOf[ref]; ok {
		item := s.book.byKey[key]
		delete(s.book.releaseOf, ref)
		if item == nil || item.peer != peer {
			return
		}
		item.releaseCounter = nil
		if item.wanted || item.pending != nil || item.established {
			item.releaseQueued = false
			item.releaseSent = false
			return
		}
		if accepted {
			item.releaseQueued = false
			item.releaseSent = false
			item.established = false
			s.book.drop(item)
			return
		}
		// SPINE 1.3.0 §7.4.4: a negative delete is sent again while the peer stays connected
		// and the pair still needs release.
		item.releaseSent = false
		item.releaseQueued = true
		if s.canExchangeLocked(peer) {
			s.flushReleaseLocked(item)
		}
	}
}

func (s *Instance) reconcileLocked(peer string) {
	if !s.canExchangeLocked(peer) {
		return
	}
	s.flushLocked(peer)
	if s.transport == nil {
		return
	}
	held, answered := s.transport.PeerHeld(peer)
	if !answered {
		return
	}
	for _, item := range s.entriesOf(peer, RoleLocalClient) {
		if !item.established || !item.wanted || item.pending != nil || item.renewCounter != nil {
			continue
		}
		if pairHeld(held, item.pair) {
			continue
		}
		before := item.lastSubscribe
		s.sendSubscribeLocked(item)
		if item.pending != nil && (before == nil || item.lastSubscribe == nil || *before != *item.lastSubscribe) {
			counter := *item.pending
			item.renewCounter = &counter
		}
	}
}

func (s *Instance) releaseLocked(peer string, role Role, pair Pair) {
	item := s.book.get(peer, role, pair)
	if item == nil {
		return
	}
	item.wanted = false
	item.releaseQueued = true
	if !s.canExchangeLocked(peer) {
		s.dropUnsentReleaseLocked(item)
		return
	}
	s.flushReleaseLocked(item)
}

func (s *Instance) acceptServerLocked(peer string, pair Pair) bool {
	if !s.canExchangeLocked(peer) {
		return false
	}
	item := s.book.ensure(peer, RoleLocalServer, pair)
	item.established = true
	item.releaseQueued = false
	item.releaseSent = false
	return true
}

func (s *Instance) applyDeleteLocked(peer string, del Delete) {
	del = fillDelete(del, peerDevice(s, peer), s.localDevice)
	var matched []*entry
	for _, item := range s.book.byKey {
		if item.peer != peer {
			continue
		}
		if !covers(del.Client, item.pair.Client) || !covers(del.Server, item.pair.Server) {
			continue
		}
		matched = append(matched, item)
	}
	for _, item := range matched {
		s.cancelPendingLocked(item)
		item.established = false
		item.wanted = false
		item.releaseQueued = false
		s.book.drop(item)
	}
}

func (s *Instance) subscriptionDataLocked(peer string) []model.SubscriptionManagementEntryDataType {
	out := make([]model.SubscriptionManagementEntryDataType, 0)
	for _, item := range s.book.byKey {
		if item.peer != peer || !item.established {
			continue
		}
		client := cloneAddress(item.pair.Client)
		server := cloneAddress(item.pair.Server)
		id := model.SubscriptionIdType(item.id)
		out = append(out, model.SubscriptionManagementEntryDataType{
			SubscriptionId: &id,
			ClientAddress:  &client,
			ServerAddress:  &server,
		})
	}
	return out
}

func fillDelete(del Delete, sender, recipient model.AddressDeviceType) Delete {
	// SPINE Protocol Specification 1.3.0 §7.4.4 absence rules.
	if del.Client.Device == nil && sender != "" {
		del.Client.Device = util.Ptr(sender)
	}
	if del.Server.Device == nil && recipient != "" {
		del.Server.Device = util.Ptr(recipient)
	}
	return del
}

func peerDevice(s *Instance, peer string) model.AddressDeviceType {
	for _, item := range s.book.byKey {
		if item.peer != peer {
			continue
		}
		if item.role == RoleLocalClient && item.pair.Server.Device != nil {
			return *item.pair.Server.Device
		}
		if item.role == RoleLocalServer && item.pair.Client.Device != nil {
			return *item.pair.Client.Device
		}
	}
	return ""
}

func covers(scope, actual model.FeatureAddressType) bool {
	if scope.Device != nil {
		if actual.Device == nil || *scope.Device != *actual.Device {
			return false
		}
	}
	if len(scope.Entity) == 0 {
		return true
	}
	if !reflect.DeepEqual(scope.Entity, actual.Entity) {
		return false
	}
	if scope.Feature == nil {
		return true
	}
	if actual.Feature == nil {
		return false
	}
	return *scope.Feature == *actual.Feature
}

func pairHeld(held []Pair, pair Pair) bool {
	for _, item := range held {
		if sameAddress(item.Client, pair.Client) && sameAddress(item.Server, pair.Server) {
			return true
		}
	}
	return false
}

func sameAddress(left, right model.FeatureAddressType) bool {
	return left.String() == right.String()
}

func clonePair(pair Pair) Pair {
	return Pair{
		Client:            cloneAddress(pair.Client),
		Server:            cloneAddress(pair.Server),
		ServerFeatureType: pair.ServerFeatureType,
	}
}

func cloneAddress(in model.FeatureAddressType) model.FeatureAddressType {
	out := model.FeatureAddressType{}
	if in.Device != nil {
		out.Device = util.Ptr(*in.Device)
	}
	if len(in.Entity) > 0 {
		out.Entity = append([]model.AddressEntityType(nil), in.Entity...)
	}
	if in.Feature != nil {
		out.Feature = util.Ptr(*in.Feature)
	}
	return out
}
