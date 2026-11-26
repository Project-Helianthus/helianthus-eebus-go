package session

import (
	"github.com/Project-Helianthus/helianthus-spine-go/model"
)

// Role is which subscription partner this instance is.
// SPINE Protocol Specification 1.3.0 §7.4.1 rule 4 applies to both partners.
type Role string

const (
	// RoleLocalClient is a subscription this instance requested to a remote server.
	RoleLocalClient Role = "local-client"
	// RoleLocalServer is a subscription a remote client holds on a local server feature.
	RoleLocalServer Role = "local-server"
)

// Pair is one SPINE feature subscription.
type Pair struct {
	Client            model.FeatureAddressType
	Server            model.FeatureAddressType
	ServerFeatureType model.FeatureTypeType
}

// Delete is a SPINE subscription delete. Absent address parts are wildcards
// as SPINE Protocol Specification 1.3.0 §7.4.4 defines them.
type Delete struct {
	Client model.FeatureAddressType
	Server model.FeatureAddressType
}

// SendResult is one subscribe or delete attempt.
// Sent is false when the datagram did not leave the host.
// A replay of an unanswered datagram returns the earlier Counter with Sent true
// and Replay true, and the peer observes no new datagram.
type SendResult struct {
	Counter uint64
	Sent    bool
	Replay  bool
}

// Transport is the per-instance exchange with a paired peer.
// The pinned spine-go Sender supplies Subscribe and Unsubscribe when a remote
// device is connected; an offline peer implements the same exchange in-process.
type Transport interface {
	SendSubscribe(peer string, pair Pair) SendResult
	SendDelete(peer string, del Delete) SendResult
	SendNotify(peer string, destination model.FeatureAddressType, function model.FunctionType, data any) error
	// PeerHeld is the peer's nodeManagementSubscriptionData reply
	// (SPINE Protocol Specification 1.3.0 §7.4.3). The bool is false when that
	// read was not answered.
	PeerHeld(peer string) ([]Pair, bool)
}

// Record is one established feature pair of this instance.
type Record struct {
	Peer string
	Role Role
	Pair Pair
	ID   uint
}
