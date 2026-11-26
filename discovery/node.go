// Package discovery reports each SHIP remote of one eeBUS instance with the
// TXT record of SHIP Specification 1.1.0 §7 and that instance's SHIP
// connection and trust state (SHIP §12.3, §13.4).
package discovery

import (
	"fmt"
	"net"
	"net/netip"
	"sort"
	"sync"

	shipapi "github.com/Project-Helianthus/helianthus-ship-go/api"
)

// WebsocketPath is the SHIP websocket path carried in the mDNS TXT path entry.
const WebsocketPath = "/ship/"

// TXT is the SHIP §7 service record this instance announces or observes.
type TXT struct {
	TXTVers  string
	ID       string
	Path     string
	SKI      string
	Register bool
	Brand    string
	Type     string
	Model    string
}

// Records returns the TXT entries in the order used for a SHIP announcement.
func (t TXT) Records() []string {
	register := "false"
	if t.Register {
		register = "true"
	}
	txtvers := t.TXTVers
	if txtvers == "" {
		txtvers = "1"
	}
	path := t.Path
	if path == "" {
		path = WebsocketPath
	}
	return []string{
		"txtvers=" + txtvers,
		"path=" + path,
		"id=" + t.ID,
		"ski=" + t.SKI,
		"brand=" + t.Brand,
		"model=" + t.Model,
		"type=" + t.Type,
		"register=" + register,
	}
}

// Announcement is one instance's SHIP mDNS service.
type Announcement struct {
	Name string
	Host string
	Port int
	TXT  TXT

	addresses []netip.Addr
}

// SHIPEntry returns the endpoint and TXT fields for the SHIP hub's mDNS report.
func (a Announcement) SHIPEntry() *shipapi.MdnsEntry {
	entry := &shipapi.MdnsEntry{
		Name: a.Name, Host: a.Host, Port: a.Port,
		Ski: a.TXT.SKI, Identifier: a.TXT.ID, Path: a.TXT.Path, Register: a.TXT.Register,
		Brand: a.TXT.Brand, Type: a.TXT.Type, Model: a.TXT.Model,
	}
	for _, address := range a.addresses {
		entry.Addresses = append(entry.Addresses, net.IP(address.AsSlice()))
	}
	return entry
}

// Remote is a discovered service plus the SHIP state this instance holds for it.
// An unpaired remote is still reported; Paired is false and Connection is the
// SHIP state, including the initial none state.
type Remote struct {
	Announcement Announcement
	Connection   shipapi.ConnectionState
	Trusted      bool
	Paired       bool
	ShipID       string
}

// Node is the discovery view of one instance. It holds no process-wide state.
type Node struct {
	mu     sync.Mutex
	local  Announcement
	remote map[string]Remote
	ship   map[string]shipState
}

type shipState struct {
	connection shipapi.ConnectionState
	trusted    bool
	shipID     string
	known      bool
}

// NewNode returns the discovery view for one local announcement.
func NewNode(local Announcement) *Node {
	if local.TXT.Path == "" {
		local.TXT.Path = WebsocketPath
	}
	if local.TXT.TXTVers == "" {
		local.TXT.TXTVers = "1"
	}
	return &Node{
		local:  local,
		remote: make(map[string]Remote),
		ship:   make(map[string]shipState),
	}
}

// Announcement returns this instance's own SHIP announcement.
func (n *Node) Announcement() Announcement {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.local
}

// SetRegister updates the register flag of this instance's announcement.
func (n *Node) SetRegister(register bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.local.TXT.Register = register
}

// Observe records one remote announcement and the SHIP state this instance
// currently holds for that SKI. A remote that is not paired is kept.
func (n *Node) Observe(announcement Announcement, connection shipapi.ConnectionState, trusted bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.observeLocked(announcement, connection, trusted)
}

func (n *Node) observeLocked(announcement Announcement, connection shipapi.ConnectionState, trusted bool) {
	ski := announcement.TXT.SKI
	if ski == "" || ski == n.local.TXT.SKI {
		return
	}
	if announcement.TXT.Path == "" {
		announcement.TXT.Path = WebsocketPath
	}
	state := n.ship[ski]
	state.connection = connection
	state.trusted = trusted
	state.known = true
	n.ship[ski] = state
	n.remote[ski] = Remote{
		Announcement: announcement,
		Connection:   connection,
		Trusted:      trusted,
		Paired:       trusted,
		ShipID:       state.shipID,
	}
}

// ReplaceAnnouncements replaces the discovered set. SHIP state already recorded
// for a SKI that is still present is kept. New SKIs start unpaired.
func (n *Node) ReplaceAnnouncements(announcements []Announcement) {
	n.mu.Lock()
	defer n.mu.Unlock()
	next := make(map[string]Remote, len(announcements))
	for _, announcement := range announcements {
		ski := announcement.TXT.SKI
		if ski == "" || ski == n.local.TXT.SKI {
			continue
		}
		state, known := n.ship[ski]
		connection := shipapi.ConnectionStateNone
		trusted := false
		if known {
			connection = state.connection
			trusted = state.trusted
		}
		if announcement.TXT.Path == "" {
			announcement.TXT.Path = WebsocketPath
		}
		next[ski] = Remote{
			Announcement: announcement,
			Connection:   connection,
			Trusted:      trusted,
			Paired:       trusted,
			ShipID:       state.shipID,
		}
	}
	n.remote = next
}

// SetConnection stores the SHIP connection and trust state for a discovered SKI.
// An unknown SKI is remembered and applied when that remote is later observed.
func (n *Node) SetConnection(ski string, connection shipapi.ConnectionState, trusted bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	state := n.ship[ski]
	state.connection = connection
	state.trusted = trusted
	state.known = true
	n.ship[ski] = state
	remote, ok := n.remote[ski]
	if !ok {
		return
	}
	remote.Connection = connection
	remote.Trusted = trusted
	remote.Paired = trusted
	n.remote[ski] = remote
}

// SetShipID stores the SHIP identifier reported for a discovered SKI.
func (n *Node) SetShipID(ski, shipID string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	state := n.ship[ski]
	state.shipID = shipID
	state.known = true
	n.ship[ski] = state
	remote, ok := n.remote[ski]
	if !ok {
		return
	}
	remote.ShipID = shipID
	n.remote[ski] = remote
}

// Remotes returns the discovered remotes ordered by SKI.
func (n *Node) Remotes() []Remote {
	n.mu.Lock()
	defer n.mu.Unlock()
	out := make([]Remote, 0, len(n.remote))
	for _, remote := range n.remote {
		out = append(out, remote)
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].Announcement.TXT.SKI < out[j].Announcement.TXT.SKI
	})
	return out
}

// Remote returns one discovered remote.
func (n *Node) Remote(ski string) (Remote, bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	remote, ok := n.remote[ski]
	return remote, ok
}

// Directory is an in-process announcement exchange shared only by the nodes
// the caller joins. It is not package state.
type Directory struct {
	mu    sync.Mutex
	nodes []*Node
}

// NewDirectory returns an empty exchange.
func NewDirectory() *Directory {
	return &Directory{}
}

// Join adds a node. Joining does not announce it.
func (d *Directory) Join(node *Node) {
	if node == nil {
		return
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	for _, existing := range d.nodes {
		if existing == node {
			return
		}
	}
	d.nodes = append(d.nodes, node)
}

// Publish updates the announcement at every other joined node without
// changing that observer's SHIP connection or trust state.
func (d *Directory) Publish(node *Node) {
	if node == nil {
		return
	}
	announcement := node.Announcement()
	d.mu.Lock()
	nodes := append([]*Node(nil), d.nodes...)
	d.mu.Unlock()
	for _, other := range nodes {
		if other == node {
			continue
		}
		other.mu.Lock()
		state := other.ship[announcement.TXT.SKI]
		other.observeLocked(announcement, state.connection, state.trusted)
		other.mu.Unlock()
	}
}

// String renders a remote for tests and logs without adding protocol behavior.
func (r Remote) String() string {
	return fmt.Sprintf("%s paired=%t state=%d", r.Announcement.TXT.SKI, r.Paired, r.Connection)
}
