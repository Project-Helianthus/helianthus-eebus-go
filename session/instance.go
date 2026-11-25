// Package session owns one eeBUS instance: its SHIP node, trust store and SPINE
// local device, plus the feature-pair subscriptions those pinned libraries do
// not retain across a SHIP disconnect.
package session

import (
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"os"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/Project-Helianthus/helianthus-eebus-go/api"
	"github.com/Project-Helianthus/helianthus-eebus-go/discovery"
	"github.com/Project-Helianthus/helianthus-eebus-go/service"
	shipapi "github.com/Project-Helianthus/helianthus-ship-go/api"
	shiputil "github.com/Project-Helianthus/helianthus-ship-go/util"
	spineapi "github.com/Project-Helianthus/helianthus-spine-go/api"
	"github.com/Project-Helianthus/helianthus-spine-go/model"
	"github.com/Project-Helianthus/helianthus-spine-go/spine"
	"github.com/Project-Helianthus/helianthus-spine-go/util"
)

var (
	errClosed         = errors.New("instance is closed")
	errState          = errors.New("instance state root is missing")
	errNoSPINE        = errors.New("remote is not ready for SPINE data")
	errNotPrimary     = errors.New("subscription read is not addressed to the primary node management")
	errNotListening   = errors.New("ship node is not listening")
	errNodeManagement = errors.New("node management feature is missing")
)

// pinnedMDNSUnstarted is not a network interface. The hub resolves it when mDNS
// starts, fails, and leaves its Zeroconf browse unstarted. That browse's
// Shutdown waits on a send its listener no longer receives once shutdown
// cancels the browse, so Close would not return while a peer record is in
// flight. This instance registers and browses the SHIP §7 service itself.
const pinnedMDNSUnstarted = "session-ship-mdns-unstarted"

// NotifyHandler receives a notify for one established subscription of this instance.
type NotifyHandler func(peer string, pair Pair, function model.FunctionType, data any, at time.Time)

// Config is the per-instance SHIP and SPINE identity. Heartbeat is passed through
// to the existing service configuration; this package does not choose a value.
type Config struct {
	VendorCode  string
	Brand       string
	Model       string
	Serial      string
	DeviceType  model.DeviceTypeType
	EntityTypes []model.EntityTypeType
	Port        int
	Certificate tls.Certificate
	Heartbeat   time.Duration
	StateRoot   string
	Host        string
	Register    bool
	Transport   Transport
}

type notified struct {
	data any
	at   time.Time
}

// Instance is one eeBUS instance. Nothing in this package stores instance
// state in a package-level variable.
type Instance struct {
	mu            sync.Mutex
	closed        bool
	listening     bool
	suspended     bool
	root          string
	service       *service.Service
	ski           string
	port          int
	localDevice   model.AddressDeviceType
	view          *discovery.Node
	transport     Transport
	trusted       map[string]bool
	connected     map[string]bool
	book          *book
	cache         map[string]notified
	handler       NotifyHandler
	peerHeld      map[string][]Pair
	peerHeldReady map[string]bool
	peerHeldAsked map[string]bool
	heldCounter   map[string]uint64
	browser       *discovery.Browser
	registration  *discovery.Registration
}

var _ api.ServiceReaderInterface = (*Instance)(nil)
var _ api.PairingCandidateReader = (*Instance)(nil)
var _ spineapi.EventHandlerInterface = (*Instance)(nil)

// Open builds one SHIP node, trust store and SPINE local device from cfg,
// starts that node's SHIP listener and mDNS announcement, and loads feature
// pairs retained in the state root.
func Open(cfg Config) (*Instance, error) {
	info, err := os.Stat(cfg.StateRoot)
	if err != nil || !info.IsDir() {
		return nil, errState
	}
	configuration, err := api.NewConfiguration(
		cfg.VendorCode,
		cfg.Brand,
		cfg.Model,
		cfg.Serial,
		cfg.DeviceType,
		cfg.EntityTypes,
		cfg.Port,
		cfg.Certificate,
		cfg.Heartbeat,
	)
	if err != nil {
		return nil, err
	}
	configuration.SetInterfaces([]string{pinnedMDNSUnstarted})
	instance := &Instance{
		root:          cfg.StateRoot,
		port:          cfg.Port,
		transport:     cfg.Transport,
		trusted:       make(map[string]bool),
		connected:     make(map[string]bool),
		book:          newBook(),
		cache:         make(map[string]notified),
		peerHeld:      make(map[string][]Pair),
		peerHeldReady: make(map[string]bool),
		peerHeldAsked: make(map[string]bool),
		heldCounter:   make(map[string]uint64),
	}
	instance.service = service.NewService(configuration, instance)
	if err := instance.service.Setup(); err != nil {
		return nil, err
	}
	local := instance.service.LocalService()
	if local == nil {
		instance.service.Shutdown()
		return nil, errState
	}
	instance.ski = local.SKI()
	if address := instance.service.LocalDevice().Address(); address != nil {
		instance.localDevice = *address
	}
	instance.view = discovery.NewNode(discovery.Announcement{
		Name: configuration.MdnsServiceName(),
		Host: cfg.Host,
		Port: configuration.Port(),
		TXT: discovery.TXT{
			TXTVers:  "1",
			ID:       configuration.Identifier(),
			Path:     discovery.WebsocketPath,
			SKI:      instance.ski,
			Register: cfg.Register,
			Brand:    configuration.DeviceBrand(),
			Type:     string(configuration.DeviceType()),
			Model:    configuration.DeviceModel(),
		},
	})
	stored, err := loadState(cfg.StateRoot)
	if err != nil {
		instance.service.Shutdown()
		return nil, err
	}
	instance.book.adopt(stored)
	// The pinned node management answers a subscription-data call from its
	// in-memory remote-client list. ProcessCmd looks the feature up on the
	// entity, so this instance answers from the retained pairs instead.
	if err := instance.installRetainedRead(); err != nil {
		instance.service.Shutdown()
		return nil, err
	}
	if local := instance.service.LocalDevice(); local != nil && local.NodeManagement() != nil {
		local.NodeManagement().AddResultCallback(instance.onSpineResult)
	}
	if err := instance.service.SetPairingRegistration(cfg.Register); err != nil {
		instance.service.Shutdown()
		return nil, err
	}
	if err := spine.Events.Subscribe(instance); err != nil {
		instance.service.Shutdown()
		return nil, err
	}
	// service.Start opens this instance's SHIP websocket. The hub mDNS browse
	// stays unstarted; this instance registers and browses the SHIP §7 record.
	instance.service.Start()
	err = waitSHIPListener(instance.port)
	if err != nil {
		_ = spine.Events.Unsubscribe(instance)
		instance.service.Shutdown()
		return nil, err
	}
	instance.browser = discovery.NewBrowser(instance.ski)
	if err := instance.browser.Start(instance.noteAnnouncements); err != nil {
		instance.browser = nil
		_ = spine.Events.Unsubscribe(instance)
		instance.service.Shutdown()
		return nil, err
	}
	registration, err := discovery.Register(instance.view.Announcement())
	if err != nil {
		instance.browser.Stop()
		instance.browser = nil
		_ = spine.Events.Unsubscribe(instance)
		instance.service.Shutdown()
		return nil, err
	}
	instance.registration = registration
	instance.listening = true
	return instance, nil
}

// Close stops this instance. Another instance in the same process is left running.
func (s *Instance) Close() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	s.listening = false
	pairs := s.book.durable()
	root := s.root
	svc := s.service
	browser := s.browser
	registration := s.registration
	s.browser = nil
	s.registration = nil
	s.mu.Unlock()
	_ = spine.Events.Unsubscribe(s)
	// Stop the browse while peer records are still in flight, then withdraw
	// this instance's announcement, then close the SHIP listener.
	if browser != nil {
		browser.Stop()
	}
	if registration != nil {
		registration.Close()
	}
	if svc != nil {
		svc.Shutdown()
	}
	return writeState(root, pairs)
}

// Running reports whether this instance's SHIP listener is accepting connections.
func (s *Instance) Running() bool {
	s.mu.Lock()
	closed := s.closed
	listening := s.listening
	port := s.port
	s.mu.Unlock()
	return !closed && listening && shipPortOpen(port)
}

func waitSHIPListener(port int) error {
	deadline := time.Now().Add(2 * time.Second)
	var last error
	for {
		if err := dialSHIP(port); err == nil {
			return nil
		} else {
			last = err
		}
		if !time.Now().Before(deadline) {
			return fmt.Errorf("port %d: %w: %w", port, last, errNotListening)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func shipPortOpen(port int) bool {
	return dialSHIP(port) == nil
}

func dialSHIP(port int) error {
	if port <= 0 {
		return errNotListening
	}
	conn, err := net.DialTimeout("tcp", net.JoinHostPort("127.0.0.1", strconv.Itoa(port)), 200*time.Millisecond)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()
	return nil
}

// SKI is this instance's SHIP subject key identifier.
func (s *Instance) SKI() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.ski
}

// Port is this instance's SHIP port.
func (s *Instance) Port() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.port
}

// LocalDevice is this instance's SPINE local device.
func (s *Instance) LocalDevice() spineapi.DeviceLocalInterface {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.service == nil {
		return nil
	}
	return s.service.LocalDevice()
}

// Discovery is this instance's discovery view.
func (s *Instance) Discovery() *discovery.Node {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.view
}

// PrimaryNodeManagement is the destination of a subscription-data read.
func (s *Instance) PrimaryNodeManagement() model.FeatureAddressType {
	s.mu.Lock()
	defer s.mu.Unlock()
	device := s.localDevice
	return model.FeatureAddressType{
		Device:  &device,
		Entity:  []model.AddressEntityType{0},
		Feature: util.Ptr(model.AddressFeatureType(0)),
	}
}

// SetNotifyHandler installs the receiver for notifies of this instance.
func (s *Instance) SetNotifyHandler(handler NotifyHandler) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.handler = handler
}

// AttachRemote adds a remote device to this instance's SPINE device only.
func (s *Instance) AttachRemote(ski string, writer shipapi.ShipConnectionDataWriterInterface) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || s.service == nil || s.service.LocalDevice() == nil {
		return errClosed
	}
	s.service.LocalDevice().SetupRemoteDevice(shiputil.NormalizeSKI(ski), writer)
	return nil
}

// Trust records SHIP trust for a remote (SHIP 1.0.1 §12.3.2, §13.4.4.1).
// Revoking trust removes that remote's feature pairs.
// The hub reports the resulting SHIP state back through this instance, so the
// call into the service is made without the instance lock.
func (s *Instance) Trust(ski string, trusted bool) error {
	ski = shiputil.NormalizeSKI(ski)
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return errClosed
	}
	svc := s.service
	if trusted {
		s.trusted[ski] = true
		s.view.SetConnection(ski, shipapi.ConnectionStateTrusted, true)
		s.flushLocked(ski)
		err := s.saveLocked()
		s.mu.Unlock()
		svc.RegisterRemoteSKI(ski)
		return err
	}
	for _, item := range s.peerEntriesLocked(ski) {
		item.wanted = false
		item.releaseQueued = true
	}
	if s.connected[ski] && !s.suspended {
		s.flushLocked(ski)
	} else {
		s.dropUnsentReleasesLocked(ski)
	}
	s.trusted[ski] = false
	s.view.SetConnection(ski, shipapi.ConnectionStateNone, false)
	s.dropRemainingLocked(ski)
	err := s.saveLocked()
	s.mu.Unlock()
	svc.UnregisterRemoteSKI(ski)
	return err
}

// Connect records the SHIP connection of one remote. A disconnect retains
// established feature pairs (SPINE 1.3.0 §7.4.1, SHIP 1.0.1 §12.3.2).
func (s *Instance) Connect(ski string, up bool) error {
	ski = shiputil.NormalizeSKI(ski)
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return errClosed
	}
	s.connected[ski] = up
	state := shipapi.ConnectionStateNone
	if up && s.trusted[ski] {
		state = shipapi.ConnectionStateTrusted
	} else if up {
		state = shipapi.ConnectionStateCompleted
	}
	s.view.SetConnection(ski, state, s.trusted[ski])
	if up {
		s.reconcileLocked(ski)
	} else {
		s.dropUnsentReleasesLocked(ski)
		// The subscribe left the host and the connection is gone, so its
		// result will not arrive. Leave the selection wanted: the next
		// discovery sends it again. Counters from this connection are not
		// persistent subscription information (SPINE 1.3.0 §7.4.1).
		s.abandonPendingLocked(ski)
		s.forgetConnectionCountersLocked(ski)
		s.forgetPeerHeldLocked(ski)
	}
	return s.saveLocked()
}

// Suspend stops connection-local sends and notifies and retains feature pairs.
func (s *Instance) Suspend() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return errClosed
	}
	s.suspended = true
	return nil
}

// Resume allows exchanges again and reconciles retained pairs.
func (s *Instance) Resume() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return errClosed
	}
	s.suspended = false
	for peer, up := range s.connected {
		if up {
			s.reconcileLocked(peer)
		}
	}
	return s.saveLocked()
}

// Select replaces the local-client feature pairs the caller wants for one trusted remote.
func (s *Instance) Select(ski string, pairs []Pair) error {
	ski = shiputil.NormalizeSKI(ski)
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return errClosed
	}
	s.selectPairsLocked(ski, pairs)
	return s.saveLocked()
}

// Release asks this instance to end one local-client pair (SPINE 1.3.0 §7.4.4).
func (s *Instance) Release(ski string, pair Pair) error {
	return s.release(ski, RoleLocalClient, pair)
}

// ReleaseServer asks this instance to end one pair on its local server feature
// (SPINE 1.3.0 §7.4.1 rules 6–7, §7.4.4, §7.4.5(b)).
func (s *Instance) ReleaseServer(ski string, pair Pair) error {
	return s.release(ski, RoleLocalServer, pair)
}

func (s *Instance) release(ski string, role Role, pair Pair) error {
	ski = shiputil.NormalizeSKI(ski)
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return errClosed
	}
	s.releaseLocked(ski, role, pair)
	return s.saveLocked()
}

// Run sends queued subscribes and deletes for one remote.
func (s *Instance) Run(ski string) error {
	ski = shiputil.NormalizeSKI(ski)
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return errClosed
	}
	s.flushLocked(ski)
	return s.saveLocked()
}

// Reconcile compares retained local-client pairs with the pairs the peer still
// holds and renews one the peer has lost (SPINE 1.3.0 §7.4.5(a)).
func (s *Instance) Reconcile(ski string) error {
	ski = shiputil.NormalizeSKI(ski)
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return errClosed
	}
	s.reconcileLocked(ski)
	return s.saveLocked()
}

// NoteResult records the peer's result for a subscribe or delete counter.
func (s *Instance) NoteResult(ski string, counter uint64, accepted bool) error {
	ski = shiputil.NormalizeSKI(ski)
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return errClosed
	}
	s.noteResultLocked(ski, counter, accepted)
	return s.saveLocked()
}

// AcceptRemoteSubscribe records a paired remote client's subscription to a
// local server feature.
func (s *Instance) AcceptRemoteSubscribe(ski string, pair Pair) error {
	ski = shiputil.NormalizeSKI(ski)
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return errClosed
	}
	if !s.acceptServerLocked(ski, pair) {
		return errNoSPINE
	}
	return s.saveLocked()
}

// ApplyRemoteDelete ends every retained pair of this peer inside the delete's
// address scope, for either role (SPINE 1.3.0 §7.4.4).
func (s *Instance) ApplyRemoteDelete(ski string, del Delete) error {
	ski = shiputil.NormalizeSKI(ski)
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return errClosed
	}
	if !s.trusted[ski] {
		return nil
	}
	s.applyDeleteLocked(ski, del)
	return s.saveLocked()
}

// SubscriptionData answers a paired peer's read of nodeManagementSubscriptionData
// addressed to this instance's primary node management.
func (s *Instance) SubscriptionData(ski string, destination model.FeatureAddressType) ([]model.SubscriptionManagementEntryDataType, error) {
	ski = shiputil.NormalizeSKI(ski)
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, errClosed
	}
	if !s.trusted[ski] || !isPrimary(s.localDevice, destination) {
		return nil, errNotPrimary
	}
	entries := s.subscriptionDataLocked(ski)
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].ClientAddress.String()+entries[i].ServerAddress.String() <
			entries[j].ClientAddress.String()+entries[j].ServerAddress.String()
	})
	return entries, nil
}

// Established returns the retained feature pairs.
func (s *Instance) Established() []Record {
	s.mu.Lock()
	defer s.mu.Unlock()
	records := s.book.records()
	sort.Slice(records, func(i, j int) bool {
		if records[i].Peer != records[j].Peer {
			return records[i].Peer < records[j].Peer
		}
		if records[i].Role != records[j].Role {
			return records[i].Role < records[j].Role
		}
		return records[i].Pair.Client.String()+records[i].Pair.Server.String() <
			records[j].Pair.Client.String()+records[j].Pair.Server.String()
	})
	return records
}

// ReceiveNotify stores a notify for an established local-client pair and
// delivers it to this instance's handler only.
func (s *Instance) ReceiveNotify(ski string, pair Pair, function model.FunctionType, data any, at time.Time) bool {
	ski = shiputil.NormalizeSKI(ski)
	s.mu.Lock()
	handler, pairCopy, ok := s.noteNotifyLocked(ski, pair, function, data, at)
	s.mu.Unlock()
	if !ok {
		return false
	}
	if handler != nil {
		handler(ski, pairCopy, function, data, at)
	}
	return true
}

// LastNotified returns the last notified value and its notification time.
// The bool is false when no notification has arrived.
func (s *Instance) LastNotified(ski string, pair Pair, function model.FunctionType) (any, time.Time, bool) {
	ski = shiputil.NormalizeSKI(ski)
	s.mu.Lock()
	defer s.mu.Unlock()
	item, ok := s.cache[cacheKey(ski, pair, function)]
	if !ok {
		return nil, time.Time{}, false
	}
	return item.data, item.at, true
}

// Publish sends one local server-feature update to each established remote client.
func (s *Instance) Publish(server model.FeatureAddressType, function model.FunctionType, data any) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || s.suspended {
		return 0
	}
	count := 0
	for _, item := range s.book.byKey {
		if item.role != RoleLocalServer || !item.established || !sameAddress(item.pair.Server, server) {
			continue
		}
		if !s.canExchangeLocked(item.peer) {
			continue
		}
		if s.sendNotifyLocked(item, function, data) == nil {
			count++
		}
	}
	return count
}

// HandleEvent delivers a SPINE data notify to this instance when the remote
// belongs to it. The pinned spine-go event bus is process-wide; filtering keeps
// each instance's handler separate. A subscription change is the pinned
// manager's record of a remote subscribe or delete.
func (s *Instance) HandleEvent(payload spineapi.EventPayload) {
	if payload.EventType == spineapi.EventTypeSubscriptionChange {
		s.noteSubscriptionEvent(payload)
		return
	}
	if payload.EventType != spineapi.EventTypeDataChange || payload.Ski == "" {
		return
	}
	ski := shiputil.NormalizeSKI(payload.Ski)
	var server *model.FeatureAddressType
	if payload.Feature != nil && payload.Feature.Address() != nil {
		copied := cloneAddress(*payload.Feature.Address())
		server = &copied
	}
	s.mu.Lock()
	if s.closed || !s.canExchangeLocked(ski) {
		s.mu.Unlock()
		return
	}
	pair, ok := s.matchClientLocked(ski, server)
	if !ok {
		s.mu.Unlock()
		return
	}
	at := time.Now()
	handler, pairCopy, noted := s.noteNotifyLocked(ski, pair, payload.Function, payload.Data, at)
	s.mu.Unlock()
	if noted && handler != nil {
		handler(ski, pairCopy, payload.Function, payload.Data, at)
	}
}

func (s *Instance) noteNotifyLocked(ski string, pair Pair, function model.FunctionType, data any, at time.Time) (NotifyHandler, Pair, bool) {
	if s.closed || !s.canExchangeLocked(ski) {
		return nil, Pair{}, false
	}
	item := s.book.get(ski, RoleLocalClient, pair)
	if item == nil || !item.established {
		return nil, Pair{}, false
	}
	s.cache[cacheKey(ski, pair, function)] = notified{data: data, at: at}
	return s.handler, clonePair(item.pair), true
}

func (s *Instance) matchClientLocked(peer string, server *model.FeatureAddressType) (Pair, bool) {
	var found Pair
	matches := 0
	for _, item := range s.book.byKey {
		if item.peer != peer || item.role != RoleLocalClient || !item.established {
			continue
		}
		if server != nil && !sameAddress(item.pair.Server, *server) {
			continue
		}
		found = item.pair
		matches++
	}
	if matches != 1 {
		return Pair{}, false
	}
	return found, true
}

func (s *Instance) sendNotifyLocked(item *entry, function model.FunctionType, data any) error {
	destination := cloneAddress(item.pair.Client)
	if s.transport != nil {
		return s.transport.SendNotify(item.peer, destination, function, data)
	}
	return s.spineNotify(item.peer, item.pair, function, data)
}

func (s *Instance) spineSubscribe(peer string, pair Pair) SendResult {
	feature := s.localFeature(pair.Client)
	if feature == nil {
		return SendResult{}
	}
	server := cloneAddress(pair.Server)
	counter, failure := feature.SubscribeToRemote(&server)
	if failure != nil || counter == nil {
		return SendResult{}
	}
	return SendResult{Counter: uint64(*counter), Sent: true}
}

func (s *Instance) spineDelete(peer string, del Delete) SendResult {
	feature := s.localFeature(del.Client)
	if feature == nil {
		return SendResult{}
	}
	server := cloneAddress(del.Server)
	counter, failure := feature.RemoveRemoteSubscription(&server)
	if failure != nil || counter == nil {
		return SendResult{}
	}
	return SendResult{Counter: uint64(*counter), Sent: true}
}

// spineDeleteServer sends the subscription delete for a pair this instance
// serves. The pinned RemoveRemoteSubscription only sends from a local client
// feature, so the server partner sends the same delete call itself
// (SPINE Protocol Specification 1.3.0 §7.4.1 rules 6–7, §7.4.4).
func (s *Instance) spineDeleteServer(peer string, del Delete) SendResult {
	if s.service == nil || s.service.LocalDevice() == nil || del.Client.Device == nil {
		return SendResult{}
	}
	remote := s.service.LocalDevice().RemoteDeviceForSki(peer)
	if remote == nil || remote.Sender() == nil {
		return SendResult{}
	}
	client := cloneAddress(del.Client)
	server := cloneAddress(del.Server)
	sourceDevice := server.Device
	if s.localDevice != "" {
		device := s.localDevice
		sourceDevice = &device
	}
	if sourceDevice == nil {
		return SendResult{}
	}
	counter, err := remote.Sender().Request(
		model.CmdClassifierTypeCall,
		spine.NodeManagementAddress(sourceDevice),
		spine.NodeManagementAddress(client.Device),
		true,
		[]model.CmdType{{
			NodeManagementSubscriptionDeleteCall: spine.NewNodeManagementSubscriptionDeleteCallType(&client, &server),
		}},
	)
	if err != nil || counter == nil {
		return SendResult{}
	}
	return SendResult{Counter: uint64(*counter), Sent: true}
}

func (s *Instance) spineNotify(peer string, pair Pair, function model.FunctionType, data any) error {
	local := s.service.LocalDevice()
	if local == nil {
		return errClosed
	}
	remote := local.RemoteDeviceForSki(peer)
	if remote == nil || remote.Sender() == nil {
		return errClosed
	}
	cmd := model.CmdType{Function: util.Ptr(function)}
	if typed, ok := data.(model.CmdType); ok {
		cmd = typed
	}
	server := cloneAddress(pair.Server)
	client := cloneAddress(pair.Client)
	_, err := remote.Sender().Notify(&server, &client, cmd)
	return err
}

func (s *Instance) localFeature(address model.FeatureAddressType) spineapi.FeatureLocalInterface {
	if s.service == nil || s.service.LocalDevice() == nil {
		return nil
	}
	copied := cloneAddress(address)
	return s.service.LocalDevice().FeatureByAddress(&copied)
}

func (s *Instance) peerEntriesLocked(peer string) []*entry {
	out := make([]*entry, 0)
	for _, item := range s.book.byKey {
		if item.peer == peer {
			out = append(out, item)
		}
	}
	return out
}

func (s *Instance) dropRemainingLocked(peer string) {
	for _, item := range s.peerEntriesLocked(peer) {
		if item.established || item.pending != nil || item.releaseQueued {
			s.cancelPendingLocked(item)
			s.book.drop(item)
		}
	}
}

func (s *Instance) saveLocked() error {
	return writeState(s.root, s.book.durable())
}

func cacheKey(peer string, pair Pair, function model.FunctionType) string {
	return peer + "\x00" + pair.Client.String() + "\x00" + pair.Server.String() + "\x00" + string(function)
}

func isPrimary(local model.AddressDeviceType, destination model.FeatureAddressType) bool {
	if destination.Device == nil || *destination.Device != local || len(destination.Entity) != 1 || destination.Entity[0] != 0 {
		return false
	}
	return destination.Feature != nil && *destination.Feature == 0
}

// RemoteSKIConnected records a SHIP connection for this instance.
func (s *Instance) RemoteSKIConnected(_ api.ServiceInterface, ski string) {
	_ = s.Connect(ski, true)
}

// RemoteSKIDisconnected retains feature pairs across a temporary SHIP disconnect.
func (s *Instance) RemoteSKIDisconnected(_ api.ServiceInterface, ski string) {
	_ = s.Connect(ski, false)
}

func (s *Instance) noteAnnouncements(found []discovery.Announcement) {
	s.mu.Lock()
	view := s.view
	closed := s.closed
	s.mu.Unlock()
	if closed || view == nil {
		return
	}
	view.ReplaceAnnouncements(found)
}

// VisibleRemoteServicesUpdated records discovered SKIs that have no snapshot yet.
func (s *Instance) VisibleRemoteServicesUpdated(_ api.ServiceInterface, entries []shipapi.RemoteService) {
	announcements := make([]discovery.Announcement, 0, len(entries))
	for _, entry := range entries {
		announcements = append(announcements, discovery.Announcement{
			Name: entry.Name,
			TXT: discovery.TXT{
				ID:    entry.Identifier,
				SKI:   entry.Ski,
				Brand: entry.Brand,
				Type:  entry.Type,
				Model: entry.Model,
			},
		})
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.view != nil {
		s.view.ReplaceAnnouncements(announcements)
	}
}

// ServiceShipIDUpdate stores the SHIP identifier a remote reported.
func (s *Instance) ServiceShipIDUpdate(ski string, shipID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.view != nil {
		s.view.SetShipID(shiputil.NormalizeSKI(ski), shipID)
	}
}

// ServicePairingDetailUpdate reports the remote's SHIP state, paired or not.
func (s *Instance) ServicePairingDetailUpdate(ski string, detail *shipapi.ConnectionStateDetail) {
	if detail == nil {
		return
	}
	ski = shiputil.NormalizeSKI(ski)
	state := detail.State()
	s.mu.Lock()
	defer s.mu.Unlock()
	if state == shipapi.ConnectionStateTrusted {
		s.trusted[ski] = true
	}
	if s.view != nil {
		s.view.SetConnection(ski, state, s.trusted[ski])
	}
}

// VisiblePairingCandidateDiscoverySnapshotUpdated joins a discovery snapshot
// with the SHIP state already known for each SKI.
func (s *Instance) VisiblePairingCandidateDiscoverySnapshotUpdated(_ api.ServiceInterface, snapshot shipapi.PairingCandidateDiscoverySnapshotV1) {
	announcements := make([]discovery.Announcement, 0, len(snapshot.Candidates))
	for _, candidate := range snapshot.Candidates {
		announcements = append(announcements, discovery.Announcement{
			Name: candidate.Name,
			Host: candidate.Host,
			Port: candidate.Port,
			TXT: discovery.TXT{
				TXTVers:  "1",
				ID:       candidate.Identifier,
				Path:     candidate.Path,
				SKI:      candidate.SKI,
				Register: candidate.Register,
				Brand:    candidate.Brand,
				Type:     candidate.Type,
				Model:    candidate.Model,
			},
		})
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.view != nil {
		s.view.ReplaceAnnouncements(announcements)
	}
}
