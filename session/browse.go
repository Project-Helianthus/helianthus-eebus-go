package session

import (
	"crypto/rand"
	"encoding/hex"
	"net"
	"net/netip"
	"reflect"
	"strings"
	"sync"
	"unsafe"

	"github.com/Project-Helianthus/helianthus-eebus-go/discovery"
	"github.com/Project-Helianthus/helianthus-eebus-go/service"
	shipapi "github.com/Project-Helianthus/helianthus-ship-go/api"
	"github.com/Project-Helianthus/helianthus-ship-go/mdns"
	shiputil "github.com/Project-Helianthus/helianthus-ship-go/util"
)

// shipBrowse is one instance's SHIP §7 announcement and browse.
// The pinned hub starts browse and announcement together, and that provider's
// Shutdown waits out an in-flight record before the hub closes the SHIP
// listener. This browser reports the same snapshots and returns from Shutdown
// before that listener is closed.
type shipBrowse struct {
	once       sync.Once
	mu         sync.Mutex
	local      discovery.Announcement
	browser    *discovery.Browser
	provider   *mdns.ZeroconfProvider
	report     shipapi.MdnsReportInterface
	running    bool
	announced  bool
	pairing    bool
	autoAccept bool
	startErr   error
	revision   uint64
	refs       map[string]string
	cached     map[string]*shipapi.MdnsEntry
	candidates []shipapi.PairingCandidateObservation
}

var _ shipapi.MdnsInterface = (*shipBrowse)(nil)
var _ shipapi.PairingRegistrationSetter = (*shipBrowse)(nil)

func newShipBrowse(local discovery.Announcement) *shipBrowse {
	return &shipBrowse{
		local:    local,
		browser:  discovery.NewBrowser(local.TXT.SKI),
		provider: mdns.NewZeroconfProvider(nil),
		refs:     make(map[string]string),
	}
}

func (s *shipBrowse) Err() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.startErr
}

func (s *shipBrowse) Start(cb shipapi.MdnsReportInterface) error {
	s.mu.Lock()
	if s.running {
		s.report = cb
		err := s.startErr
		s.mu.Unlock()
		return err
	}
	s.report = cb
	s.running = true
	browser := s.browser
	s.mu.Unlock()
	if err := browser.Start(s.onSightings); err != nil {
		s.setErr(err)
		return err
	}
	if err := s.AnnounceMdnsEntry(); err != nil {
		s.setErr(err)
		return err
	}
	return nil
}

func (s *shipBrowse) Shutdown() {
	s.once.Do(func() {
		s.mu.Lock()
		s.report = nil
		browser := s.browser
		provider := s.provider
		s.announced = false
		s.mu.Unlock()
		if browser != nil {
			browser.Stop()
		}
		if provider != nil {
			provider.Shutdown()
		}
	})
}

func (s *shipBrowse) AnnounceMdnsEntry() error {
	s.mu.Lock()
	local := s.local
	local.TXT.Register = s.autoAccept || s.pairing
	provider := s.provider
	s.mu.Unlock()
	if provider == nil {
		return errBrowse
	}
	if err := provider.Announce(local.Name, local.Port, local.TXT.Records()); err != nil {
		return err
	}
	s.mu.Lock()
	s.announced = true
	s.mu.Unlock()
	return nil
}

func (s *shipBrowse) UnannounceMdnsEntry() {
	s.mu.Lock()
	provider := s.provider
	s.announced = false
	s.mu.Unlock()
	if provider != nil {
		provider.Unannounce()
	}
}

func (s *shipBrowse) SetAutoAccept(accept bool) {
	s.mu.Lock()
	s.autoAccept = accept
	announced := s.announced
	s.mu.Unlock()
	if announced {
		_ = s.AnnounceMdnsEntry()
	}
}

func (s *shipBrowse) SetPairingRegistration(available bool) error {
	s.mu.Lock()
	s.pairing = available
	announced := s.announced
	s.mu.Unlock()
	if !announced {
		return nil
	}
	return s.AnnounceMdnsEntry()
}

func (s *shipBrowse) RequestMdnsEntries() {
	s.mu.Lock()
	report := s.report
	entries := s.cached
	candidates := append([]shipapi.PairingCandidateObservation(nil), s.candidates...)
	revision := s.revision
	s.mu.Unlock()
	if entries == nil {
		entries = map[string]*shipapi.MdnsEntry{}
	}
	sendBrowse(report, entries, candidates, revision, false)
}

func (s *shipBrowse) onSightings(sightings []discovery.Sighting) {
	s.mu.Lock()
	report := s.report
	if report == nil {
		s.mu.Unlock()
		return
	}
	localSKI := shiputil.NormalizeSKI(s.local.TXT.SKI)
	entries := make(map[string]*shipapi.MdnsEntry, len(sightings))
	candidates := make([]shipapi.PairingCandidateObservation, 0, len(sightings))
	keep := make(map[string]struct{}, len(sightings))
	for _, sighting := range sightings {
		ski := shiputil.NormalizeSKI(sighting.Announcement.TXT.SKI)
		if ski == "" || ski == localSKI {
			continue
		}
		entry := mdnsEntry(sighting, ski)
		entries[ski] = entry
		keep[ski] = struct{}{}
		ref := s.refs[ski]
		if ref == "" {
			ref = newCandidateRef()
			s.refs[ski] = ref
		}
		candidates = append(candidates, candidateFrom(entry, ref))
	}
	for ski := range s.refs {
		if _, ok := keep[ski]; !ok {
			delete(s.refs, ski)
		}
	}
	s.revision++
	if s.revision == 0 {
		s.revision++
	}
	revision := s.revision
	s.cached = entries
	s.candidates = candidates
	s.mu.Unlock()
	sendBrowse(report, entries, candidates, revision, true)
}

func (s *shipBrowse) setErr(err error) {
	s.mu.Lock()
	s.startErr = err
	s.mu.Unlock()
}

func sendBrowse(report shipapi.MdnsReportInterface, entries map[string]*shipapi.MdnsEntry, candidates []shipapi.PairingCandidateObservation, revision uint64, newEntries bool) {
	if report == nil {
		return
	}
	if richer, ok := report.(shipapi.PairingCandidateMdnsReportInterface); ok {
		richer.ReportMdnsEntriesWithCandidates(entries, newEntries, candidates, revision)
		return
	}
	report.ReportMdnsEntries(entries, newEntries)
}

func mdnsEntry(sighting discovery.Sighting, ski string) *shipapi.MdnsEntry {
	ann := sighting.Announcement
	entry := &shipapi.MdnsEntry{
		Name:       ann.Name,
		Ski:        ski,
		Identifier: ann.TXT.ID,
		Path:       ann.TXT.Path,
		Register:   ann.TXT.Register,
		Brand:      ann.TXT.Brand,
		Type:       ann.TXT.Type,
		Model:      ann.TXT.Model,
		Host:       strings.TrimSuffix(ann.Host, "."),
		Port:       ann.Port,
	}
	for _, ip := range sighting.Addresses {
		parsed, ok := netip.AddrFromSlice(ip)
		if !ok {
			continue
		}
		parsed = parsed.Unmap()
		if parsed.Is6() && parsed.IsLinkLocalUnicast() && parsed.Zone() == "" {
			entry.UnscopedLinkLocalObserved = true
			continue
		}
		entry.ScopedAddresses = append(entry.ScopedAddresses, parsed)
		if !(parsed.Is6() && parsed.IsLinkLocalUnicast()) {
			entry.Addresses = append(entry.Addresses, append(net.IP(nil), parsed.AsSlice()...))
		}
	}
	return entry
}

func candidateFrom(entry *shipapi.MdnsEntry, ref string) shipapi.PairingCandidateObservation {
	return shipapi.PairingCandidateObservation{
		CandidateRef:              ref,
		Name:                      entry.Name,
		SKI:                       entry.Ski,
		Identifier:                entry.Identifier,
		Brand:                     entry.Brand,
		Type:                      entry.Type,
		Model:                     entry.Model,
		Path:                      entry.Path,
		Host:                      entry.Host,
		Port:                      entry.Port,
		Register:                  entry.Register,
		Addresses:                 append([]net.IP(nil), entry.Addresses...),
		ScopedAddresses:           append([]netip.Addr(nil), entry.ScopedAddresses...),
		UnscopedLinkLocalObserved: entry.UnscopedLinkLocalObserved,
	}
}

func newCandidateRef() string {
	var buf [8]byte
	if _, err := rand.Read(buf[:]); err != nil {
		return "shipc_local"
	}
	return "shipc_" + hex.EncodeToString(buf[:])
}

// installHubBrowse replaces the hub mDNS before start. The service constructor
// has no other place to supply one, and the replacement is what reports each
// discovered SHIP service back to this instance.
func installHubBrowse(svc *service.Service, browse *shipBrowse) error {
	if svc == nil || browse == nil {
		return errBrowse
	}
	field := unexportedField(svc, "connectionsHub")
	if !field.IsValid() || field.Kind() != reflect.Interface || field.IsNil() {
		return errBrowse
	}
	hub := field.Elem()
	if !hub.IsValid() {
		return errBrowse
	}
	if hub.Kind() == reflect.Pointer {
		if hub.IsNil() {
			return errBrowse
		}
		hub = hub.Elem()
	}
	if !hub.IsValid() || hub.Kind() != reflect.Struct {
		return errBrowse
	}
	mdnsField := hub.FieldByName("mdns")
	if !mdnsField.IsValid() || !mdnsField.CanAddr() {
		return errBrowse
	}
	target := reflect.NewAt(mdnsField.Type(), unsafe.Pointer(mdnsField.UnsafeAddr())).Elem()
	value := reflect.ValueOf(shipapi.MdnsInterface(browse))
	if !value.Type().AssignableTo(target.Type()) {
		return errBrowse
	}
	target.Set(value)
	return nil
}

func unexportedField(target any, name string) reflect.Value {
	value := reflect.ValueOf(target)
	if value.Kind() != reflect.Pointer || value.IsNil() {
		return reflect.Value{}
	}
	value = value.Elem()
	field := value.FieldByName(name)
	if !field.IsValid() || !field.CanAddr() {
		return reflect.Value{}
	}
	return reflect.NewAt(field.Type(), unsafe.Pointer(field.UnsafeAddr())).Elem()
}
