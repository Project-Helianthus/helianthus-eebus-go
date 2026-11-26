package session

import (
	"sync"

	"github.com/Project-Helianthus/helianthus-eebus-go/discovery"
	shipapi "github.com/Project-Helianthus/helianthus-ship-go/api"
)

// shipDiscovery feeds the instance's browser results to the SHIP hub. The
// instance owns announcement and browse lifetimes so the browser can drain
// records when it stops. SHIP owns trust, connection initiation, and retries.
type shipDiscovery struct {
	mu      sync.Mutex
	report  shipapi.MdnsReportInterface
	entries map[string]*shipapi.MdnsEntry
}

func (d *shipDiscovery) Start(report shipapi.MdnsReportInterface) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.report = report
	return nil
}

func (d *shipDiscovery) Shutdown() {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.report = nil
	d.entries = nil
}

func (*shipDiscovery) AnnounceMdnsEntry() error { return nil }
func (*shipDiscovery) UnannounceMdnsEntry()     {}
func (*shipDiscovery) SetAutoAccept(bool)       {}

func (d *shipDiscovery) replace(found []discovery.Announcement) {
	entries := make(map[string]*shipapi.MdnsEntry, len(found))
	for _, announcement := range found {
		entry := announcement.SHIPEntry()
		entries[entry.Ski] = entry
	}

	d.mu.Lock()
	defer d.mu.Unlock()
	d.entries = entries
	if d.report != nil {
		d.report.ReportMdnsEntries(d.entries, true)
	}
}

// RequestMdnsEntries replays the latest snapshot when a discovered peer is
// trusted after the browse result arrived or when the hub needs to reconnect.
func (d *shipDiscovery) RequestMdnsEntries() {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.report != nil {
		d.report.ReportMdnsEntries(d.entries, false)
	}
}
