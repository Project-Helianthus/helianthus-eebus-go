package session

import (
	"testing"

	"github.com/Project-Helianthus/helianthus-eebus-go/discovery"
	shipapi "github.com/Project-Helianthus/helianthus-ship-go/api"
)

type shipDiscoveryReport struct {
	entries    map[string]*shipapi.MdnsEntry
	newEntries bool
	calls      int
}

func (r *shipDiscoveryReport) ReportMdnsEntries(entries map[string]*shipapi.MdnsEntry, newEntries bool) {
	r.entries = entries
	r.newEntries = newEntries
	r.calls++
}

func TestDiscoveryReportsAndReplaysLatestSHIPSnapshot(t *testing.T) {
	instance := prepareTestInstance(t, "snapshot", nil)
	var report shipDiscoveryReport
	if err := instance.mdns.Start(&report); err != nil {
		t.Fatal(err)
	}

	ann := discovery.Announcement{
		Name: "offline-peer", Host: "offline.local", Port: 4791,
		TXT: discovery.TXT{SKI: skiRemoteA, ID: "id-peer", Path: discovery.WebsocketPath, Register: true},
	}
	instance.noteAnnouncements([]discovery.Announcement{ann})
	check := func(calls int, newEntries bool) {
		t.Helper()
		entry := report.entries[skiRemoteA]
		if report.calls != calls || report.newEntries != newEntries || len(report.entries) != 1 ||
			entry == nil || entry.Ski != skiRemoteA || entry.Name != ann.Name || entry.Host != ann.Host ||
			entry.Port != ann.Port || entry.Path != ann.TXT.Path || entry.Identifier != ann.TXT.ID || !entry.Register {
			t.Fatalf("mDNS report: calls=%d new=%t entries=%+v peer=%+v", report.calls, report.newEntries, report.entries, entry)
		}
	}
	check(1, true)
	if err := instance.Trust(skiRemoteA, true); err != nil {
		t.Fatal(err)
	}
	check(2, false)

	instance.noteAnnouncements(nil)
	instance.mdns.RequestMdnsEntries()
	if report.calls != 4 || report.newEntries || len(report.entries) != 0 {
		t.Fatalf("removed peer was replayed: calls=%d new=%t entries=%+v", report.calls, report.newEntries, report.entries)
	}
}
