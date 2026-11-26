package discovery

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/enbility/zeroconf/v2"
)

func TestBrowserReportsBrowseFailure(t *testing.T) {
	var logs bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, nil)))
	t.Cleanup(func() { slog.SetDefault(previous) })

	failure := errors.New("browse socket unavailable")
	failBrowse := func(context.Context, string, string, chan<- *zeroconf.ServiceEntry, chan<- *zeroconf.ServiceEntry, ...zeroconf.ClientOption) error {
		return failure
	}
	browser := NewBrowser("")
	if err := browser.start(nil, failBrowse); err != nil {
		t.Fatal(err)
	}
	browser.Stop()
	if got := logs.String(); !strings.Contains(got, "mDNS browse failed") || !strings.Contains(got, failure.Error()) {
		t.Fatalf("browse failure not reported: %q", got)
	}
}

func TestAnnouncementKeepsResolvedAddressesForSHIP(t *testing.T) {
	const ski = "abababababababababababababababababababab"
	entry := &zeroconf.ServiceEntry{
		Port: 4791, HostName: "offline.local.",
		AddrIPv4: []net.IP{net.ParseIP("192.0.2.10")},
		AddrIPv6: []net.IP{net.ParseIP("2001:db8::10")},
	}
	entry.Instance = "offline-peer"
	entry.Text = TXT{ID: "id-peer", SKI: ski, Register: true}.Records()
	ann, ok := announcementFrom(entry)
	if !ok {
		t.Fatal("valid SHIP announcement rejected")
	}
	got := ann.SHIPEntry()
	if got.Host != "offline.local" || len(got.Addresses) != 2 ||
		!got.Addresses[0].Equal(net.ParseIP("192.0.2.10")) || !got.Addresses[1].Equal(net.ParseIP("2001:db8::10")) {
		t.Fatalf("SHIP endpoint: host=%q addresses=%v", got.Host, got.Addresses)
	}
}

func TestBrowserConsumesUnbufferedRecordsThroughShutdown(t *testing.T) {
	const ski = "abababababababababababababababababababab"
	entry := &zeroconf.ServiceEntry{Port: 4791, HostName: "offline.local."}
	entry.Instance = "offline-peer"
	entry.Text = TXT{ID: "id-peer", SKI: ski, Register: true}.Records()

	type browseResult struct{ entries, removed int }
	result := make(chan browseResult, 1)
	browse := func(ctx context.Context, _, _ string, entries, removed chan<- *zeroconf.ServiceEntry, _ ...zeroconf.ClientOption) error {
		result <- browseResult{cap(entries), cap(removed)}
		entries <- entry
		<-ctx.Done()
		removed <- entry
		return ctx.Err()
	}
	seen := make(chan []Announcement, 1)
	browser := NewBrowser("")
	if err := browser.start(func(found []Announcement) { seen <- found }, browse); err != nil {
		t.Fatal(err)
	}
	select {
	case found := <-seen:
		if len(found) != 1 || found[0].TXT.SKI != ski {
			t.Fatalf("observed records = %+v", found)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("record was not delivered")
	}
	stopped := make(chan struct{})
	go func() {
		browser.Stop()
		close(stopped)
	}()
	select {
	case <-stopped:
	case <-time.After(5 * time.Second):
		t.Fatal("Stop did not drain the final removal")
	}
	if got := <-result; got != (browseResult{}) {
		t.Fatalf("record channels are buffered: entries=%d removed=%d", got.entries, got.removed)
	}
	browser.mu.Lock()
	defer browser.mu.Unlock()
	if len(browser.found) != 0 {
		t.Fatal("final removal was not applied")
	}
}

func TestBrowserStopReturnsWhilePeerRecordIsInFlight(t *testing.T) {
	const ski = "abababababababababababababababababababab"
	registration, err := Register(Announcement{
		Name: "offline-peer",
		Port: 4791,
		TXT: TXT{
			ID:       "id-peer",
			SKI:      ski,
			Register: true,
			Brand:    "helianthus",
			Type:     "EnergyManagementSystem",
			Model:    "stack",
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(registration.Close)

	seen := make(chan Announcement, 1)
	browser := NewBrowser("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
	if err := browser.Start(func(found []Announcement) {
		for _, ann := range found {
			if ann.TXT.SKI == ski {
				select {
				case seen <- ann:
				default:
				}
			}
		}
	}); err != nil {
		t.Fatal(err)
	}

	select {
	case ann := <-seen:
		if ann.Name != "offline-peer" || ann.Port != 4791 || ann.Host == "" || !ann.TXT.Register || ann.TXT.Path != WebsocketPath {
			t.Fatalf("announcement = %+v", ann)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("peer SHIP service was not browsed")
	}

	stopped := make(chan struct{})
	go func() {
		browser.Stop()
		close(stopped)
	}()
	select {
	case <-stopped:
	case <-time.After(5 * time.Second):
		t.Fatal("Stop did not return while a peer SHIP record was in flight")
	}
}
