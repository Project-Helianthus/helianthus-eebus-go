package discovery

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
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
