package discovery

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"

	shiputil "github.com/Project-Helianthus/helianthus-ship-go/util"
	"github.com/enbility/zeroconf/v2"
)

var errBrowsing = errors.New("ship browse already started")

// SHIP Specification 1.1.0 §7 announces this service. The TTL is the two
// minutes the pinned SHIP provider uses for that announcement.
const (
	shipServiceType = "_ship._tcp"
	shipDomain      = "local."
	shipTTL         = 120
)

// Registration is one instance's SHIP announcement.
type Registration struct {
	once   sync.Once
	server *zeroconf.Server
}

// Register publishes ann on the SHIP mDNS service.
func Register(ann Announcement) (*Registration, error) {
	server, err := zeroconf.Register(
		ann.Name,
		shipServiceType,
		shipDomain,
		ann.Port,
		ann.TXT.Records(),
		nil,
		zeroconf.TTL(shipTTL),
	)
	if err != nil {
		return nil, err
	}
	return &Registration{server: server}, nil
}

// Close withdraws the announcement and returns.
func (r *Registration) Close() {
	if r == nil {
		return
	}
	r.once.Do(func() {
		if r.server != nil {
			r.server.Shutdown()
		}
	})
}

// Browser reports SHIP services other than localSKI.
// Stop keeps reading until the browse returns. The browse sends on an
// unbuffered channel and only leaves that send after its context ends, so a
// reader that stops when the context ends waits forever while a record is in flight.
type Browser struct {
	once     sync.Once
	mu       sync.Mutex
	localSKI string
	found    map[string]Announcement
	on       func([]Announcement)
	cancel   context.CancelFunc
	done     chan struct{}
}

// NewBrowser returns a browser that ignores localSKI.
func NewBrowser(localSKI string) *Browser {
	return &Browser{
		localSKI: shiputil.NormalizeSKI(localSKI),
		found:    make(map[string]Announcement),
	}
}

// Start begins browsing. The callback receives the current set.
func (b *Browser) Start(on func([]Announcement)) error {
	return b.start(on, zeroconf.Browse)
}

type browseFunc func(context.Context, string, string, chan<- *zeroconf.ServiceEntry, chan<- *zeroconf.ServiceEntry, ...zeroconf.ClientOption) error

func (b *Browser) start(on func([]Announcement), browse browseFunc) error {
	b.mu.Lock()
	if b.cancel != nil {
		b.mu.Unlock()
		return errBrowsing
	}
	ctx, cancel := context.WithCancel(context.Background())
	b.cancel = cancel
	b.on = on
	b.done = make(chan struct{})
	done := b.done
	b.mu.Unlock()
	go b.loop(ctx, done, browse)
	return nil
}

// Stop returns while peer records are still being sent.
func (b *Browser) Stop() {
	if b == nil {
		return
	}
	b.once.Do(func() {
		b.mu.Lock()
		cancel := b.cancel
		done := b.done
		b.on = nil
		b.mu.Unlock()
		if cancel != nil {
			cancel()
		}
		if done != nil {
			<-done
		}
	})
}

func (b *Browser) loop(ctx context.Context, done chan struct{}, browse browseFunc) {
	defer close(done)
	entries := make(chan *zeroconf.ServiceEntry, 32)
	removed := make(chan *zeroconf.ServiceEntry, 32)
	browseDone := make(chan error)
	go func() {
		browseDone <- browse(ctx, shipServiceType, shipDomain, entries, removed)
	}()
	for {
		select {
		case err := <-browseDone:
			if err != nil && !errors.Is(err, context.Canceled) {
				slog.Error("SHIP mDNS browse failed", "error", err)
			}
			return
		case entry, ok := <-entries:
			if !ok {
				entries = nil
				continue
			}
			b.observe(entry, false)
		case entry, ok := <-removed:
			if !ok {
				removed = nil
				continue
			}
			b.observe(entry, true)
		}
	}
}

func (b *Browser) observe(entry *zeroconf.ServiceEntry, remove bool) {
	ann, ok := announcementFrom(entry)
	if !ok || ann.TXT.SKI == b.localSKI {
		return
	}
	b.mu.Lock()
	if remove {
		delete(b.found, ann.TXT.SKI)
	} else {
		b.found[ann.TXT.SKI] = ann
	}
	snapshot := make([]Announcement, 0, len(b.found))
	for _, item := range b.found {
		snapshot = append(snapshot, item)
	}
	on := b.on
	b.mu.Unlock()
	if on != nil {
		on(snapshot)
	}
}

func announcementFrom(entry *zeroconf.ServiceEntry) (Announcement, bool) {
	if entry == nil || entry.Port <= 0 || entry.Instance == "" || len(entry.Text) == 0 {
		return Announcement{}, false
	}
	elements := make(map[string]string, len(entry.Text))
	for _, item := range entry.Text {
		key, value, found := strings.Cut(item, "=")
		if !found || key == "" {
			continue
		}
		elements[key] = value
	}
	register := elements["register"]
	if elements["txtvers"] != "1" || elements["id"] == "" || elements["path"] == "" {
		return Announcement{}, false
	}
	if register != "true" && register != "false" {
		return Announcement{}, false
	}
	ski := shiputil.NormalizeSKI(elements["ski"])
	if !isSKI(ski) {
		return Announcement{}, false
	}
	host := strings.TrimSuffix(entry.HostName, ".")
	if host == "" {
		return Announcement{}, false
	}
	return Announcement{
		Name: strings.TrimSuffix(entry.Instance, "."),
		Host: host,
		Port: entry.Port,
		TXT: TXT{
			TXTVers:  "1",
			ID:       elements["id"],
			Path:     elements["path"],
			SKI:      ski,
			Register: register == "true",
			Brand:    elements["brand"],
			Type:     elements["type"],
			Model:    elements["model"],
		},
	}, true
}

func isSKI(ski string) bool {
	if len(ski) != 40 {
		return false
	}
	for _, c := range ski {
		switch {
		case c >= '0' && c <= '9', c >= 'a' && c <= 'f':
		default:
			return false
		}
	}
	return true
}
