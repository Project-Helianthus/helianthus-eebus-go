package discovery

import (
	"sync"
	"testing"
	"time"

	"github.com/Project-Helianthus/helianthus-ship-go/mdns"
)

func TestParsedSHIPRecordKeepsTXTAndPort(t *testing.T) {
	const ski = "dddddddddddddddddddddddddddddddddddddddd"
	packet := shipResponse(t, "peer", "host.local", 4791, ski, true)
	browser := NewBrowser("eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee")
	var got []Sighting
	browser.onChange = func(sightings []Sighting) { got = sightings }
	browser.ingest(packet)
	if len(got) != 1 {
		t.Fatalf("sightings = %+v", got)
	}
	remote := got[0].Announcement
	if remote.Name != "peer" || remote.Host != "host.local" || remote.Port != 4791 {
		t.Fatalf("announcement = %+v", remote)
	}
	if remote.TXT.SKI != ski || remote.TXT.Path != WebsocketPath || !remote.TXT.Register || remote.TXT.ID != "id-peer" {
		t.Fatalf("txt = %+v", remote.TXT)
	}
	if len(got[0].Addresses) != 1 || !got[0].Addresses[0].Equal([]byte{192, 0, 2, 10}) {
		t.Fatalf("addresses = %v", got[0].Addresses)
	}
}

func TestBrowserDiscoversAnnouncedSHIPService(t *testing.T) {
	const ski = "dddddddddddddddddddddddddddddddddddddddd"
	browser := NewBrowser("eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee")
	var mu sync.Mutex
	var seen []Sighting
	if err := browser.Start(func(sightings []Sighting) {
		mu.Lock()
		seen = append([]Sighting(nil), sightings...)
		mu.Unlock()
	}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(browser.Stop)

	provider := mdns.NewZeroconfProvider(nil)
	t.Cleanup(provider.Shutdown)
	txt := TXT{
		ID:       "id-peer",
		SKI:      ski,
		Register: true,
		Brand:    "helianthus",
		Type:     "EnergyManagementSystem",
		Model:    "stack",
	}
	if err := provider.Announce("peer-service", 4791, txt.Records()); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		got := append([]Sighting(nil), seen...)
		mu.Unlock()
		for _, item := range got {
			ann := item.Announcement
			if ann.TXT.SKI == ski && ann.Port == 4791 && ann.TXT.Register && ann.TXT.Path == WebsocketPath && ann.Host != "" {
				return
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	mu.Lock()
	defer mu.Unlock()
	t.Fatalf("sightings = %+v", seen)
}

func shipResponse(t *testing.T, instance, host string, port int, ski string, register bool) []byte {
	t.Helper()
	registerText := "register=false"
	if register {
		registerText = "register=true"
	}
	txt := encodeTXT(
		"txtvers=1",
		"path="+WebsocketPath,
		"id=id-peer",
		"ski="+ski,
		"brand=helianthus",
		"model=stack",
		"type=EnergyManagementSystem",
		registerText,
	)
	srv := make([]byte, 6)
	srv[4] = byte(port >> 8)
	srv[5] = byte(port)
	srv = appendName(srv, host)
	body := []byte{0, 0, 0x84, 0, 0, 0, 0, 3, 0, 0, 0, 1}
	body = appendRR(body, instance+"."+shipServiceName, dnsTypeSRV, 120, srv)
	instanceAt := 12
	body = appendPointerRR(body, instanceAt, dnsTypeTXT, 120, txt)
	body = appendRR(body, shipServiceName, dnsTypePTR, 120, pointer(instanceAt))
	body = appendRR(body, host, dnsTypeA, 120, []byte{192, 0, 2, 10})
	body[6] = 0
	body[7] = 3
	body[10] = 0
	body[11] = 1
	return body
}

func appendRR(buf []byte, name string, typ uint16, ttl uint32, data []byte) []byte {
	buf = appendName(buf, name)
	return appendRRTail(buf, typ, ttl, data)
}

func appendPointerRR(buf []byte, target int, typ uint16, ttl uint32, data []byte) []byte {
	buf = append(buf, pointer(target)...)
	return appendRRTail(buf, typ, ttl, data)
}

func appendRRTail(buf []byte, typ uint16, ttl uint32, data []byte) []byte {
	buf = append(buf, byte(typ>>8), byte(typ), 0, 1)
	buf = append(buf, byte(ttl>>24), byte(ttl>>16), byte(ttl>>8), byte(ttl))
	buf = append(buf, byte(len(data)>>8), byte(len(data)))
	return append(buf, data...)
}

func appendName(buf []byte, name string) []byte {
	if name == "" {
		return append(buf, 0)
	}
	for _, label := range splitDot(name) {
		buf = append(buf, byte(len(label)))
		buf = append(buf, label...)
	}
	return append(buf, 0)
}

func splitDot(name string) []string {
	var out []string
	start := 0
	for i := 0; i <= len(name); i++ {
		if i == len(name) || name[i] == '.' {
			out = append(out, name[start:i])
			start = i + 1
		}
	}
	return out
}

func pointer(offset int) []byte {
	return []byte{0xC0 | byte(offset>>8), byte(offset)}
}

func encodeTXT(values ...string) []byte {
	var buf []byte
	for _, value := range values {
		buf = append(buf, byte(len(value)))
		buf = append(buf, value...)
	}
	return buf
}
