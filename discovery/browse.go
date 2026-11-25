package discovery

import (
	"context"
	"net"
	"strings"
	"time"
)

const (
	dnsTypeA    = 1
	dnsTypePTR  = 12
	dnsTypeTXT  = 16
	dnsTypeAAAA = 28
	dnsTypeSRV  = 33

	shipServiceName   = "_ship._tcp.local"
	shipServiceSuffix = "._ship._tcp.local"

	// shipProbeInterval is the pause between solicited reads for the SHIP §7
	// service. A single query misses an announcement that starts later.
	shipProbeInterval = 500 * time.Millisecond
)

// Sighting is one discovered SHIP service and the addresses named with it.
type Sighting struct {
	Announcement Announcement
	Addresses    []net.IP
}

type shipService struct {
	instance string
	host     string
	port     int
	txt      map[string]string
	ips      []net.IP
	expiry   time.Time
	haveSRV  bool
	haveTXT  bool
}

// Browser solicits and reads SHIP Specification 1.1.0 §7 mDNS records for one
// instance. It keeps no process-wide state. Shutdown closes its socket and
// returns; it does not wait on a browse the caller has stopped reading.
type Browser struct {
	localSKI string
	onChange func([]Sighting)
	last     []Sighting

	services map[string]*shipService
	pending  map[string][]net.IP

	conn   *net.UDPConn
	cancel context.CancelFunc
	done   chan struct{}
}

// NewBrowser returns a browser that ignores announcements for localSKI.
func NewBrowser(localSKI string) *Browser {
	return &Browser{
		localSKI: strings.ToLower(localSKI),
		services: make(map[string]*shipService),
		pending:  make(map[string][]net.IP),
	}
}

// Start listens for SHIP service responses and reports the current set whenever
// it changes. onChange may be nil.
func (b *Browser) Start(onChange func([]Sighting)) error {
	if b.conn != nil {
		b.onChange = onChange
		return nil
	}
	conn, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4zero, Port: 0})
	if err != nil {
		return err
	}
	ctx, cancel := context.WithCancel(context.Background())
	b.conn = conn
	b.cancel = cancel
	b.onChange = onChange
	b.done = make(chan struct{})
	go b.loop(ctx, conn, b.done)
	return nil
}

// Stop closes the browse socket and waits until the read loop has returned.
func (b *Browser) Stop() {
	if b.cancel != nil {
		b.cancel()
		b.cancel = nil
	}
	if b.conn != nil {
		_ = b.conn.Close()
		b.conn = nil
	}
	if b.done != nil {
		<-b.done
		b.done = nil
	}
}

func (b *Browser) loop(ctx context.Context, conn *net.UDPConn, done chan struct{}) {
	defer close(done)
	writeSHIPQuery(conn)
	buf := make([]byte, 65535)
	for {
		if ctx.Err() != nil {
			return
		}
		_ = conn.SetReadDeadline(time.Now().Add(shipProbeInterval))
		n, _, err := conn.ReadFromUDP(buf)
		if n > 0 {
			b.ingest(buf[:n])
		}
		if err == nil {
			continue
		}
		if ctx.Err() != nil || isClosed(err) {
			return
		}
		writeSHIPQuery(conn)
	}
}

func isClosed(err error) bool {
	return err != nil && strings.Contains(err.Error(), "closed")
}

func writeSHIPQuery(conn *net.UDPConn) {
	dst := &net.UDPAddr{IP: net.IPv4(224, 0, 0, 251), Port: 5353}
	_, _ = conn.WriteToUDP(shipBrowseQuery(), dst)
}

func shipBrowseQuery() []byte {
	name := []byte{
		5, '_', 's', 'h', 'i', 'p',
		4, '_', 't', 'c', 'p',
		5, 'l', 'o', 'c', 'a', 'l',
		0,
	}
	msg := make([]byte, 12+len(name)+4)
	msg[5] = 1
	copy(msg[12:], name)
	off := 12 + len(name)
	msg[off], msg[off+1] = 0, dnsTypePTR
	msg[off+2], msg[off+3] = 0x80, 1
	return msg
}

func (b *Browser) ingest(packet []byte) {
	records, ok := decodePacket(packet)
	if !ok {
		return
	}
	now := time.Now()
	changed := b.prune(now)
	for _, record := range records {
		if b.apply(record, now) {
			changed = true
		}
	}
	if b.prune(now) {
		changed = true
	}
	next := b.snapshot(now)
	if !changed || sameSightings(b.last, next) {
		b.last = next
		return
	}
	b.last = next
	if b.onChange != nil {
		b.onChange(next)
	}
}

func (b *Browser) prune(now time.Time) bool {
	changed := false
	for key, svc := range b.services {
		if !svc.expiry.IsZero() && !now.Before(svc.expiry) {
			delete(b.services, key)
			changed = true
		}
	}
	return changed
}

func (b *Browser) apply(rr dnsRR, now time.Time) bool {
	switch rr.typ {
	case dnsTypePTR:
		if normalizeDNSName(rr.name) != shipServiceName {
			return false
		}
		if rr.ttl == 0 {
			return b.drop(rr.target)
		}
		return false
	case dnsTypeSRV:
		if rr.ttl == 0 {
			return b.drop(rr.name)
		}
		svc := b.ship(rr.name)
		if svc == nil {
			return false
		}
		host := strings.TrimSuffix(rr.target, ".")
		svc.host = host
		svc.port = rr.port
		svc.haveSRV = true
		svc.expiry = now.Add(time.Duration(rr.ttl) * time.Second)
		if ips := b.pending[normalizeDNSName(host)]; len(ips) > 0 {
			for _, ip := range ips {
				svc.ips = addIP(svc.ips, ip)
			}
		}
		return true
	case dnsTypeTXT:
		svc := b.ship(rr.name)
		if svc == nil {
			return false
		}
		if rr.ttl == 0 {
			return b.drop(rr.name)
		}
		svc.txt = txtMap(rr.txt)
		svc.haveTXT = true
		svc.expiry = now.Add(time.Duration(rr.ttl) * time.Second)
		return true
	case dnsTypeA, dnsTypeAAAA:
		if len(rr.ip) == 0 {
			return false
		}
		if rr.ip.IsLoopback() {
			return false
		}
		if rr.ip.To16() != nil && rr.ip.To4() == nil && rr.ip.IsLinkLocalUnicast() {
			return false
		}
		key := normalizeDNSName(rr.name)
		if rr.ttl == 0 {
			return false
		}
		b.pending[key] = addIP(b.pending[key], rr.ip)
		changed := false
		for _, svc := range b.services {
			if svc.host != "" && normalizeDNSName(svc.host) == key {
				next := addIP(svc.ips, rr.ip)
				if len(next) != len(svc.ips) {
					svc.ips = next
					changed = true
				}
			}
		}
		return changed
	default:
		return false
	}
}

func (b *Browser) ship(name string) *shipService {
	key := normalizeDNSName(name)
	if !strings.HasSuffix(key, shipServiceSuffix) {
		return nil
	}
	svc := b.services[key]
	if svc == nil {
		svc = &shipService{instance: strings.TrimSuffix(name, ".")}
		b.services[key] = svc
	}
	return svc
}

func (b *Browser) drop(name string) bool {
	key := normalizeDNSName(name)
	if _, ok := b.services[key]; !ok {
		return false
	}
	delete(b.services, key)
	return true
}

func (b *Browser) snapshot(now time.Time) []Sighting {
	out := make([]Sighting, 0, len(b.services))
	for _, svc := range b.services {
		if !svc.expiry.IsZero() && !now.Before(svc.expiry) {
			continue
		}
		sighting, ok := svc.sighting(b.localSKI)
		if !ok {
			continue
		}
		out = append(out, sighting)
	}
	sortSightings(out)
	return out
}

func (svc *shipService) sighting(localSKI string) (Sighting, bool) {
	if !svc.haveSRV || !svc.haveTXT || svc.port <= 0 || svc.txt["txtvers"] != "1" {
		return Sighting{}, false
	}
	ski := strings.ToLower(svc.txt["ski"])
	if !validSKI(ski) || ski == localSKI || svc.txt["id"] == "" || svc.txt["path"] == "" {
		return Sighting{}, false
	}
	register, ok := shipRegister(svc.txt["register"])
	if !ok {
		return Sighting{}, false
	}
	ips := make([]net.IP, len(svc.ips))
	for i, ip := range svc.ips {
		ips[i] = append(net.IP(nil), ip...)
	}
	return Sighting{
		Announcement: Announcement{
			Name: instanceLabel(svc.instance),
			Host: strings.TrimSuffix(svc.host, "."),
			Port: svc.port,
			TXT: TXT{
				TXTVers:  "1",
				ID:       svc.txt["id"],
				Path:     svc.txt["path"],
				SKI:      ski,
				Register: register,
				Brand:    svc.txt["brand"],
				Type:     svc.txt["type"],
				Model:    svc.txt["model"],
			},
		},
		Addresses: ips,
	}, true
}

func instanceLabel(name string) string {
	trimmed := strings.TrimSuffix(name, ".")
	key := normalizeDNSName(trimmed)
	if strings.HasSuffix(key, shipServiceSuffix) {
		return trimmed[:len(key)-len(shipServiceSuffix)]
	}
	return trimmed
}

func normalizeDNSName(name string) string {
	return strings.ToLower(strings.TrimSuffix(name, "."))
}

func validSKI(ski string) bool {
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

func shipRegister(value string) (bool, bool) {
	switch value {
	case "true":
		return true, true
	case "false":
		return false, true
	default:
		return false, false
	}
}

func txtMap(values []string) map[string]string {
	out := make(map[string]string, len(values))
	for _, value := range values {
		parts := strings.SplitN(value, "=", 2)
		if len(parts) != 2 || parts[0] == "" {
			continue
		}
		out[parts[0]] = parts[1]
	}
	return out
}

func addIP(ips []net.IP, ip net.IP) []net.IP {
	for _, have := range ips {
		if have.Equal(ip) {
			return ips
		}
	}
	return append(ips, append(net.IP(nil), ip...))
}

func sortSightings(out []Sighting) {
	for i := 1; i < len(out); i++ {
		item := out[i]
		j := i
		for j > 0 && out[j-1].Announcement.TXT.SKI > item.Announcement.TXT.SKI {
			out[j] = out[j-1]
			j--
		}
		out[j] = item
	}
}

func sameSightings(left, right []Sighting) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i].Announcement.Name != right[i].Announcement.Name ||
			left[i].Announcement.Host != right[i].Announcement.Host ||
			left[i].Announcement.Port != right[i].Announcement.Port ||
			left[i].Announcement.TXT != right[i].Announcement.TXT ||
			len(left[i].Addresses) != len(right[i].Addresses) {
			return false
		}
		for n := range left[i].Addresses {
			if !left[i].Addresses[n].Equal(right[i].Addresses[n]) {
				return false
			}
		}
	}
	return true
}

type dnsRR struct {
	name   string
	typ    uint16
	ttl    uint32
	target string
	port   int
	txt    []string
	ip     net.IP
}

func decodePacket(packet []byte) ([]dnsRR, bool) {
	if len(packet) < 12 || packet[2]&0x80 == 0 {
		return nil, false
	}
	questions := int(packet[4])<<8 | int(packet[5])
	total := int(packet[6])<<8 | int(packet[7])
	total += int(packet[8])<<8 | int(packet[9])
	total += int(packet[10])<<8 | int(packet[11])
	offset := 12
	var ok bool
	for i := 0; i < questions; i++ {
		_, offset, ok = decodeName(packet, offset, 0)
		if !ok || offset+4 > len(packet) {
			return nil, false
		}
		offset += 4
	}
	out := make([]dnsRR, 0, total)
	for i := 0; i < total; i++ {
		var record dnsRR
		record, offset, ok = decodeRR(packet, offset)
		if !ok {
			return nil, false
		}
		out = append(out, record)
	}
	return out, true
}

func decodeRR(packet []byte, offset int) (dnsRR, int, bool) {
	name, offset, ok := decodeName(packet, offset, 0)
	if !ok || offset+10 > len(packet) {
		return dnsRR{}, offset, false
	}
	typ := uint16(packet[offset])<<8 | uint16(packet[offset+1])
	ttl := uint32(packet[offset+4])<<24 | uint32(packet[offset+5])<<16 | uint32(packet[offset+6])<<8 | uint32(packet[offset+7])
	rdlen := int(packet[offset+8])<<8 | int(packet[offset+9])
	offset += 10
	if rdlen < 0 || offset+rdlen > len(packet) {
		return dnsRR{}, offset, false
	}
	next := offset + rdlen
	record := dnsRR{name: name, typ: typ, ttl: ttl}
	switch typ {
	case dnsTypePTR:
		target, _, good := decodeName(packet, offset, 0)
		if !good {
			return dnsRR{}, next, false
		}
		record.target = target
	case dnsTypeSRV:
		if rdlen < 7 {
			return dnsRR{}, next, false
		}
		record.port = int(packet[offset+4])<<8 | int(packet[offset+5])
		target, _, good := decodeName(packet, offset+6, 0)
		if !good {
			return dnsRR{}, next, false
		}
		record.target = target
	case dnsTypeTXT:
		record.txt = decodeTXT(packet[offset:next])
	case dnsTypeA:
		if rdlen == 4 {
			record.ip = net.IP(append([]byte(nil), packet[offset:next]...))
		}
	case dnsTypeAAAA:
		if rdlen == 16 {
			record.ip = net.IP(append([]byte(nil), packet[offset:next]...))
		}
	}
	return record, next, true
}

func decodeTXT(rdata []byte) []string {
	var out []string
	for len(rdata) > 0 {
		n := int(rdata[0])
		rdata = rdata[1:]
		if n > len(rdata) {
			return out
		}
		out = append(out, string(rdata[:n]))
		rdata = rdata[n:]
	}
	return out
}

func decodeName(packet []byte, offset, depth int) (string, int, bool) {
	if depth > 10 || offset < 0 || offset >= len(packet) {
		return "", offset, false
	}
	var b strings.Builder
	end := offset
	jumped := false
	for steps := 0; steps < 128; steps++ {
		if offset >= len(packet) {
			return "", end, false
		}
		length := int(packet[offset])
		if length == 0 {
			if !jumped {
				end = offset + 1
			}
			return b.String(), end, true
		}
		if length&0xC0 == 0xC0 {
			if offset+1 >= len(packet) {
				return "", end, false
			}
			if !jumped {
				end = offset + 2
				jumped = true
			}
			ptr := int(length&0x3F)<<8 | int(packet[offset+1])
			rest, _, ok := decodeName(packet, ptr, depth+1)
			if !ok {
				return "", end, false
			}
			if rest != "" {
				if b.Len() > 0 {
					b.WriteByte('.')
				}
				b.WriteString(rest)
			}
			return b.String(), end, true
		}
		if length&0xC0 != 0 || offset+1+length > len(packet) {
			return "", end, false
		}
		if b.Len() > 0 {
			b.WriteByte('.')
		}
		b.Write(packet[offset+1 : offset+1+length])
		offset += 1 + length
		if !jumped {
			end = offset
		}
	}
	return "", end, false
}
