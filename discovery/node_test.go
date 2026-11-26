package discovery

import (
	"testing"

	shipapi "github.com/Project-Helianthus/helianthus-ship-go/api"
)

func announcement(ski string, port int, register bool) Announcement {
	return Announcement{
		Name: "node-" + ski[:4],
		Host: "local",
		Port: port,
		TXT: TXT{
			ID:       "id-" + ski[:4],
			SKI:      ski,
			Register: register,
			Brand:    "helianthus",
			Type:     "EnergyManagementSystem",
			Model:    "stack",
		},
	}
}

func TestInstancesAnnounceDiscoverEachOtherAndUnpairedPeer(t *testing.T) {
	const (
		skiA = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
		skiB = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
		skiC = "cccccccccccccccccccccccccccccccccccccccc"
	)
	directory := NewDirectory()
	nodeA := NewNode(announcement(skiA, 4711, true))
	nodeB := NewNode(announcement(skiB, 4712, false))
	directory.Join(nodeA)
	directory.Join(nodeB)
	directory.Publish(nodeA)
	directory.Publish(nodeB)

	third := announcement(skiC, 4713, true)
	nodeA.Observe(third, shipapi.ConnectionStateInProgress, false)
	nodeB.Observe(third, shipapi.ConnectionStateNone, false)

	assertDiscovered(t, nodeA, skiB, false, shipapi.ConnectionStateNone)
	assertDiscovered(t, nodeB, skiA, false, shipapi.ConnectionStateNone)
	remote, ok := nodeA.Remote(skiC)
	if !ok {
		t.Fatal("missing unpaired remote")
	}
	if remote.Paired || remote.Trusted || remote.Connection != shipapi.ConnectionStateInProgress {
		t.Fatalf("unpaired remote = %+v", remote)
	}
	if remote.Announcement.TXT.SKI != skiC || remote.Announcement.Port != 4713 || remote.Announcement.TXT.Path != WebsocketPath {
		t.Fatalf("unpaired announcement = %+v", remote.Announcement)
	}
	records := remote.Announcement.TXT.Records()
	if len(records) != 8 || records[0] != "txtvers=1" || records[3] != "ski="+skiC || records[7] != "register=true" {
		t.Fatalf("txt = %v", records)
	}
	if nodeA.Announcement().Port == nodeB.Announcement().Port || nodeA.Announcement().TXT.SKI == nodeB.Announcement().TXT.SKI {
		t.Fatal("instances share an announcement")
	}
}

func assertDiscovered(t *testing.T, node *Node, ski string, paired bool, state shipapi.ConnectionState) {
	t.Helper()
	remote, ok := node.Remote(ski)
	if !ok {
		t.Fatalf("missing %s", ski)
	}
	if remote.Paired != paired || remote.Connection != state || remote.Announcement.TXT.SKI != ski {
		t.Fatalf("remote %s = %+v", ski, remote)
	}
}

func TestConnectionStateSurvivesALaterAnnouncement(t *testing.T) {
	const ski = "dddddddddddddddddddddddddddddddddddddddd"
	node := NewNode(announcement("eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee", 4714, true))
	node.Observe(announcement(ski, 4715, false), shipapi.ConnectionStateNone, false)
	node.SetConnection(ski, shipapi.ConnectionStateReceivedPairingRequest, false)
	node.ReplaceAnnouncements([]Announcement{announcement(ski, 4715, false)})
	remote, ok := node.Remote(ski)
	if !ok || remote.Paired || remote.Connection != shipapi.ConnectionStateReceivedPairingRequest {
		t.Fatalf("remote = %+v ok=%t", remote, ok)
	}
}

func TestDirectoryPublishPreservesConnectionState(t *testing.T) {
	const localSKI = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	const peerSKI = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	observer := NewNode(announcement(localSKI, 4711, false))
	peer := NewNode(announcement(peerSKI, 4712, false))
	directory := NewDirectory()
	directory.Join(observer)
	directory.Join(peer)
	directory.Publish(peer)
	assertDiscovered(t, observer, peerSKI, false, shipapi.ConnectionStateNone)

	observer.SetConnection(peerSKI, shipapi.ConnectionStateCompleted, true)
	observer.SetShipID(peerSKI, "id-peer")
	peer.SetRegister(true)
	directory.Publish(peer)
	assertDiscovered(t, observer, peerSKI, true, shipapi.ConnectionStateCompleted)
	remote, _ := observer.Remote(peerSKI)
	if !remote.Trusted || remote.ShipID != "id-peer" || !remote.Announcement.TXT.Register {
		t.Fatalf("updated remote = %+v", remote)
	}
}
