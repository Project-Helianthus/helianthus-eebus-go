package session

import (
	"bytes"
	"log/slog"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/Project-Helianthus/helianthus-eebus-go/discovery"
	spineapi "github.com/Project-Helianthus/helianthus-spine-go/api"
	"github.com/Project-Helianthus/helianthus-spine-go/model"
)

type subscribeHandler struct {
	spineapi.FeatureLocalInterface
	calls int
}

func (h *subscribeHandler) HandleMessage(*spineapi.Message) *model.ErrorType {
	h.calls++
	return nil
}

type subscriptionRemote struct{ spineapi.DeviceRemoteInterface }

func (subscriptionRemote) Ski() string { return skiRemoteA }

func subscriptionRequestFor(pair Pair) *model.SubscriptionManagementRequestCallType {
	return &model.SubscriptionManagementRequestCallType{
		ClientAddress: &pair.Client, ServerAddress: &pair.Server, ServerFeatureType: &pair.ServerFeatureType,
	}
}

func TestRetainedReadRejectsSubscribeBeforePinnedHandlerWhenNotReady(t *testing.T) {
	instance := &Instance{
		root:      t.TempDir(),
		book:      newBook(),
		trusted:   map[string]bool{skiRemoteA: true},
		connected: map[string]bool{skiRemoteA: true},
	}
	pinned := &subscribeHandler{}
	feature := &retainedRead{FeatureLocalInterface: pinned, instance: instance}
	pair := localServerPair(1, 21, 1, 2)
	message := &spineapi.Message{
		DeviceRemote:  subscriptionRemote{},
		CmdClassifier: model.CmdClassifierTypeCall,
		Cmd: model.CmdType{NodeManagementSubscriptionRequestCall: &model.NodeManagementSubscriptionRequestCallType{
			SubscriptionRequest: subscriptionRequestFor(pair),
		}},
	}
	for _, state := range []struct {
		name                          string
		trusted, connected, suspended bool
	}{
		{name: "suspended", trusted: true, connected: true, suspended: true},
		{name: "disconnected", trusted: true},
		{name: "untrusted", connected: true},
	} {
		t.Run(state.name, func(t *testing.T) {
			instance.trusted[skiRemoteA] = state.trusted
			instance.connected[skiRemoteA] = state.connected
			instance.suspended = state.suspended
			result := feature.HandleMessage(message)
			if result == nil || result.ErrorNumber != model.ErrorNumberTypeGeneralError || pinned.calls != 0 {
				t.Fatalf("subscribe result=%+v pinned calls=%d", result, pinned.calls)
			}
		})
	}

	instance.trusted[skiRemoteA] = true
	instance.connected[skiRemoteA] = true
	instance.suspended = false
	if result := feature.HandleMessage(message); result != nil || pinned.calls != 1 {
		t.Fatalf("ready subscribe result=%+v pinned calls=%d", result, pinned.calls)
	}
	stored, err := loadState(instance.root)
	if err != nil || len(stored) != 1 || stored[0].Role != RoleLocalServer || !pairHeld([]Pair{stored[0].pair()}, pair) {
		t.Fatalf("retained subscription=%+v err=%v", stored, err)
	}
}

type logBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *logBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *logBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func TestSessionCallbacksReportErrors(t *testing.T) {
	var logs logBuffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, nil)))
	t.Cleanup(func() { slog.SetDefault(previous) })

	instance := &Instance{
		root:      filepath.Join(t.TempDir(), "missing"),
		book:      newBook(),
		view:      discovery.NewNode(discovery.Announcement{}),
		trusted:   map[string]bool{skiRemoteA: true},
		connected: map[string]bool{skiRemoteA: true},
	}
	pair := localServerPair(1, 21, 1, 2)
	instance.suspended = true
	instance.noteSubscriptionEvent(spineapi.EventPayload{
		Ski: skiRemoteA, ChangeType: spineapi.ElementChangeAdd, Data: subscriptionRequestFor(pair),
	})
	instance.onSpineResult(spineapi.ResponseMessage{DeviceRemote: subscriptionRemote{}})
	instance.RemoteSKIConnected(nil, skiRemoteA)
	instance.RemoteSKIDisconnected(nil, skiRemoteA)

	instance.acceptServerLocked(skiRemoteA, pair)
	feature := &retainedRead{FeatureLocalInterface: &subscribeHandler{}, instance: instance}
	if result := feature.HandleMessage(&spineapi.Message{
		DeviceRemote: subscriptionRemote{}, CmdClassifier: model.CmdClassifierTypeCall,
		Cmd: model.CmdType{NodeManagementSubscriptionDeleteCall: &model.NodeManagementSubscriptionDeleteCallType{
			SubscriptionDelete: &model.SubscriptionManagementDeleteCallType{ClientAddress: &pair.Client, ServerAddress: &pair.Server},
		}},
	}); result != nil || len(instance.Established()) != 0 {
		t.Fatalf("delete result=%+v records=%+v", result, instance.Established())
	}

	instance.heldCounter = map[string]uint64{skiRemoteA: 1}
	instance.peerHeld = make(map[string][]Pair)
	instance.peerHeldReady = make(map[string]bool)
	instance.deliverPeerHeld(skiRemoteA, 1, &model.NodeManagementSubscriptionDataType{})
	for _, want := range []string{
		"record SPINE subscription event", errNoSPINE.Error(),
		"record SPINE subscription result", "record SHIP connect", "record SHIP disconnect", stateFileName,
		"persist SPINE subscription delete", "reconcile SPINE subscription data",
	} {
		if !strings.Contains(logs.String(), want) {
			t.Errorf("missing %q in diagnostics: %s", want, logs.String())
		}
	}
}
