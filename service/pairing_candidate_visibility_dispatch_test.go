package service

import (
	"sync"
	"testing"
	"time"

	"github.com/Project-Helianthus/helianthus-eebus-go/api"
	shipapi "github.com/Project-Helianthus/helianthus-ship-go/api"
)

type serializedCandidateVisibilityReader struct {
	legacyPairingCandidateVisibilityReader

	mux                   sync.Mutex
	active                int
	maxActive             int
	labels                []string
	firstEntered          chan struct{}
	firstRelease          chan struct{}
	firstOnce             sync.Once
	hubShutdownDone       <-chan struct{}
	emptyAfterHubShutdown bool
	reenter               bool
	shutdownOn            string
}

func (reader *serializedCandidateVisibilityReader) VisiblePairingCandidatesUpdated(
	service api.ServiceInterface,
	candidates []shipapi.PairingCandidateRef,
) {
	label := candidateVisibilityLabel(candidates)
	reader.mux.Lock()
	reader.active++
	if reader.active > reader.maxActive {
		reader.maxActive = reader.active
	}
	reader.labels = append(reader.labels, label)
	if len(candidates) == 0 && reader.hubShutdownDone != nil {
		select {
		case <-reader.hubShutdownDone:
			reader.emptyAfterHubShutdown = true
		default:
		}
	}
	reader.mux.Unlock()

	if label == "first" && reader.firstEntered != nil {
		reader.firstOnce.Do(func() { close(reader.firstEntered) })
		<-reader.firstRelease
	}
	if label == "outer" && reader.reenter {
		service.(*Service).VisiblePairingCandidatesUpdated(candidateVisibilitySnapshot("inner"))
	}
	if label == reader.shutdownOn {
		service.Shutdown()
	}

	reader.mux.Lock()
	reader.active--
	reader.mux.Unlock()
}

func (reader *serializedCandidateVisibilityReader) state() ([]string, int, bool) {
	reader.mux.Lock()
	defer reader.mux.Unlock()
	return append([]string(nil), reader.labels...), reader.maxActive, reader.emptyAfterHubShutdown
}

type candidateVisibilityShutdownHub struct {
	shipapi.HubInterface
	entered chan struct{}
	release chan struct{}
	done    chan struct{}
	once    sync.Once
}

func (hub *candidateVisibilityShutdownHub) Shutdown() {
	hub.once.Do(func() {
		close(hub.entered)
		<-hub.release
		close(hub.done)
	})
}

func TestCandidateVisibilityShutdownRacingInFlightCompletesTerminalAsynchronously(t *testing.T) {
	hub := &candidateVisibilityShutdownHub{
		entered: make(chan struct{}),
		release: make(chan struct{}),
		done:    make(chan struct{}),
	}
	reader := &serializedCandidateVisibilityReader{
		firstEntered:    make(chan struct{}),
		firstRelease:    make(chan struct{}),
		hubShutdownDone: hub.done,
	}
	service := &Service{serviceHandler: reader, connectionsHub: hub, lifecycle: lifecycleRunning}

	firstDone := make(chan struct{})
	go func() {
		service.VisiblePairingCandidatesUpdated(candidateVisibilitySnapshot("first"))
		close(firstDone)
	}()
	<-reader.firstEntered

	shutdownDone := make(chan struct{})
	go func() {
		service.Shutdown()
		close(shutdownDone)
	}()
	<-hub.entered
	service.VisiblePairingCandidatesUpdated(candidateVisibilitySnapshot("during-shutdown"))
	close(hub.release)

	select {
	case <-shutdownDone:
	case <-time.After(time.Second):
		close(reader.firstRelease)
		<-firstDone
		t.Fatal("Shutdown did not return while an in-flight dispatcher owned terminal delivery")
	}
	service.VisiblePairingCandidatesUpdated(candidateVisibilitySnapshot("after-shutdown-return"))
	close(reader.firstRelease)
	<-firstDone

	labels, maxActive, emptyAfterHub := reader.state()
	if !stringSlicesEqual(labels, []string{"first", "<empty>"}) {
		t.Fatalf("candidate callback order = %v, want [first <empty>]", labels)
	}
	if maxActive != 1 {
		t.Fatalf("max concurrent candidate callbacks = %d, want 1", maxActive)
	}
	if !emptyAfterHub {
		t.Fatal("terminal empty snapshot was emitted before hub shutdown completed")
	}
}

func TestCandidateVisibilityCallbackCanReenterShutdownWithBoundedReturn(t *testing.T) {
	hub := &candidateVisibilityShutdownHub{
		entered: make(chan struct{}),
		release: make(chan struct{}),
		done:    make(chan struct{}),
	}
	close(hub.release)
	reader := &serializedCandidateVisibilityReader{
		hubShutdownDone: hub.done,
		shutdownOn:      "shutdown",
	}
	service := &Service{serviceHandler: reader, connectionsHub: hub, lifecycle: lifecycleRunning}

	reportDone := make(chan struct{})
	go func() {
		service.VisiblePairingCandidatesUpdated(candidateVisibilitySnapshot("shutdown"))
		close(reportDone)
	}()
	select {
	case <-reportDone:
	case <-time.After(time.Second):
		t.Fatal("candidate callback re-entering Shutdown deadlocked")
	}
	service.VisiblePairingCandidatesUpdated(candidateVisibilitySnapshot("after"))

	labels, maxActive, emptyAfterHub := reader.state()
	if !stringSlicesEqual(labels, []string{"shutdown", "<empty>"}) {
		t.Fatalf("reentrant shutdown callback order = %v, want [shutdown <empty>]", labels)
	}
	if maxActive != 1 {
		t.Fatalf("reentrant shutdown callback nesting = %d, want 1", maxActive)
	}
	if !emptyAfterHub {
		t.Fatal("reentrant shutdown emitted terminal empty before hub shutdown")
	}
}

func TestCandidateVisibilityTerminalEmptyIsOnceLastAndSuppressesLaterReports(t *testing.T) {
	hub := &candidateVisibilityShutdownHub{
		entered: make(chan struct{}),
		release: make(chan struct{}),
		done:    make(chan struct{}),
	}
	close(hub.release)
	reader := &serializedCandidateVisibilityReader{hubShutdownDone: hub.done}
	service := &Service{serviceHandler: reader, connectionsHub: hub, lifecycle: lifecycleRunning}

	service.VisiblePairingCandidatesUpdated(candidateVisibilitySnapshot("before"))
	service.Shutdown()
	service.VisiblePairingCandidatesUpdated(candidateVisibilitySnapshot("after"))
	service.Shutdown()

	labels, maxActive, emptyAfterHub := reader.state()
	if !stringSlicesEqual(labels, []string{"before", "<empty>"}) {
		t.Fatalf("terminal candidate callback order = %v, want [before <empty>]", labels)
	}
	if maxActive != 1 {
		t.Fatalf("max concurrent candidate callbacks = %d, want 1", maxActive)
	}
	if !emptyAfterHub {
		t.Fatal("terminal empty snapshot was not emitted after hub shutdown")
	}
}

func TestCandidateVisibilityFloodCoalescesLatestPendingAndBoundsTerminalQueue(t *testing.T) {
	hub := &candidateVisibilityShutdownHub{
		entered: make(chan struct{}),
		release: make(chan struct{}),
		done:    make(chan struct{}),
	}
	close(hub.release)
	reader := &serializedCandidateVisibilityReader{
		firstEntered:    make(chan struct{}),
		firstRelease:    make(chan struct{}),
		hubShutdownDone: hub.done,
	}
	service := &Service{serviceHandler: reader, connectionsHub: hub, lifecycle: lifecycleRunning}

	firstDone := make(chan struct{})
	go func() {
		service.VisiblePairingCandidatesUpdated(candidateVisibilitySnapshot("first"))
		close(firstDone)
	}()
	<-reader.firstEntered
	for index := 0; index < 1024; index++ {
		service.VisiblePairingCandidatesUpdated(candidateVisibilitySnapshot("stale"))
	}
	latest := candidateVisibilitySnapshot("latest")
	service.VisiblePairingCandidatesUpdated(latest)
	latest[0].Name = "mutated-input"
	normalBefore, totalBefore, labelsBefore := pendingCandidateVisibilityState(service)

	service.Shutdown()
	service.VisiblePairingCandidatesUpdated(candidateVisibilitySnapshot("after-shutdown"))
	normalAfter, totalAfter, labelsAfter := pendingCandidateVisibilityState(service)

	close(reader.firstRelease)
	<-firstDone

	if normalBefore != 1 || totalBefore != 1 || !stringSlicesEqual(labelsBefore, []string{"latest"}) {
		t.Fatalf("pending queue before terminal = normal:%d total:%d labels:%v, want normal:1 total:1 labels:[latest]", normalBefore, totalBefore, labelsBefore)
	}
	if normalAfter != 1 || totalAfter != 2 || !stringSlicesEqual(labelsAfter, []string{"latest", "<empty>"}) {
		t.Fatalf("pending queue with terminal = normal:%d total:%d labels:%v, want normal:1 total:2 labels:[latest <empty>]", normalAfter, totalAfter, labelsAfter)
	}

	labels, maxActive, emptyAfterHub := reader.state()
	if !stringSlicesEqual(labels, []string{"first", "latest", "<empty>"}) {
		t.Fatalf("coalesced candidate callback order = %v, want [first latest <empty>]", labels)
	}
	if maxActive != 1 {
		t.Fatalf("max concurrent candidate callbacks = %d, want 1", maxActive)
	}
	if !emptyAfterHub {
		t.Fatal("coalesced terminal empty snapshot was emitted before hub shutdown")
	}
}

func TestCandidateVisibilityReentrantReportQueuesWithoutCallbackNesting(t *testing.T) {
	reader := &serializedCandidateVisibilityReader{reenter: true}
	service := &Service{serviceHandler: reader}

	service.VisiblePairingCandidatesUpdated(candidateVisibilitySnapshot("outer"))

	labels, maxActive, _ := reader.state()
	if !stringSlicesEqual(labels, []string{"outer", "inner"}) {
		t.Fatalf("reentrant candidate callback order = %v, want [outer inner]", labels)
	}
	if maxActive != 1 {
		t.Fatalf("reentrant callback nesting = %d, want 1", maxActive)
	}
}

func candidateVisibilitySnapshot(label string) []shipapi.PairingCandidateRef {
	return []shipapi.PairingCandidateRef{{CandidateRef: "candidate-" + label, Name: label, SKI: "ski-a"}}
}

func candidateVisibilityLabel(candidates []shipapi.PairingCandidateRef) string {
	if len(candidates) == 0 {
		return "<empty>"
	}
	return candidates[0].Name
}

func pendingCandidateVisibilityState(service *Service) (normal, total int, labels []string) {
	service.candidateVisibilityMux.Lock()
	defer service.candidateVisibilityMux.Unlock()
	total = len(service.candidateVisibility)
	labels = make([]string, 0, total)
	for _, event := range service.candidateVisibility {
		if event.done == nil {
			normal++
		}
		labels = append(labels, candidateVisibilityLabel(event.candidates))
	}
	return normal, total, labels
}

func stringSlicesEqual(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}
