package session

import (
	"log/slog"

	shiputil "github.com/Project-Helianthus/helianthus-ship-go/util"
	spineapi "github.com/Project-Helianthus/helianthus-spine-go/api"
	"github.com/Project-Helianthus/helianthus-spine-go/model"
	"github.com/Project-Helianthus/helianthus-spine-go/spine"
)

// retainedRead is the node-management feature ProcessCmd looks up.
// The pinned feature answers a subscription-data call from memory and
// only for a remote client, so this feature answers, records, and
// applies the delete from the retained pairs.
type retainedRead struct {
	spineapi.FeatureLocalInterface
	instance *Instance
}

func (s *Instance) installRetainedRead() error {
	local := s.service.LocalDevice()
	if local == nil {
		return errNodeManagement
	}
	for _, entity := range local.Entities() {
		features := entity.Features()
		for i, feature := range features {
			if feature == nil || feature.Type() != model.FeatureTypeTypeNodeManagement {
				continue
			}
			features[i] = &retainedRead{FeatureLocalInterface: feature, instance: s}
			return nil
		}
	}
	return errNodeManagement
}

func (f *retainedRead) HandleMessage(message *spineapi.Message) *model.ErrorType {
	if message == nil || f.instance == nil {
		return f.FeatureLocalInterface.HandleMessage(message)
	}
	switch {
	case message.Cmd.NodeManagementSubscriptionData != nil && message.CmdClassifier == model.CmdClassifierTypeCall:
		return f.answer(message)
	case message.Cmd.NodeManagementSubscriptionData != nil && message.CmdClassifier == model.CmdClassifierTypeReply:
		// The pinned handler rejects a reply. Recording it here is what lets
		// reconciliation see the pairs the peer still holds.
		f.noteReply(message)
		return nil
	case message.Cmd.NodeManagementSubscriptionDeleteCall != nil && message.CmdClassifier == model.CmdClassifierTypeCall:
		return f.remove(message)
	case message.Cmd.NodeManagementSubscriptionRequestCall != nil && message.CmdClassifier == model.CmdClassifierTypeCall:
		return f.request(message)
	case message.Cmd.NodeManagementDetailedDiscoveryData != nil && message.CmdClassifier == model.CmdClassifierTypeReply:
		return f.detailedDiscovery(message)
	default:
		return f.FeatureLocalInterface.HandleMessage(message)
	}
}

func (f *retainedRead) detailedDiscovery(message *spineapi.Message) *model.ErrorType {
	s := f.instance
	var persistErr error
	s.mu.Lock()
	defer func() {
		s.mu.Unlock()
		reportAsyncError("reconcile SPINE detailed discovery", persistErr)
	}()
	// SPINE sets the peer's address here, after SHIP's connected callback.
	// Serialize that update with reads of the address in Connect and Run.
	if pinned := f.FeatureLocalInterface.HandleMessage(message); pinned != nil {
		return pinned
	}
	peer := shiputil.NormalizeSKI(message.DeviceRemote.Ski())
	if !s.closed && s.canExchangeLocked(peer) && s.service.LocalDevice().RemoteDeviceForSki(peer) == message.DeviceRemote {
		s.reconcileLocked(peer)
		persistErr = s.saveLocked()
	}
	return nil
}

func (f *retainedRead) answer(message *spineapi.Message) *model.ErrorType {
	if message.DeviceRemote == nil || message.FeatureRemote == nil || message.FeatureRemote.Device() == nil || message.RequestHeader == nil || message.RequestHeader.AddressDestination == nil {
		return model.NewErrorType(model.ErrorNumberTypeGeneralError, errNotPrimary.Error())
	}
	entries, err := f.instance.SubscriptionData(message.DeviceRemote.Ski(), *message.RequestHeader.AddressDestination)
	if err != nil {
		return model.NewErrorType(model.ErrorNumberTypeGeneralError, err.Error())
	}
	sender := message.FeatureRemote.Device().Sender()
	if sender == nil {
		return model.NewErrorType(model.ErrorNumberTypeGeneralError, errClosed.Error())
	}
	cmd := model.CmdType{
		NodeManagementSubscriptionData: &model.NodeManagementSubscriptionDataType{SubscriptionEntry: entries},
	}
	if err := sender.Reply(message.RequestHeader, f.Address(), cmd); err != nil {
		return model.NewErrorType(model.ErrorNumberTypeGeneralError, err.Error())
	}
	return nil
}

func (f *retainedRead) noteReply(message *spineapi.Message) {
	if message.DeviceRemote == nil || message.RequestHeader == nil || message.RequestHeader.MsgCounterReference == nil {
		return
	}
	f.instance.deliverPeerHeld(message.DeviceRemote.Ski(), uint64(*message.RequestHeader.MsgCounterReference), message.Cmd.NodeManagementSubscriptionData)
}

func (f *retainedRead) remove(message *spineapi.Message) *model.ErrorType {
	pinned := f.FeatureLocalInterface.HandleMessage(message)
	call := message.Cmd.NodeManagementSubscriptionDeleteCall
	if message.DeviceRemote == nil || call == nil || call.SubscriptionDelete == nil {
		return pinned
	}
	del := Delete{}
	if call.SubscriptionDelete.ClientAddress != nil {
		del.Client = *call.SubscriptionDelete.ClientAddress
	}
	if call.SubscriptionDelete.ServerAddress != nil {
		del.Server = *call.SubscriptionDelete.ServerAddress
	}
	removed, err := f.instance.removeRemoteDelete(message.DeviceRemote.Ski(), del)
	// Keep the live delete even when persisting it fails, as for a request.
	reportAsyncError("persist SPINE subscription delete", err)
	if removed {
		return nil
	}
	return pinned
}

func (f *retainedRead) request(message *spineapi.Message) *model.ErrorType {
	if message.DeviceRemote == nil {
		return f.FeatureLocalInterface.HandleMessage(message)
	}
	s := f.instance
	peer := shiputil.NormalizeSKI(message.DeviceRemote.Ski())
	var persistErr error
	s.mu.Lock()
	defer func() {
		s.mu.Unlock()
		reportAsyncError("persist SPINE subscription request", persistErr)
	}()
	// Use the same readiness check as AcceptRemoteSubscribe before the pinned
	// manager adds a live subscription. Keep acceptance and recording together.
	if err := s.checkRemoteSubscribeLocked(peer); err != nil {
		return model.NewErrorType(model.ErrorNumberTypeGeneralError, err.Error())
	}
	if pinned := f.FeatureLocalInterface.HandleMessage(message); pinned != nil {
		return pinned
	}
	call := message.Cmd.NodeManagementSubscriptionRequestCall
	if call == nil || call.SubscriptionRequest == nil {
		return nil
	}
	req := call.SubscriptionRequest
	if req.ClientAddress == nil || req.ServerAddress == nil {
		return nil
	}
	pair := Pair{Client: cloneAddress(*req.ClientAddress), Server: cloneAddress(*req.ServerAddress)}
	if req.ServerFeatureType != nil {
		pair.ServerFeatureType = *req.ServerFeatureType
	}
	s.acceptServerLocked(peer, pair)
	// A storage failure does not undo the manager's live acceptance. Keep
	// the pair in memory and report the failed persistence separately.
	persistErr = s.saveLocked()
	return nil
}

func (s *Instance) removeRemoteDelete(ski string, del Delete) (bool, error) {
	ski = shiputil.NormalizeSKI(ski)
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || !s.trusted[ski] {
		return false, nil
	}
	if s.applyDeleteLocked(ski, del) == 0 {
		return false, nil
	}
	return true, s.saveLocked()
}

func reportAsyncError(operation string, err error) {
	if err != nil {
		slog.Error(operation, "error", err)
	}
}

func (s *Instance) onSpineResult(msg spineapi.ResponseMessage) {
	if msg.DeviceRemote == nil {
		return
	}
	reportAsyncError("record SPINE subscription result", s.NoteResult(msg.DeviceRemote.Ski(), uint64(msg.MsgCounterReference), resultAccepted(msg.Data)))
}

func resultAccepted(data any) bool {
	switch result := data.(type) {
	case *model.ResultDataType:
		return result != nil && result.ErrorNumber != nil && *result.ErrorNumber == model.ErrorNumberTypeNoError
	case model.ResultDataType:
		return result.ErrorNumber != nil && *result.ErrorNumber == model.ErrorNumberTypeNoError
	default:
		return false
	}
}

func (s *Instance) noteSubscriptionEvent(payload spineapi.EventPayload) {
	if payload.Ski == "" {
		return
	}
	switch payload.ChangeType {
	case spineapi.ElementChangeAdd:
		req, ok := subscriptionRequest(payload.Data)
		if !ok || req.ClientAddress == nil || req.ServerAddress == nil {
			return
		}
		pair := Pair{Client: cloneAddress(*req.ClientAddress), Server: cloneAddress(*req.ServerAddress)}
		if req.ServerFeatureType != nil {
			pair.ServerFeatureType = *req.ServerFeatureType
		}
		reportAsyncError("record SPINE subscription event", s.AcceptRemoteSubscribe(payload.Ski, pair))
	case spineapi.ElementChangeRemove:
		del, ok := subscriptionDelete(payload.Data)
		if !ok {
			return
		}
		remove := Delete{}
		if del.ClientAddress != nil {
			remove.Client = cloneAddress(*del.ClientAddress)
		}
		if del.ServerAddress != nil {
			remove.Server = cloneAddress(*del.ServerAddress)
		}
		reportAsyncError("record SPINE subscription delete event", s.ApplyRemoteDelete(payload.Ski, remove))
	}
}

func subscriptionRequest(data any) (model.SubscriptionManagementRequestCallType, bool) {
	switch req := data.(type) {
	case model.SubscriptionManagementRequestCallType:
		return req, true
	case *model.SubscriptionManagementRequestCallType:
		if req == nil {
			return model.SubscriptionManagementRequestCallType{}, false
		}
		return *req, true
	default:
		return model.SubscriptionManagementRequestCallType{}, false
	}
}

func subscriptionDelete(data any) (model.SubscriptionManagementDeleteCallType, bool) {
	switch del := data.(type) {
	case model.SubscriptionManagementDeleteCallType:
		return del, del.ClientAddress != nil || del.ServerAddress != nil
	case *model.SubscriptionManagementDeleteCallType:
		if del == nil {
			return model.SubscriptionManagementDeleteCallType{}, false
		}
		return *del, del.ClientAddress != nil || del.ServerAddress != nil
	default:
		return model.SubscriptionManagementDeleteCallType{}, false
	}
}

func (s *Instance) peerHeldLocked(peer string) ([]Pair, bool) {
	if s.peerHeldReady[peer] {
		return s.peerHeld[peer], true
	}
	if !s.peerHeldAsked[peer] {
		if counter, ok := s.sendSubscriptionRead(peer); ok {
			s.peerHeldAsked[peer] = true
			s.heldCounter[peer] = counter
		}
	}
	return nil, false
}

func (s *Instance) forgetPeerHeldLocked(peer string) {
	delete(s.peerHeld, peer)
	delete(s.peerHeldReady, peer)
	delete(s.peerHeldAsked, peer)
	delete(s.heldCounter, peer)
}

func (s *Instance) sendSubscriptionRead(peer string) (uint64, bool) {
	if s.service == nil || s.service.LocalDevice() == nil || s.service.LocalDevice().NodeManagement() == nil {
		return 0, false
	}
	remote := s.service.LocalDevice().RemoteDeviceForSki(peer)
	if remote == nil || remote.Sender() == nil || remote.Address() == nil {
		return 0, false
	}
	counter, err := remote.Sender().Request(
		model.CmdClassifierTypeCall,
		s.service.LocalDevice().NodeManagement().Address(),
		spine.NodeManagementAddress(remote.Address()),
		true,
		[]model.CmdType{{
			NodeManagementSubscriptionData: &model.NodeManagementSubscriptionDataType{},
		}},
	)
	if err != nil || counter == nil {
		return 0, false
	}
	return uint64(*counter), true
}

func (s *Instance) deliverPeerHeld(peer string, counter uint64, data *model.NodeManagementSubscriptionDataType) {
	peer = shiputil.NormalizeSKI(peer)
	s.mu.Lock()
	expected, asking := s.heldCounter[peer]
	if !asking || expected != counter {
		s.mu.Unlock()
		return
	}
	delete(s.heldCounter, peer)
	s.peerHeld[peer] = pairsFromSubscription(data)
	s.peerHeldReady[peer] = true
	delete(s.peerHeldAsked, peer)
	s.mu.Unlock()
	reportAsyncError("reconcile SPINE subscription data", s.Reconcile(peer))
}

func pairsFromSubscription(data *model.NodeManagementSubscriptionDataType) []Pair {
	if data == nil {
		return nil
	}
	out := make([]Pair, 0, len(data.SubscriptionEntry))
	for _, entry := range data.SubscriptionEntry {
		if entry.ClientAddress == nil || entry.ServerAddress == nil {
			continue
		}
		out = append(out, Pair{
			Client: cloneAddress(*entry.ClientAddress),
			Server: cloneAddress(*entry.ServerAddress),
		})
	}
	return out
}
