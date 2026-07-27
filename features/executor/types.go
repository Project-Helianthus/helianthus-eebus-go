package executor

import (
	"context"
	"errors"
	"time"

	spineapi "github.com/Project-Helianthus/helianthus-spine-go/api"
	"github.com/Project-Helianthus/helianthus-spine-go/model"
)

var (
	ErrInvalidExactSource             = errors.New("invalid exact source")
	ErrInvalidExactTarget             = errors.New("invalid exact target")
	ErrExactSourceNotFound            = errors.New("exact source feature not found")
	ErrExactTargetNotFound            = errors.New("exact target feature not found")
	ErrExactTargetAmbiguous           = errors.New("exact target device is ambiguous")
	ErrExactTargetMismatch            = errors.New("exact target metadata mismatch")
	ErrExactRemoteResolverUnavailable = errors.New("exact remote peer resolver is unavailable")
	ErrExactRemoteBindingMismatch     = errors.New("exact remote peer binding mismatch")
	ErrExactSourceMismatch            = errors.New("exact source metadata mismatch")
	ErrExactOperationNotSupported     = errors.New("exact operation is not supported")
	ErrExactPartialOperation          = errors.New("partial exact operation is forbidden")
	ErrExactCommandCount              = errors.New("exact write requires one command")
	ErrExactFunctionMismatch          = errors.New("typed command function mismatch")
	ErrExactFunctionDataUnavailable   = errors.New("typed function data is unavailable")
	ErrExactRoundTripperUnavailable   = errors.New("correlated round-tripper is unavailable")
	ErrMalformedExactResponse         = errors.New("malformed exact correlated response")
)

// ExactRemoteIdentity is an immutable opaque peer identity supplied by the
// upper runtime. The executor assigns no protocol or trust semantics to it.
type ExactRemoteIdentity string

// ExactConnectionGeneration identifies one live connection incarnation.
type ExactConnectionGeneration uint64

// ExactRemoteBindingFailure identifies which resolver proof did not match.
type ExactRemoteBindingFailure string

const (
	ExactRemoteBindingProofMissing       ExactRemoteBindingFailure = "proof_missing"
	ExactRemoteBindingAddressMismatch    ExactRemoteBindingFailure = "address_mismatch"
	ExactRemoteBindingIdentityMismatch   ExactRemoteBindingFailure = "identity_mismatch"
	ExactRemoteBindingGenerationMismatch ExactRemoteBindingFailure = "generation_mismatch"
)

// ExactRemoteBindingError is a structured zero-send admission failure.
type ExactRemoteBindingError struct {
	Failure ExactRemoteBindingFailure
}

func (e *ExactRemoteBindingError) Error() string {
	if e == nil || e.Failure == "" {
		return ErrExactRemoteBindingMismatch.Error()
	}
	return ErrExactRemoteBindingMismatch.Error() + ": " + string(e.Failure)
}

func (e *ExactRemoteBindingError) Unwrap() error {
	return ErrExactRemoteBindingMismatch
}

// ExactRemoteBinding identifies the peer generation expected at dispatch.
type ExactRemoteBinding struct {
	DeviceAddress        model.AddressDeviceType
	RemoteIdentity       ExactRemoteIdentity
	ConnectionGeneration ExactConnectionGeneration
}

// ExactRemoteRuntime owns topology resolution and generation-bound dispatch.
// RoundTripIfCurrent must atomically verify every expected binding field and
// the owned transport capability before sending. A changed binding must return
// ExactRemoteBindingError without invoking an underlying round-tripper.
type ExactRemoteRuntime interface {
	ResolveExactRemoteDevice(model.AddressDeviceType) (spineapi.DeviceRemoteInterface, error)
	RoundTripIfCurrent(
		context.Context,
		ExactRemoteBinding,
		spineapi.CorrelatedRequest,
	) (spineapi.CorrelatedResponse, error)
}

// ExactFeatureOperation is the closed operation set accepted by the executor.
type ExactFeatureOperation model.CmdClassifierType

const (
	ExactFeatureOperationRead  ExactFeatureOperation = ExactFeatureOperation(model.CmdClassifierTypeRead)
	ExactFeatureOperationWrite ExactFeatureOperation = ExactFeatureOperation(model.CmdClassifierTypeWrite)
)

// ExactFeatureTarget binds the current remote feature metadata and function.
type ExactFeatureTarget struct {
	Address              model.FeatureAddressType
	FeatureType          model.FeatureTypeType
	Role                 model.RoleType
	Function             model.FunctionType
	RemoteIdentity       ExactRemoteIdentity
	ConnectionGeneration ExactConnectionGeneration
}

// ExactFeatureRequest describes one full READ or WRITE.
//
// Commands must be empty for READ and contain exactly one full typed CmdType
// for WRITE. The slice keeps malformed multi-command inputs observable so they
// can be rejected before contact.
type ExactFeatureRequest struct {
	Source    model.FeatureAddressType
	Target    ExactFeatureTarget
	Operation ExactFeatureOperation
	Commands  []model.CmdType
}

// ExactFeatureResult preserves the typed correlated SPINE boundary.
type ExactFeatureResult struct {
	Target         ExactFeatureTarget
	Operation      ExactFeatureOperation
	CorrelationKey model.MsgCounterType
	Request        model.CmdType
	Response       model.CmdType
	RemoteError    *spineapi.CorrelatedRemoteError
	ProtocolError  *spineapi.CorrelatedProtocolError
	RequestedAt    time.Time
	RespondedAt    time.Time
}
