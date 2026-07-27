package exact

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	spineapi "github.com/Project-Helianthus/helianthus-spine-go/api"
	"github.com/Project-Helianthus/helianthus-spine-go/model"
	"github.com/Project-Helianthus/helianthus-spine-go/spine"
)

const (
	testFunction      = model.FunctionTypeDeviceClassificationManufacturerData
	testOtherFunction = model.FunctionTypeDeviceClassificationUserData
)

type recordingSender struct {
	spineapi.SenderInterface

	calls atomic.Int64
	mux   sync.Mutex
	last  correlatedRequest
	fn    func(context.Context, correlatedRequest) (correlatedResponse, error)
}

func (s *recordingSender) RoundTrip(ctx context.Context, request correlatedRequest) (correlatedResponse, error) {
	s.calls.Add(1)
	s.mux.Lock()
	s.last = request
	s.mux.Unlock()
	if s.fn != nil {
		return s.fn(ctx, request)
	}
	return successfulResponse(request), nil
}

func (s *recordingSender) Stats() correlatedRoundTripStats {
	return correlatedRoundTripStats{}
}

func (s *recordingSender) Close() error {
	return nil
}

func (s *recordingSender) callCount() int64 {
	return s.calls.Load()
}

func (s *recordingSender) lastRequest() correlatedRequest {
	s.mux.Lock()
	defer s.mux.Unlock()
	return s.last
}

type exactFixture struct {
	local   *spine.DeviceLocal
	source  model.FeatureAddressType
	target  model.FeatureAddressType
	remote  *spine.DeviceRemote
	entity  *spine.EntityRemote
	feature *spine.FeatureRemote
	sender  *recordingSender
}

func TestExecutorFullReadBuildsTypedFactoryCommandAndReturnsCorrelatedReply(t *testing.T) {
	fixture := newExactFixture(t, true, true)
	reply := manufacturerCommand("opaque-device")
	fixture.sender.fn = func(_ context.Context, request correlatedRequest) (correlatedResponse, error) {
		return responseFor(request, model.CmdClassifierTypeReply, reply, 41), nil
	}

	before := time.Now()
	outcome, err := NewExecutor(fixture.local).Execute(context.Background(), Request{
		Source:    fixture.source,
		Target:    fixture.target,
		Function:  testFunction,
		Operation: OperationRead,
	})
	after := time.Now()
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}

	sent := fixture.sender.lastRequest()
	if sent.Classifier != model.CmdClassifierTypeRead {
		t.Errorf("classifier = %q, want read", sent.Classifier)
	}
	if !reflect.DeepEqual(sent.Source, fixture.source) || !reflect.DeepEqual(sent.Destination, fixture.target) {
		t.Errorf("addresses = %#v -> %#v, want %#v -> %#v", sent.Source, sent.Destination, fixture.source, fixture.target)
	}
	if sent.AckRequest {
		t.Error("full read unexpectedly requested an acknowledgement")
	}
	if sent.Cmd.Function != nil || len(sent.Cmd.Filter) != 0 {
		t.Fatalf("full read command contains function/filter options: %#v", sent.Cmd)
	}
	data, dataErr := sent.Cmd.Data()
	if dataErr != nil || data.Function == nil || *data.Function != testFunction {
		t.Fatalf("full read typed data = %#v, error = %v", data, dataErr)
	}
	if _, ok := data.Value.(*model.DeviceClassificationManufacturerDataType); !ok {
		t.Fatalf("full read typed data = %T, want manufacturer data", data.Value)
	}

	if outcome.Operation != OperationRead || outcome.Function != testFunction ||
		!reflect.DeepEqual(outcome.Target, fixture.target) {
		t.Errorf("outcome identity = %#v", outcome)
	}
	if outcome.CorrelationKey != 41 || outcome.CorrelatedResponse.CorrelationKey != 41 {
		t.Errorf("correlation keys = %d/%d, want 41", outcome.CorrelationKey, outcome.CorrelatedResponse.CorrelationKey)
	}
	if !reflect.DeepEqual(outcome.CorrelatedRequest, sent) ||
		!reflect.DeepEqual(outcome.CorrelatedResponse.Cmd, reply) {
		t.Error("outcome did not preserve the typed correlated request/response")
	}
	if outcome.RequestTimestamp.Before(before) || outcome.ResponseTimestamp.Before(outcome.RequestTimestamp) ||
		outcome.ResponseTimestamp.After(after) {
		t.Errorf("timestamps out of order: before=%v request=%v response=%v after=%v",
			before, outcome.RequestTimestamp, outcome.ResponseTimestamp, after)
	}
}

func TestExecutorFullWriteSendsExactlyOneMatchingTypedCommand(t *testing.T) {
	fixture := newExactFixture(t, true, true)
	write := manufacturerCommand("write-device")

	outcome, err := NewExecutor(fixture.local).Execute(context.Background(), Request{
		Source:    fixture.source,
		Target:    fixture.target,
		Function:  testFunction,
		Operation: OperationWrite,
		Commands:  []model.CmdType{write},
	})
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}

	sent := fixture.sender.lastRequest()
	if fixture.sender.callCount() != 1 || sent.Classifier != model.CmdClassifierTypeWrite {
		t.Fatalf("RoundTrip calls/classifier = %d/%q, want 1/write", fixture.sender.callCount(), sent.Classifier)
	}
	if !reflect.DeepEqual(sent.Cmd, write) || !reflect.DeepEqual(outcome.CorrelatedRequest.Cmd, write) {
		t.Error("full write did not preserve the caller's typed command")
	}
	if outcome.Operation != OperationWrite || outcome.CorrelatedResponse.Cmd.ResultData == nil {
		t.Errorf("write outcome = %#v", outcome)
	}
}

func TestExecutorSelectsOnlyExactDeviceEntityFeatureAndFunction(t *testing.T) {
	fixture := newExactFixture(t, true, true)
	decoySender := &recordingSender{}
	addRemoteFeature(
		fixture.local,
		"decoy-ski",
		"decoy-device",
		[]model.AddressEntityType{2},
		3,
		model.RoleTypeServer,
		model.FeatureTypeTypeDeviceClassification,
		testFunction,
		true,
		true,
		decoySender,
	)
	decoyFeature := spine.NewFeatureRemote(4, fixture.entity, model.FeatureTypeTypeDeviceClassification, model.RoleTypeServer)
	decoyFeature.SetOperations(functionProperties(testOtherFunction, true, true))
	fixture.entity.AddFeature(decoyFeature)

	if _, err := NewExecutor(fixture.local).Execute(context.Background(), Request{
		Source: fixture.source, Target: fixture.target, Function: testFunction, Operation: OperationRead,
	}); err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if fixture.sender.callCount() != 1 || decoySender.callCount() != 0 {
		t.Errorf("target/decoy calls = %d/%d, want 1/0", fixture.sender.callCount(), decoySender.callCount())
	}
}

func TestExecutorRequiresDeclaredFullOperation(t *testing.T) {
	tests := []struct {
		name      string
		read      bool
		write     bool
		operation Operation
		commands  []model.CmdType
	}{
		{name: "read not declared", write: true, operation: OperationRead},
		{name: "write not declared", read: true, operation: OperationWrite, commands: []model.CmdType{manufacturerCommand("write")}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newExactFixture(t, test.read, test.write)
			_, err := NewExecutor(fixture.local).Execute(context.Background(), Request{
				Source: fixture.source, Target: fixture.target, Function: testFunction,
				Operation: test.operation, Commands: test.commands,
			})
			if !errors.Is(err, ErrOperationNotSupported) {
				t.Fatalf("Execute() error = %v, want ErrOperationNotSupported", err)
			}
			if fixture.sender.callCount() != 0 {
				t.Fatalf("RoundTrip calls = %d, want zero", fixture.sender.callCount())
			}
		})
	}
}

func TestExecutorRejectsForbiddenPayloadsBeforeRoundTrip(t *testing.T) {
	partialRead := spine.NewFunctionDataCmd[model.DeviceClassificationManufacturerDataType](testFunction).
		ReadCmdType(nil, &model.DeviceClassificationManufacturerDataElementsType{})
	partialWrite := spine.NewFunctionDataCmd[model.DeviceClassificationManufacturerDataType](testFunction).
		NotifyOrWriteCmdType(nil, nil, true, nil)
	deleteWrite := manufacturerCommand("delete")
	deleteWrite.Filter = []model.FilterType{{
		CmdControl: &model.CmdControlType{Delete: &model.ElementTagType{}},
	}}
	selectorWrite := manufacturerCommand("selector")
	selectorWrite.Filter = []model.FilterType{{
		DeviceConfigurationKeyValueListDataSelectors: &model.DeviceConfigurationKeyValueListDataSelectorsType{},
	}}
	wrongFunction := userCommand("wrong")
	multiData := manufacturerCommand("multi")
	user := model.DeviceClassificationUserDataType{}
	multiData.DeviceClassificationUserData = &user
	functionOnly := model.CmdType{Function: pointer(testFunction)}

	tests := []struct {
		name      string
		operation Operation
		commands  []model.CmdType
	}{
		{name: "partial read", operation: OperationRead, commands: []model.CmdType{partialRead}},
		{name: "read payload", operation: OperationRead, commands: []model.CmdType{model.CmdType{Function: pointer(testFunction)}}},
		{name: "partial write", operation: OperationWrite, commands: []model.CmdType{partialWrite}},
		{name: "filter delete", operation: OperationWrite, commands: []model.CmdType{deleteWrite}},
		{name: "selector", operation: OperationWrite, commands: []model.CmdType{selectorWrite}},
		{name: "write without command", operation: OperationWrite},
		{name: "multi command", operation: OperationWrite, commands: []model.CmdType{manufacturerCommand("one"), manufacturerCommand("two")}},
		{name: "wrong typed function", operation: OperationWrite, commands: []model.CmdType{wrongFunction}},
		{name: "multiple typed fields", operation: OperationWrite, commands: []model.CmdType{multiData}},
		{name: "function without typed data", operation: OperationWrite, commands: []model.CmdType{functionOnly}},
		{name: "invoke call", operation: Operation(model.CmdClassifierTypeCall), commands: []model.CmdType{functionOnly}},
		{name: "notify", operation: Operation(model.CmdClassifierTypeNotify), commands: []model.CmdType{manufacturerCommand("notify")}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newExactFixture(t, true, true)
			_, err := NewExecutor(fixture.local).Execute(context.Background(), Request{
				Source: fixture.source, Target: fixture.target, Function: testFunction,
				Operation: test.operation, Commands: test.commands,
			})
			if !errors.Is(err, ErrInvalidRequest) {
				t.Fatalf("Execute() error = %v, want ErrInvalidRequest", err)
			}
			if fixture.sender.callCount() != 0 {
				t.Fatalf("RoundTrip calls = %d, want zero", fixture.sender.callCount())
			}
		})
	}
}

func TestExecutorRejectsMissingAmbiguousMismatchedTargetsBeforeRoundTrip(t *testing.T) {
	tests := []struct {
		name    string
		prepare func(*exactFixture)
		want    error
	}{
		{
			name: "missing source feature",
			prepare: func(f *exactFixture) {
				f.source.Feature = pointer(model.AddressFeatureType(99))
			},
			want: ErrTargetNotFound,
		},
		{
			name: "wrong source device address",
			prepare: func(f *exactFixture) {
				f.source.Device = pointer(model.AddressDeviceType("other-local"))
			},
			want: ErrTargetMismatch,
		},
		{
			name: "wrong source role",
			prepare: func(f *exactFixture) {
				entity := spine.NewEntityLocal(f.local, model.EntityTypeTypeEVSE, []model.AddressEntityType{9}, 0)
				f.local.AddEntity(entity)
				feature := spine.NewFeatureLocal(9, entity, model.FeatureTypeTypeDeviceClassification, model.RoleTypeServer)
				entity.AddFeature(feature)
				f.source = *feature.Address()
			},
			want: ErrTargetMismatch,
		},
		{
			name: "missing device",
			prepare: func(f *exactFixture) {
				f.target.Device = pointer(model.AddressDeviceType("absent"))
			},
			want: ErrTargetNotFound,
		},
		{
			name: "missing entity",
			prepare: func(f *exactFixture) {
				f.target.Entity = []model.AddressEntityType{99}
			},
			want: ErrTargetNotFound,
		},
		{
			name: "missing feature",
			prepare: func(f *exactFixture) {
				f.target.Feature = pointer(model.AddressFeatureType(99))
			},
			want: ErrTargetNotFound,
		},
		{
			name: "missing function",
			prepare: func(f *exactFixture) {
				f.feature.SetOperations(functionProperties(testOtherFunction, true, true))
			},
			want: ErrTargetNotFound,
		},
		{
			name: "wrong target role",
			prepare: func(f *exactFixture) {
				f.entity.RemoveAllFeatures()
				feature := spine.NewFeatureRemote(3, f.entity, model.FeatureTypeTypeDeviceClassification, model.RoleTypeClient)
				feature.SetOperations(functionProperties(testFunction, true, true))
				f.entity.AddFeature(feature)
			},
			want: ErrTargetMismatch,
		},
		{
			name: "mismatched feature and function",
			prepare: func(f *exactFixture) {
				f.entity.RemoveAllFeatures()
				feature := spine.NewFeatureRemote(3, f.entity, model.FeatureTypeTypeMeasurement, model.RoleTypeServer)
				feature.SetOperations(functionProperties(testFunction, true, true))
				f.entity.AddFeature(feature)
			},
			want: ErrTargetMismatch,
		},
		{
			name: "ambiguous device",
			prepare: func(f *exactFixture) {
				addRemoteFeature(
					f.local, "duplicate-ski", string(*f.target.Device), f.target.Entity, uint(*f.target.Feature),
					model.RoleTypeServer, model.FeatureTypeTypeDeviceClassification, testFunction, true, true,
					&recordingSender{},
				)
			},
			want: ErrTargetAmbiguous,
		},
		{
			name: "ambiguous feature",
			prepare: func(f *exactFixture) {
				feature := spine.NewFeatureRemote(3, f.entity, model.FeatureTypeTypeDeviceClassification, model.RoleTypeServer)
				feature.SetOperations(functionProperties(testFunction, true, true))
				f.entity.AddFeature(feature)
			},
			want: ErrTargetAmbiguous,
		},
		{
			name: "malformed address",
			prepare: func(f *exactFixture) {
				f.target.Feature = nil
			},
			want: ErrInvalidRequest,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newExactFixture(t, true, true)
			test.prepare(fixture)
			_, err := NewExecutor(fixture.local).Execute(context.Background(), Request{
				Source: fixture.source, Target: fixture.target, Function: testFunction, Operation: OperationRead,
			})
			if !errors.Is(err, test.want) {
				t.Fatalf("Execute() error = %v, want %v", err, test.want)
			}
			if fixture.sender.callCount() != 0 {
				t.Fatalf("RoundTrip calls = %d, want zero", fixture.sender.callCount())
			}
		})
	}
}

func TestExecutorRejectsMissingRoundTripperBeforeSend(t *testing.T) {
	fixture := newExactFixture(t, true, true)
	fixture.local.RemoveRemoteDevice("remote-ski")
	remote, _, feature := addRemoteFeature(
		fixture.local, "legacy-ski", "remote-device", []model.AddressEntityType{2}, 3,
		model.RoleTypeServer, model.FeatureTypeTypeDeviceClassification, testFunction, true, true,
		&legacySender{},
	)
	fixture.remote, fixture.feature, fixture.target = remote, feature, *feature.Address()

	_, err := NewExecutor(fixture.local).Execute(context.Background(), Request{
		Source: fixture.source, Target: fixture.target, Function: testFunction, Operation: OperationRead,
	})
	if !errors.Is(err, ErrRoundTripperUnavailable) {
		t.Fatalf("Execute() error = %v, want ErrRoundTripperUnavailable", err)
	}
}

func TestExecutorPropagatesTypedRemoteErrorWithCorrelationAndTimestamps(t *testing.T) {
	fixture := newExactFixture(t, true, true)
	description := model.DescriptionType("rejected")
	remoteErr := &correlatedRemoteError{
		ErrorNumber: model.ErrorNumberTypeGeneralError,
		Description: &description,
	}
	fixture.sender.fn = func(_ context.Context, request correlatedRequest) (correlatedResponse, error) {
		cmd := resultCommand(model.ErrorNumberTypeGeneralError)
		return responseFor(request, model.CmdClassifierTypeResult, cmd, 52), remoteErr
	}

	outcome, err := NewExecutor(fixture.local).Execute(context.Background(), Request{
		Source: fixture.source, Target: fixture.target, Function: testFunction, Operation: OperationWrite,
		Commands: []model.CmdType{manufacturerCommand("rejected")},
	})
	var got *correlatedRemoteError
	if !errors.As(err, &got) || got != remoteErr {
		t.Fatalf("Execute() error = %T %v, want exact correlated remote error", err, err)
	}
	if outcome.CorrelationKey != 52 || outcome.RequestTimestamp.IsZero() || outcome.ResponseTimestamp.IsZero() ||
		outcome.ResponseTimestamp.Before(outcome.RequestTimestamp) {
		t.Errorf("remote-error outcome = %#v", outcome)
	}
}

func TestExecutorPropagatesCancellationTimeoutDisconnectAndProtocolErrors(t *testing.T) {
	t.Run("cancelled before dispatch", func(t *testing.T) {
		fixture := newExactFixture(t, true, true)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		_, err := NewExecutor(fixture.local).Execute(ctx, Request{
			Source: fixture.source, Target: fixture.target, Function: testFunction, Operation: OperationRead,
		})
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Execute() error = %v, want context canceled", err)
		}
		if fixture.sender.callCount() != 0 {
			t.Fatalf("RoundTrip calls = %d, want zero", fixture.sender.callCount())
		}
	})

	t.Run("timeout", func(t *testing.T) {
		fixture := newExactFixture(t, true, true)
		fixture.sender.fn = func(ctx context.Context, _ correlatedRequest) (correlatedResponse, error) {
			<-ctx.Done()
			return correlatedResponse{}, ctx.Err()
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
		defer cancel()
		_, err := NewExecutor(fixture.local).Execute(ctx, Request{
			Source: fixture.source, Target: fixture.target, Function: testFunction, Operation: OperationRead,
		})
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("Execute() error = %v, want deadline exceeded", err)
		}
		if fixture.sender.callCount() != 1 {
			t.Fatalf("RoundTrip calls = %d, want one", fixture.sender.callCount())
		}
	})

	t.Run("disconnect", func(t *testing.T) {
		fixture := newExactFixture(t, true, true)
		fixture.sender.fn = func(context.Context, correlatedRequest) (correlatedResponse, error) {
			return correlatedResponse{}, errCorrelatedRoundTripClosed
		}
		_, err := NewExecutor(fixture.local).Execute(context.Background(), Request{
			Source: fixture.source, Target: fixture.target, Function: testFunction, Operation: OperationRead,
		})
		if !errors.Is(err, errCorrelatedRoundTripClosed) {
			t.Fatalf("Execute() error = %v, want disconnect error", err)
		}
	})

	t.Run("malformed correlated response", func(t *testing.T) {
		fixture := newExactFixture(t, true, true)
		protocolErr := &correlatedProtocolError{Message: "malformed response"}
		fixture.sender.fn = func(context.Context, correlatedRequest) (correlatedResponse, error) {
			return correlatedResponse{}, protocolErr
		}
		_, err := NewExecutor(fixture.local).Execute(context.Background(), Request{
			Source: fixture.source, Target: fixture.target, Function: testFunction, Operation: OperationRead,
		})
		var got *correlatedProtocolError
		if !errors.As(err, &got) || got != protocolErr {
			t.Fatalf("Execute() error = %T %v, want exact protocol error", err, err)
		}
	})
}

func TestExecutorRejectsEmptySuccess(t *testing.T) {
	fixture := newExactFixture(t, true, true)
	fixture.sender.fn = func(context.Context, correlatedRequest) (correlatedResponse, error) {
		return correlatedResponse{}, nil
	}
	outcome, err := NewExecutor(fixture.local).Execute(context.Background(), Request{
		Source: fixture.source, Target: fixture.target, Function: testFunction, Operation: OperationRead,
	})
	var protocolErr *correlatedProtocolError
	if !errors.As(err, &protocolErr) {
		t.Fatalf("Execute() error = %T %v, want correlated protocol error", err, err)
	}
	if outcome.RequestTimestamp.IsZero() || outcome.ResponseTimestamp.IsZero() {
		t.Errorf("empty-success outcome lacks timestamps: %#v", outcome)
	}
}

type legacySender struct {
	spineapi.SenderInterface
}

func newExactFixture(t *testing.T, read, write bool) *exactFixture {
	t.Helper()
	local := spine.NewDeviceLocal(
		"brand", "model", "serial", "code", "local-device",
		model.DeviceTypeTypeEnergyManagementSystem, model.NetworkManagementFeatureSetTypeSmart,
	)
	localEntity := spine.NewEntityLocal(local, model.EntityTypeTypeCEM, []model.AddressEntityType{1}, 0)
	local.AddEntity(localEntity)
	localFeature := spine.NewFeatureLocal(1, localEntity, model.FeatureTypeTypeDeviceClassification, model.RoleTypeClient)
	localEntity.AddFeature(localFeature)

	sender := &recordingSender{}
	remote, entity, feature := addRemoteFeature(
		local, "remote-ski", "remote-device", []model.AddressEntityType{2}, 3,
		model.RoleTypeServer, model.FeatureTypeTypeDeviceClassification, testFunction, read, write, sender,
	)
	return &exactFixture{
		local: local, source: *localFeature.Address(), target: *feature.Address(),
		remote: remote, entity: entity, feature: feature, sender: sender,
	}
}

func addRemoteFeature(
	local *spine.DeviceLocal,
	ski string,
	deviceAddress string,
	entityAddress []model.AddressEntityType,
	featureAddress uint,
	role model.RoleType,
	featureType model.FeatureTypeType,
	function model.FunctionType,
	read bool,
	write bool,
	sender spineapi.SenderInterface,
) (*spine.DeviceRemote, *spine.EntityRemote, *spine.FeatureRemote) {
	remote := spine.NewDeviceRemote(local, ski, sender)
	address := model.AddressDeviceType(deviceAddress)
	remote.UpdateDevice(&model.NetworkManagementDeviceDescriptionDataType{
		DeviceAddress: &model.DeviceAddressType{Device: &address},
	})
	local.AddRemoteDeviceForSki(ski, remote)
	entity := spine.NewEntityRemote(remote, model.EntityTypeTypeEVSE, entityAddress)
	remote.AddEntity(entity)
	feature := spine.NewFeatureRemote(featureAddress, entity, featureType, role)
	feature.SetOperations(functionProperties(function, read, write))
	entity.AddFeature(feature)
	return remote, entity, feature
}

func functionProperties(function model.FunctionType, read, write bool) []model.FunctionPropertyType {
	operations := &model.PossibleOperationsType{}
	if read {
		operations.Read = &model.PossibleOperationsReadType{}
	}
	if write {
		operations.Write = &model.PossibleOperationsWriteType{}
	}
	return []model.FunctionPropertyType{{
		Function: &function, PossibleOperations: operations,
	}}
}

func manufacturerCommand(name string) model.CmdType {
	value := model.DeviceClassificationStringType(name)
	data := model.DeviceClassificationManufacturerDataType{DeviceName: &value}
	cmd := model.CmdType{}
	cmd.SetDataForFunction(testFunction, &data)
	return cmd
}

func userCommand(name string) model.CmdType {
	value := model.LabelType(name)
	data := model.DeviceClassificationUserDataType{UserLabel: &value}
	cmd := model.CmdType{}
	cmd.SetDataForFunction(testOtherFunction, &data)
	return cmd
}

func resultCommand(number model.ErrorNumberType) model.CmdType {
	return model.CmdType{ResultData: &model.ResultDataType{ErrorNumber: &number}}
}

func successfulResponse(request correlatedRequest) correlatedResponse {
	classifier := model.CmdClassifierTypeReply
	cmd := manufacturerCommand("reply")
	if request.Classifier == model.CmdClassifierTypeWrite {
		classifier = model.CmdClassifierTypeResult
		cmd = resultCommand(model.ErrorNumberTypeNoError)
	}
	return responseFor(request, classifier, cmd, 1)
}

func responseFor(
	request correlatedRequest,
	classifier model.CmdClassifierType,
	cmd model.CmdType,
	key model.MsgCounterType,
) correlatedResponse {
	return correlatedResponse{
		CorrelationKey: key,
		Header: model.HeaderType{
			AddressSource:       &request.Destination,
			AddressDestination:  &request.Source,
			MsgCounterReference: &key,
			CmdClassifier:       &classifier,
		},
		Cmd: cmd,
	}
}

func pointer[T any](value T) *T {
	return &value
}
