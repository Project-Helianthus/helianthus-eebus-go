package executor

import (
	"context"
	"errors"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	spineapi "github.com/Project-Helianthus/helianthus-spine-go/api"
	"github.com/Project-Helianthus/helianthus-spine-go/model"
	"github.com/Project-Helianthus/helianthus-spine-go/spine"
)

type roundTripSender struct {
	spineapi.SenderInterface
	calls     atomic.Int64
	roundTrip func(context.Context, spineapi.CorrelatedRequest) (spineapi.CorrelatedResponse, error)
}

func (s *roundTripSender) RoundTrip(
	ctx context.Context,
	request spineapi.CorrelatedRequest,
) (spineapi.CorrelatedResponse, error) {
	s.calls.Add(1)
	return s.roundTrip(ctx, request)
}

func (s *roundTripSender) Stats() spineapi.CorrelatedRoundTripStats {
	return spineapi.CorrelatedRoundTripStats{}
}

func (s *roundTripSender) Close() error {
	return nil
}

type legacySender struct {
	spineapi.SenderInterface
}

type executorFixture struct {
	local         spineapi.DeviceLocalInterface
	localFeature  spineapi.FeatureLocalInterface
	remote        spineapi.DeviceRemoteInterface
	remoteFeature spineapi.FeatureRemoteInterface
	sender        *roundTripSender
	request       ExactFeatureRequest
}

func newExecutorFixture(
	t *testing.T,
	read bool,
	write bool,
	roundTrip func(context.Context, spineapi.CorrelatedRequest) (spineapi.CorrelatedResponse, error),
) *executorFixture {
	t.Helper()

	sender := &roundTripSender{roundTrip: roundTrip}
	local := spine.NewDeviceLocal(
		"brand",
		"model",
		"serial",
		"code",
		"local-device",
		model.DeviceTypeTypeEnergyManagementSystem,
		model.NetworkManagementFeatureSetTypeSmart,
	)
	localEntity := spine.NewEntityLocal(
		local,
		model.EntityTypeTypeDeviceInformation,
		[]model.AddressEntityType{1},
		time.Second,
	)
	localFeature := spine.NewFeatureLocal(
		1,
		localEntity,
		model.FeatureTypeTypeMeasurement,
		model.RoleTypeClient,
	)
	localEntity.AddFeature(localFeature)
	local.AddEntity(localEntity)

	remote := spine.NewDeviceRemote(local, "remote-ski", sender)
	remoteAddress := model.AddressDeviceType("remote-device")
	remoteType := model.DeviceTypeTypeSubmeter
	remote.UpdateDevice(&model.NetworkManagementDeviceDescriptionDataType{
		DeviceAddress: &model.DeviceAddressType{Device: &remoteAddress},
		DeviceType:    &remoteType,
	})
	remoteEntity := spine.NewEntityRemote(
		remote,
		model.EntityTypeTypeGridConnectionPointOfPremises,
		[]model.AddressEntityType{1},
	)
	remoteFeature := spine.NewFeatureRemote(
		1,
		remoteEntity,
		model.FeatureTypeTypeMeasurement,
		model.RoleTypeServer,
	)
	function := model.FunctionTypeMeasurementListData
	operations := &model.PossibleOperationsType{}
	if read {
		operations.Read = &model.PossibleOperationsReadType{}
	}
	if write {
		operations.Write = &model.PossibleOperationsWriteType{}
	}
	remoteFeature.SetOperations([]model.FunctionPropertyType{{
		Function:           &function,
		PossibleOperations: operations,
	}})
	remoteEntity.AddFeature(remoteFeature)
	remote.AddEntity(remoteEntity)
	local.AddRemoteDeviceForSki(remote.Ski(), remote)

	request := ExactFeatureRequest{
		Source: *localFeature.Address(),
		Target: ExactFeatureTarget{
			Address:     *remoteFeature.Address(),
			FeatureType: remoteFeature.Type(),
			Role:        remoteFeature.Role(),
			Function:    function,
		},
		Operation: ExactFeatureOperationRead,
	}

	return &executorFixture{
		local:         local,
		localFeature:  localFeature,
		remote:        remote,
		remoteFeature: remoteFeature,
		sender:        sender,
		request:       request,
	}
}

func ptr[T any](value T) *T {
	return &value
}

func readReply(
	request spineapi.CorrelatedRequest,
	key model.MsgCounterType,
) spineapi.CorrelatedResponse {
	classifier := model.CmdClassifierTypeReply
	return spineapi.CorrelatedResponse{
		CorrelationKey: key,
		Header: model.HeaderType{
			AddressSource:       &request.Destination,
			AddressDestination:  &request.Source,
			MsgCounterReference: &key,
			CmdClassifier:       &classifier,
		},
		Cmd: model.CmdType{
			MeasurementListData: &model.MeasurementListDataType{},
		},
	}
}

func writeResult(
	request spineapi.CorrelatedRequest,
	key model.MsgCounterType,
) spineapi.CorrelatedResponse {
	classifier := model.CmdClassifierTypeResult
	noError := model.ErrorNumberTypeNoError
	return spineapi.CorrelatedResponse{
		CorrelationKey: key,
		Header: model.HeaderType{
			AddressSource:       &request.Destination,
			AddressDestination:  &request.Source,
			MsgCounterReference: &key,
			CmdClassifier:       &classifier,
		},
		Cmd: model.CmdType{
			ResultData: &model.ResultDataType{ErrorNumber: &noError},
		},
	}
}

func TestExactFeatureExecutorFullRead(t *testing.T) {
	var captured spineapi.CorrelatedRequest
	fixture := newExecutorFixture(t, true, false, func(
		_ context.Context,
		request spineapi.CorrelatedRequest,
	) (spineapi.CorrelatedResponse, error) {
		captured = request
		return readReply(request, 41), nil
	})

	result, err := NewExactFeatureExecutor(fixture.local).Execute(
		context.Background(),
		fixture.request,
	)
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if fixture.sender.calls.Load() != 1 {
		t.Fatalf("RoundTrip() calls = %d, want 1", fixture.sender.calls.Load())
	}
	if captured.Classifier != model.CmdClassifierTypeRead {
		t.Fatalf("classifier = %q, want read", captured.Classifier)
	}
	if captured.AckRequest {
		t.Fatal("AckRequest = true, want false")
	}
	if !reflect.DeepEqual(captured.Source, fixture.request.Source) {
		t.Fatalf("source = %+v, want %+v", captured.Source, fixture.request.Source)
	}
	if !reflect.DeepEqual(captured.Destination, fixture.request.Target.Address) {
		t.Fatalf("destination = %+v, want %+v", captured.Destination, fixture.request.Target.Address)
	}
	if captured.Cmd.Function != nil {
		t.Fatalf("read command function override = %q, want nil", *captured.Cmd.Function)
	}
	if len(captured.Cmd.Filter) != 0 {
		t.Fatalf("read command filters = %+v, want none", captured.Cmd.Filter)
	}
	data, dataErr := captured.Cmd.Data()
	if dataErr != nil {
		t.Fatalf("read command Data() error = %v", dataErr)
	}
	if data.Function == nil || *data.Function != fixture.request.Target.Function {
		t.Fatalf("read command function = %v, want %q", data.Function, fixture.request.Target.Function)
	}
	if result.CorrelationKey != 41 {
		t.Fatalf("correlation key = %d, want 41", result.CorrelationKey)
	}
	if !reflect.DeepEqual(result.Target, fixture.request.Target) {
		t.Fatalf("result target = %+v, want %+v", result.Target, fixture.request.Target)
	}
	if result.Operation != ExactFeatureOperationRead {
		t.Fatalf("result operation = %q, want READ", result.Operation)
	}
	if !reflect.DeepEqual(result.Request, captured.Cmd) {
		t.Fatal("result did not preserve the typed request command")
	}
	if result.Response.MeasurementListData == nil {
		t.Fatal("result did not preserve the typed reply command")
	}
	if result.RemoteError != nil || result.ProtocolError != nil {
		t.Fatalf("typed error fields = remote:%v protocol:%v, want nil", result.RemoteError, result.ProtocolError)
	}
	if result.RequestedAt.IsZero() || result.RespondedAt.IsZero() {
		t.Fatalf("timestamps = %v / %v, want both set", result.RequestedAt, result.RespondedAt)
	}
	if result.RespondedAt.Before(result.RequestedAt) {
		t.Fatalf("response timestamp %v precedes request timestamp %v", result.RespondedAt, result.RequestedAt)
	}
}

func TestExactFeatureExecutorFullWrite(t *testing.T) {
	extension := "opaque-write-extension"
	writeCmd := model.CmdType{
		MeasurementListData:           &model.MeasurementListDataType{},
		ManufacturerSpecificExtension: &extension,
	}
	fixture := newExecutorFixture(t, false, true, func(
		_ context.Context,
		request spineapi.CorrelatedRequest,
	) (spineapi.CorrelatedResponse, error) {
		if request.Classifier != model.CmdClassifierTypeWrite {
			t.Fatalf("classifier = %q, want write", request.Classifier)
		}
		if !reflect.DeepEqual(request.Cmd, writeCmd) {
			t.Fatalf("write command = %+v, want %+v", request.Cmd, writeCmd)
		}
		return writeResult(request, 52), nil
	})
	fixture.request.Operation = ExactFeatureOperationWrite
	fixture.request.Commands = []model.CmdType{writeCmd}

	result, err := NewExactFeatureExecutor(fixture.local).Execute(
		context.Background(),
		fixture.request,
	)
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if fixture.sender.calls.Load() != 1 {
		t.Fatalf("RoundTrip() calls = %d, want 1", fixture.sender.calls.Load())
	}
	if result.CorrelationKey != 52 {
		t.Fatalf("correlation key = %d, want 52", result.CorrelationKey)
	}
	if result.Response.ResultData == nil {
		t.Fatal("result did not preserve the typed result command")
	}
	if result.Request.ManufacturerSpecificExtension != &extension {
		t.Fatal("result did not preserve the opaque typed request extension")
	}
}

func TestExactFeatureExecutorPreservesReadReplyExtension(t *testing.T) {
	extension := "opaque-read-extension"
	fixture := newExecutorFixture(t, true, false, func(
		_ context.Context,
		request spineapi.CorrelatedRequest,
	) (spineapi.CorrelatedResponse, error) {
		response := readReply(request, 53)
		response.Cmd.ManufacturerSpecificExtension = &extension
		return response, nil
	})

	result, err := NewExactFeatureExecutor(fixture.local).Execute(
		context.Background(),
		fixture.request,
	)
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if result.Response.ManufacturerSpecificExtension != &extension {
		t.Fatal("result did not preserve the opaque typed response extension")
	}
}

func TestExactFeatureExecutorExactSelection(t *testing.T) {
	fixture := newExecutorFixture(t, true, false, func(
		_ context.Context,
		request spineapi.CorrelatedRequest,
	) (spineapi.CorrelatedResponse, error) {
		return readReply(request, 1), nil
	})
	executor := NewExactFeatureExecutor(fixture.local)

	tests := []struct {
		name   string
		mutate func(*ExactFeatureRequest)
		want   error
	}{
		{
			name: "wrong remote device address",
			mutate: func(request *ExactFeatureRequest) {
				request.Target.Address.Device = ptr(model.AddressDeviceType("other-device"))
			},
			want: ErrExactTargetNotFound,
		},
		{
			name: "wrong remote entity address",
			mutate: func(request *ExactFeatureRequest) {
				request.Target.Address.Entity = []model.AddressEntityType{9}
			},
			want: ErrExactTargetNotFound,
		},
		{
			name: "wrong remote feature address",
			mutate: func(request *ExactFeatureRequest) {
				request.Target.Address.Feature = ptr(model.AddressFeatureType(9))
			},
			want: ErrExactTargetNotFound,
		},
		{
			name: "wrong feature type",
			mutate: func(request *ExactFeatureRequest) {
				request.Target.FeatureType = model.FeatureTypeTypeLoadControl
			},
			want: ErrExactTargetMismatch,
		},
		{
			name: "wrong feature role",
			mutate: func(request *ExactFeatureRequest) {
				request.Target.Role = model.RoleTypeClient
			},
			want: ErrExactTargetMismatch,
		},
		{
			name: "wrong source device address",
			mutate: func(request *ExactFeatureRequest) {
				request.Source.Device = ptr(model.AddressDeviceType("other-local"))
			},
			want: ErrExactSourceNotFound,
		},
		{
			name: "wrong source role",
			mutate: func(request *ExactFeatureRequest) {
				request.Target.Role = model.RoleTypeClient
			},
			want: ErrExactTargetMismatch,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			request := fixture.request
			request.Source = cloneAddress(fixture.request.Source)
			request.Target.Address = cloneAddress(fixture.request.Target.Address)
			test.mutate(&request)

			_, err := executor.Execute(context.Background(), request)
			if !errors.Is(err, test.want) {
				t.Fatalf("Execute() error = %v, want %v", err, test.want)
			}
		})
	}

	if fixture.sender.calls.Load() != 0 {
		t.Fatalf("RoundTrip() calls = %d, want 0", fixture.sender.calls.Load())
	}
}

func TestExactFeatureExecutorRejectsAmbiguousRemoteDevice(t *testing.T) {
	fixture := newExecutorFixture(t, true, false, func(
		_ context.Context,
		request spineapi.CorrelatedRequest,
	) (spineapi.CorrelatedResponse, error) {
		return readReply(request, 1), nil
	})
	duplicate := spine.NewDeviceRemote(fixture.local, "duplicate-ski", fixture.sender)
	duplicateAddress := *fixture.remote.Address()
	duplicateType := model.DeviceTypeTypeSubmeter
	duplicate.UpdateDevice(&model.NetworkManagementDeviceDescriptionDataType{
		DeviceAddress: &model.DeviceAddressType{Device: &duplicateAddress},
		DeviceType:    &duplicateType,
	})
	fixture.local.AddRemoteDeviceForSki(duplicate.Ski(), duplicate)

	_, err := NewExactFeatureExecutor(fixture.local).Execute(
		context.Background(),
		fixture.request,
	)
	if !errors.Is(err, ErrExactTargetAmbiguous) {
		t.Fatalf("Execute() error = %v, want %v", err, ErrExactTargetAmbiguous)
	}
	if fixture.sender.calls.Load() != 0 {
		t.Fatalf("RoundTrip() calls = %d, want 0", fixture.sender.calls.Load())
	}
}

func TestExactFeatureExecutorDeclaredOperationGate(t *testing.T) {
	tests := []struct {
		name      string
		read      bool
		write     bool
		operation ExactFeatureOperation
		commands  []model.CmdType
	}{
		{
			name:      "read not declared",
			operation: ExactFeatureOperationRead,
		},
		{
			name:      "write not declared",
			operation: ExactFeatureOperationWrite,
			commands: []model.CmdType{{
				MeasurementListData: &model.MeasurementListDataType{},
			}},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newExecutorFixture(t, test.read, test.write, func(
				_ context.Context,
				request spineapi.CorrelatedRequest,
			) (spineapi.CorrelatedResponse, error) {
				return readReply(request, 1), nil
			})
			fixture.request.Operation = test.operation
			fixture.request.Commands = test.commands

			_, err := NewExactFeatureExecutor(fixture.local).Execute(
				context.Background(),
				fixture.request,
			)
			if !errors.Is(err, ErrExactOperationNotSupported) {
				t.Fatalf("Execute() error = %v, want %v", err, ErrExactOperationNotSupported)
			}
			if fixture.sender.calls.Load() != 0 {
				t.Fatalf("RoundTrip() calls = %d, want 0", fixture.sender.calls.Load())
			}
		})
	}
}

func TestExactFeatureExecutorForbiddenRequestsAreZeroContact(t *testing.T) {
	measurementFunction := model.FunctionTypeMeasurementListData
	descriptionFunction := model.FunctionTypeMeasurementDescriptionListData
	partial := model.ElementTagType{}
	deleteTag := model.ElementTagType{}
	selector := model.MeasurementListDataSelectorsType{}
	fullWrite := model.CmdType{
		MeasurementListData: &model.MeasurementListDataType{},
	}

	tests := []struct {
		name      string
		operation ExactFeatureOperation
		commands  []model.CmdType
		want      error
	}{
		{
			name:      "read command injection",
			operation: ExactFeatureOperationRead,
			commands:  []model.CmdType{{Function: &measurementFunction}},
			want:      ErrExactPartialOperation,
		},
		{
			name:      "partial read selector",
			operation: ExactFeatureOperationRead,
			commands: []model.CmdType{{
				Filter: []model.FilterType{{
					CmdControl:                   &model.CmdControlType{Partial: &partial},
					MeasurementListDataSelectors: &selector,
				}},
			}},
			want: ErrExactPartialOperation,
		},
		{
			name:      "write missing command",
			operation: ExactFeatureOperationWrite,
			want:      ErrExactCommandCount,
		},
		{
			name:      "write multi command",
			operation: ExactFeatureOperationWrite,
			commands:  []model.CmdType{fullWrite, fullWrite},
			want:      ErrExactCommandCount,
		},
		{
			name:      "write filter",
			operation: ExactFeatureOperationWrite,
			commands: []model.CmdType{{
				MeasurementListData: &model.MeasurementListDataType{},
				Filter:              []model.FilterType{{FilterId: ptr(model.FilterIdType(1))}},
			}},
			want: ErrExactPartialOperation,
		},
		{
			name:      "write partial",
			operation: ExactFeatureOperationWrite,
			commands: []model.CmdType{{
				MeasurementListData: &model.MeasurementListDataType{},
				Filter: []model.FilterType{{
					CmdControl: &model.CmdControlType{Partial: &partial},
				}},
			}},
			want: ErrExactPartialOperation,
		},
		{
			name:      "write filter delete",
			operation: ExactFeatureOperationWrite,
			commands: []model.CmdType{{
				MeasurementListData: &model.MeasurementListDataType{},
				Filter: []model.FilterType{{
					CmdControl: &model.CmdControlType{Delete: &deleteTag},
				}},
			}},
			want: ErrExactPartialOperation,
		},
		{
			name:      "write function option",
			operation: ExactFeatureOperationWrite,
			commands: []model.CmdType{{
				Function:            &measurementFunction,
				MeasurementListData: &model.MeasurementListDataType{},
			}},
			want: ErrExactPartialOperation,
		},
		{
			name:      "wrong typed function",
			operation: ExactFeatureOperationWrite,
			commands: []model.CmdType{{
				Function:                       &descriptionFunction,
				MeasurementDescriptionListData: &model.MeasurementDescriptionListDataType{},
			}},
			want: ErrExactFunctionMismatch,
		},
		{
			name:      "empty typed command",
			operation: ExactFeatureOperationWrite,
			commands:  []model.CmdType{{}},
			want:      ErrExactFunctionMismatch,
		},
		{
			name:      "invoke call",
			operation: ExactFeatureOperation(model.CmdClassifierTypeCall),
			want:      ErrExactOperationNotSupported,
		},
		{
			name:      "notify",
			operation: ExactFeatureOperation(model.CmdClassifierTypeNotify),
			want:      ErrExactOperationNotSupported,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newExecutorFixture(t, true, true, func(
				_ context.Context,
				request spineapi.CorrelatedRequest,
			) (spineapi.CorrelatedResponse, error) {
				return readReply(request, 1), nil
			})
			fixture.request.Operation = test.operation
			fixture.request.Commands = test.commands

			_, err := NewExactFeatureExecutor(fixture.local).Execute(
				context.Background(),
				fixture.request,
			)
			if !errors.Is(err, test.want) {
				t.Fatalf("Execute() error = %v, want %v", err, test.want)
			}
			if fixture.sender.calls.Load() != 0 {
				t.Fatalf("RoundTrip() calls = %d, want 0", fixture.sender.calls.Load())
			}
		})
	}
}

func TestExactFeatureExecutorMissingTargetIsZeroContact(t *testing.T) {
	fixture := newExecutorFixture(t, true, false, func(
		_ context.Context,
		request spineapi.CorrelatedRequest,
	) (spineapi.CorrelatedResponse, error) {
		return readReply(request, 1), nil
	})
	fixture.request.Target.Address.Device = nil

	_, err := NewExactFeatureExecutor(fixture.local).Execute(
		context.Background(),
		fixture.request,
	)
	if !errors.Is(err, ErrInvalidExactTarget) {
		t.Fatalf("Execute() error = %v, want %v", err, ErrInvalidExactTarget)
	}
	if fixture.sender.calls.Load() != 0 {
		t.Fatalf("RoundTrip() calls = %d, want 0", fixture.sender.calls.Load())
	}
}

func TestExactFeatureExecutorMissingRoundTripperIsZeroContact(t *testing.T) {
	fixture := newExecutorFixture(t, true, false, func(
		_ context.Context,
		request spineapi.CorrelatedRequest,
	) (spineapi.CorrelatedResponse, error) {
		return readReply(request, 1), nil
	})
	legacy := &legacySender{}
	remote := spine.NewDeviceRemote(fixture.local, "legacy-ski", legacy)
	remoteAddress := model.AddressDeviceType("legacy-device")
	remoteType := model.DeviceTypeTypeSubmeter
	remote.UpdateDevice(&model.NetworkManagementDeviceDescriptionDataType{
		DeviceAddress: &model.DeviceAddressType{Device: &remoteAddress},
		DeviceType:    &remoteType,
	})
	remoteEntity := spine.NewEntityRemote(
		remote,
		model.EntityTypeTypeGridConnectionPointOfPremises,
		[]model.AddressEntityType{2},
	)
	remoteFeature := spine.NewFeatureRemote(
		1,
		remoteEntity,
		model.FeatureTypeTypeMeasurement,
		model.RoleTypeServer,
	)
	function := model.FunctionTypeMeasurementListData
	remoteFeature.SetOperations([]model.FunctionPropertyType{{
		Function: &function,
		PossibleOperations: &model.PossibleOperationsType{
			Read: &model.PossibleOperationsReadType{},
		},
	}})
	remoteEntity.AddFeature(remoteFeature)
	remote.AddEntity(remoteEntity)
	fixture.local.AddRemoteDeviceForSki(remote.Ski(), remote)

	request := fixture.request
	request.Target.Address = *remoteFeature.Address()
	_, err := NewExactFeatureExecutor(fixture.local).Execute(context.Background(), request)
	if !errors.Is(err, ErrExactRoundTripperUnavailable) {
		t.Fatalf("Execute() error = %v, want %v", err, ErrExactRoundTripperUnavailable)
	}
	if fixture.sender.calls.Load() != 0 {
		t.Fatalf("other RoundTrip() calls = %d, want 0", fixture.sender.calls.Load())
	}
}

func TestExactFeatureExecutorTypedRemoteError(t *testing.T) {
	description := model.DescriptionType("rejected")
	remoteError := &spineapi.CorrelatedRemoteError{
		ErrorNumber: model.ErrorNumberTypeCommandNotSupported,
		Description: &description,
	}
	fixture := newExecutorFixture(t, false, true, func(
		_ context.Context,
		request spineapi.CorrelatedRequest,
	) (spineapi.CorrelatedResponse, error) {
		response := writeResult(request, 88)
		response.Cmd.ResultData.ErrorNumber = ptr(model.ErrorNumberTypeCommandNotSupported)
		response.Cmd.ResultData.Description = &description
		return response, remoteError
	})
	fixture.request.Operation = ExactFeatureOperationWrite
	fixture.request.Commands = []model.CmdType{{
		MeasurementListData: &model.MeasurementListDataType{},
	}}

	result, err := NewExactFeatureExecutor(fixture.local).Execute(
		context.Background(),
		fixture.request,
	)
	if !errors.Is(err, remoteError) {
		t.Fatalf("Execute() error = %v, want original remote error %v", err, remoteError)
	}
	if result.RemoteError != remoteError {
		t.Fatalf("result remote error = %p, want %p", result.RemoteError, remoteError)
	}
	if result.ProtocolError != nil {
		t.Fatalf("result protocol error = %v, want nil", result.ProtocolError)
	}
	if result.CorrelationKey != 88 || result.Response.ResultData == nil {
		t.Fatalf("correlated remote result was not preserved: %+v", result)
	}
}

func TestExactFeatureExecutorTerminalPropagation(t *testing.T) {
	disconnectErr := spineapi.ErrCorrelatedRoundTripClosed
	malformedErr := &spineapi.CorrelatedProtocolError{Message: "malformed response"}

	tests := []struct {
		name string
		run  func(context.Context) error
		want error
	}{
		{
			name: "cancelled",
			run: func(ctx context.Context) error {
				<-ctx.Done()
				return ctx.Err()
			},
			want: context.Canceled,
		},
		{
			name: "timeout",
			run: func(ctx context.Context) error {
				<-ctx.Done()
				return ctx.Err()
			},
			want: context.DeadlineExceeded,
		},
		{
			name: "disconnect",
			run: func(context.Context) error {
				return disconnectErr
			},
			want: disconnectErr,
		},
		{
			name: "malformed",
			run: func(context.Context) error {
				return malformedErr
			},
			want: malformedErr,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newExecutorFixture(t, true, false, func(
				ctx context.Context,
				_ spineapi.CorrelatedRequest,
			) (spineapi.CorrelatedResponse, error) {
				return spineapi.CorrelatedResponse{}, test.run(ctx)
			})

			ctx := context.Background()
			cancel := func() {}
			switch test.name {
			case "cancelled":
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			case "timeout":
				ctx, cancel = context.WithTimeout(ctx, time.Millisecond)
			}
			defer cancel()

			result, err := NewExactFeatureExecutor(fixture.local).Execute(ctx, fixture.request)
			if !errors.Is(err, test.want) {
				t.Fatalf("Execute() error = %v, want %v", err, test.want)
			}
			if result.RequestedAt.IsZero() || result.RespondedAt.IsZero() {
				t.Fatalf("terminal timestamps = %v / %v, want both set", result.RequestedAt, result.RespondedAt)
			}
			if test.name == "malformed" && result.ProtocolError != malformedErr {
				t.Fatalf("protocol error = %p, want %p", result.ProtocolError, malformedErr)
			}
			if fixture.sender.calls.Load() != 1 {
				t.Fatalf("RoundTrip() calls = %d, want 1", fixture.sender.calls.Load())
			}
		})
	}
}

func TestExactFeatureExecutorRejectsEmptySuccess(t *testing.T) {
	fixture := newExecutorFixture(t, true, false, func(
		context.Context,
		spineapi.CorrelatedRequest,
	) (spineapi.CorrelatedResponse, error) {
		return spineapi.CorrelatedResponse{}, nil
	})

	result, err := NewExactFeatureExecutor(fixture.local).Execute(
		context.Background(),
		fixture.request,
	)
	if !errors.Is(err, ErrMalformedExactResponse) {
		t.Fatalf("Execute() error = %v, want %v", err, ErrMalformedExactResponse)
	}
	if result.ProtocolError == nil {
		t.Fatal("empty success did not return a typed protocol error")
	}
}

func TestExactFeatureExecutorSynchronousReplyAndTimestampOrder(t *testing.T) {
	callObserved := time.Now()
	returnObserved := callObserved.Add(time.Millisecond)
	fixture := newExecutorFixture(t, true, false, func(
		_ context.Context,
		request spineapi.CorrelatedRequest,
	) (spineapi.CorrelatedResponse, error) {
		callObserved = time.Now()
		response := readReply(request, 99)
		returnObserved = time.Now()
		return response, nil
	})

	before := time.Now()
	result, err := NewExactFeatureExecutor(fixture.local).Execute(
		context.Background(),
		fixture.request,
	)
	after := time.Now()
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if result.CorrelationKey != 99 || result.Response.MeasurementListData == nil {
		t.Fatalf("synchronous reply was not propagated: %+v", result)
	}
	if result.RequestedAt.Before(before) || result.RequestedAt.After(callObserved) {
		t.Fatalf("requested_at = %v, want in [%v, %v]", result.RequestedAt, before, callObserved)
	}
	if result.RespondedAt.Before(returnObserved) || result.RespondedAt.After(after) {
		t.Fatalf("responded_at = %v, want in [%v, %v]", result.RespondedAt, returnObserved, after)
	}
	if result.RespondedAt.Before(result.RequestedAt) {
		t.Fatalf("responded_at %v precedes requested_at %v", result.RespondedAt, result.RequestedAt)
	}
}

func TestExactFeatureExecutorAcceptsExactCompatibleRolePairs(t *testing.T) {
	tests := []struct {
		name       string
		localType  model.FeatureTypeType
		localRole  model.RoleType
		remoteType model.FeatureTypeType
		remoteRole model.RoleType
	}{
		{
			name:       "remote server",
			localType:  model.FeatureTypeTypeMeasurement,
			localRole:  model.RoleTypeClient,
			remoteType: model.FeatureTypeTypeMeasurement,
			remoteRole: model.RoleTypeServer,
		},
		{
			name:       "remote client",
			localType:  model.FeatureTypeTypeMeasurement,
			localRole:  model.RoleTypeServer,
			remoteType: model.FeatureTypeTypeMeasurement,
			remoteRole: model.RoleTypeClient,
		},
		{
			name:       "special pair",
			localType:  model.FeatureTypeTypeMeasurement,
			localRole:  model.RoleTypeSpecial,
			remoteType: model.FeatureTypeTypeMeasurement,
			remoteRole: model.RoleTypeSpecial,
		},
		{
			name:       "generic local client",
			localType:  model.FeatureTypeTypeGeneric,
			localRole:  model.RoleTypeClient,
			remoteType: model.FeatureTypeTypeMeasurement,
			remoteRole: model.RoleTypeServer,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newRoleFixture(
				t,
				test.localType,
				test.localRole,
				test.remoteType,
				test.remoteRole,
			)

			result, err := NewExactFeatureExecutor(fixture.local).Execute(
				context.Background(),
				fixture.request,
			)
			if err != nil {
				t.Fatalf("Execute() error = %v", err)
			}
			if fixture.sender.calls.Load() != 1 {
				t.Fatalf("RoundTrip() calls = %d, want 1", fixture.sender.calls.Load())
			}
			if result.Response.MeasurementListData == nil {
				t.Fatal("typed response was not preserved")
			}
		})
	}
}

func TestExactFeatureExecutorRejectsCallFunctionUnderWrite(t *testing.T) {
	fixture := newExecutorFixture(t, false, true, func(
		_ context.Context,
		request spineapi.CorrelatedRequest,
	) (spineapi.CorrelatedResponse, error) {
		return writeResult(request, 1), nil
	})
	callFunction := model.FunctionTypeDataTunnelingCall
	fixture.remoteFeature.SetOperations([]model.FunctionPropertyType{{
		Function: &callFunction,
		PossibleOperations: &model.PossibleOperationsType{
			Write: &model.PossibleOperationsWriteType{},
		},
	}})
	fixture.request.Target.Function = callFunction
	fixture.request.Operation = ExactFeatureOperationWrite
	fixture.request.Commands = []model.CmdType{{
		DataTunnelingCall: &model.DataTunnelingCallType{},
	}}

	_, err := NewExactFeatureExecutor(fixture.local).Execute(
		context.Background(),
		fixture.request,
	)
	if !errors.Is(err, ErrExactOperationNotSupported) {
		t.Fatalf("Execute() error = %v, want %v", err, ErrExactOperationNotSupported)
	}
	if fixture.sender.calls.Load() != 0 {
		t.Fatalf("RoundTrip() calls = %d, want 0", fixture.sender.calls.Load())
	}
}

func TestExactFeatureExecutorRejectsCrossFamilyWrite(t *testing.T) {
	fixture := newExecutorFixture(t, false, true, func(
		_ context.Context,
		request spineapi.CorrelatedRequest,
	) (spineapi.CorrelatedResponse, error) {
		return writeResult(request, 1), nil
	})
	function := model.FunctionTypeLoadControlLimitListData
	fixture.remoteFeature.SetOperations([]model.FunctionPropertyType{{
		Function: &function,
		PossibleOperations: &model.PossibleOperationsType{
			Write: &model.PossibleOperationsWriteType{},
		},
	}})
	fixture.request.Target.Function = function
	fixture.request.Operation = ExactFeatureOperationWrite
	fixture.request.Commands = []model.CmdType{{
		LoadControlLimitListData: &model.LoadControlLimitListDataType{},
	}}

	_, err := NewExactFeatureExecutor(fixture.local).Execute(
		context.Background(),
		fixture.request,
	)
	if !errors.Is(err, ErrExactFunctionDataUnavailable) {
		t.Fatalf("Execute() error = %v, want %v", err, ErrExactFunctionDataUnavailable)
	}
	if fixture.sender.calls.Load() != 0 {
		t.Fatalf("RoundTrip() calls = %d, want 0", fixture.sender.calls.Load())
	}
}

func TestExactFeatureExecutorRejectsMixedWriteResult(t *testing.T) {
	measurementFunction := model.FunctionTypeMeasurementListData
	partial := model.ElementTagType{}
	tests := []struct {
		name   string
		mutate func(*model.CmdType)
	}{
		{
			name: "typed data",
			mutate: func(command *model.CmdType) {
				command.MeasurementListData = &model.MeasurementListDataType{}
			},
		},
		{
			name: "function",
			mutate: func(command *model.CmdType) {
				command.Function = &measurementFunction
			},
		},
		{
			name: "filter",
			mutate: func(command *model.CmdType) {
				command.Filter = []model.FilterType{{
					CmdControl: &model.CmdControlType{Partial: &partial},
				}}
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newExecutorFixture(t, false, true, func(
				_ context.Context,
				request spineapi.CorrelatedRequest,
			) (spineapi.CorrelatedResponse, error) {
				response := writeResult(request, 54)
				test.mutate(&response.Cmd)
				return response, nil
			})
			fixture.request.Operation = ExactFeatureOperationWrite
			fixture.request.Commands = []model.CmdType{{
				MeasurementListData: &model.MeasurementListDataType{},
			}}

			result, err := NewExactFeatureExecutor(fixture.local).Execute(
				context.Background(),
				fixture.request,
			)
			if !errors.Is(err, ErrMalformedExactResponse) {
				t.Fatalf("Execute() error = %v, want %v", err, ErrMalformedExactResponse)
			}
			if result.ProtocolError == nil {
				t.Fatal("mixed result did not return a typed protocol error")
			}
			if fixture.sender.calls.Load() != 1 {
				t.Fatalf("RoundTrip() calls = %d, want 1", fixture.sender.calls.Load())
			}
		})
	}
}

func TestExactFeatureExecutorFactoryOmissionIsZeroContact(t *testing.T) {
	fixture := newRoleFixture(
		t,
		model.FeatureTypeTypeNodeManagement,
		model.RoleTypeSpecial,
		model.FeatureTypeTypeNodeManagement,
		model.RoleTypeSpecial,
	)
	function := model.FunctionTypeNodeManagementSubscriptionData
	fixture.remoteFeature.SetOperations([]model.FunctionPropertyType{{
		Function: &function,
		PossibleOperations: &model.PossibleOperationsType{
			Read: &model.PossibleOperationsReadType{},
		},
	}})
	fixture.request.Target.Function = function

	_, err := NewExactFeatureExecutor(fixture.local).Execute(
		context.Background(),
		fixture.request,
	)
	if !errors.Is(err, ErrExactFunctionDataUnavailable) {
		t.Fatalf("Execute() error = %v, want %v", err, ErrExactFunctionDataUnavailable)
	}
	if fixture.sender.calls.Load() != 0 {
		t.Fatalf("RoundTrip() calls = %d, want 0", fixture.sender.calls.Load())
	}
}

func TestExactFeatureExecutorRejectsZeroCorrelationKey(t *testing.T) {
	fixture := newExecutorFixture(t, true, false, func(
		_ context.Context,
		request spineapi.CorrelatedRequest,
	) (spineapi.CorrelatedResponse, error) {
		return readReply(request, 0), nil
	})

	result, err := NewExactFeatureExecutor(fixture.local).Execute(
		context.Background(),
		fixture.request,
	)
	if !errors.Is(err, ErrMalformedExactResponse) {
		t.Fatalf("Execute() error = %v, want %v", err, ErrMalformedExactResponse)
	}
	if result.ProtocolError == nil {
		t.Fatal("zero correlation key did not return a typed protocol error")
	}
}

func newRoleFixture(
	t *testing.T,
	localType model.FeatureTypeType,
	localRole model.RoleType,
	remoteType model.FeatureTypeType,
	remoteRole model.RoleType,
) *executorFixture {
	t.Helper()

	var sender *roundTripSender
	sender = &roundTripSender{roundTrip: func(
		_ context.Context,
		request spineapi.CorrelatedRequest,
	) (spineapi.CorrelatedResponse, error) {
		return readReply(request, 71), nil
	}}
	local := spine.NewDeviceLocal(
		"brand",
		"model",
		"serial",
		"code",
		"role-local",
		model.DeviceTypeTypeEnergyManagementSystem,
		model.NetworkManagementFeatureSetTypeSmart,
	)
	localEntity := spine.NewEntityLocal(
		local,
		model.EntityTypeTypeDeviceInformation,
		[]model.AddressEntityType{2},
		time.Second,
	)
	localFeature := spine.NewFeatureLocal(1, localEntity, localType, localRole)
	localEntity.AddFeature(localFeature)
	local.AddEntity(localEntity)

	remote := spine.NewDeviceRemote(local, "role-remote-ski", sender)
	remoteAddress := model.AddressDeviceType("role-remote")
	deviceType := model.DeviceTypeTypeSubmeter
	remote.UpdateDevice(&model.NetworkManagementDeviceDescriptionDataType{
		DeviceAddress: &model.DeviceAddressType{Device: &remoteAddress},
		DeviceType:    &deviceType,
	})
	remoteEntity := spine.NewEntityRemote(
		remote,
		model.EntityTypeTypeGridConnectionPointOfPremises,
		[]model.AddressEntityType{2},
	)
	remoteFeature := spine.NewFeatureRemote(1, remoteEntity, remoteType, remoteRole)
	function := model.FunctionTypeMeasurementListData
	remoteFeature.SetOperations([]model.FunctionPropertyType{{
		Function: &function,
		PossibleOperations: &model.PossibleOperationsType{
			Read: &model.PossibleOperationsReadType{},
		},
	}})
	remoteEntity.AddFeature(remoteFeature)
	remote.AddEntity(remoteEntity)
	local.AddRemoteDeviceForSki(remote.Ski(), remote)

	return &executorFixture{
		local:         local,
		localFeature:  localFeature,
		remote:        remote,
		remoteFeature: remoteFeature,
		sender:        sender,
		request: ExactFeatureRequest{
			Source: *localFeature.Address(),
			Target: ExactFeatureTarget{
				Address:     *remoteFeature.Address(),
				FeatureType: remoteFeature.Type(),
				Role:        remoteFeature.Role(),
				Function:    function,
			},
			Operation: ExactFeatureOperationRead,
		},
	}
}

func cloneAddress(address model.FeatureAddressType) model.FeatureAddressType {
	result := address
	if address.Device != nil {
		result.Device = ptr(*address.Device)
	}
	result.Entity = append([]model.AddressEntityType(nil), address.Entity...)
	if address.Feature != nil {
		result.Feature = ptr(*address.Feature)
	}
	return result
}
