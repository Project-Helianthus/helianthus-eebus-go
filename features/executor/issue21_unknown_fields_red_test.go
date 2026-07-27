package executor

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"
	"unsafe"

	spineapi "github.com/Project-Helianthus/helianthus-spine-go/api"
	"github.com/Project-Helianthus/helianthus-spine-go/model"
)

func TestIssue21ExactFeatureResultCarriesCorrelatedUnknownFields(t *testing.T) {
	resultType := reflect.TypeOf(ExactFeatureResult{})
	resultField, ok := resultType.FieldByName("UnknownFields")
	if !ok {
		t.Fatal("ExactFeatureResult drops correlated response bounded unknown fields")
	}

	responseType := reflect.TypeOf(spineapi.CorrelatedResponse{})
	responseField, ok := responseType.FieldByName("UnknownFields")
	if !ok {
		t.Fatal("pinned spine CorrelatedResponse.UnknownFields carrier is missing")
	}
	if resultField.Type != responseField.Type {
		t.Fatalf(
			"ExactFeatureResult.UnknownFields type = %s, want %s",
			resultField.Type,
			responseField.Type,
		)
	}
	for _, forbidden := range []string{
		"Raw", "RawMessage", "Message", "Frame", "Payload", "Transcript", "Bytes",
	} {
		if _, exists := resultType.FieldByName(forbidden); exists {
			t.Errorf("ExactFeatureResult exposes forbidden whole-payload field %q", forbidden)
		}
	}
}

func TestIssue21ExactFeatureResultKnownOnlyResponse(t *testing.T) {
	fixture := newExecutorFixture(t, true, false, func(
		_ context.Context,
		request spineapi.CorrelatedRequest,
	) (spineapi.CorrelatedResponse, error) {
		return readReply(request, 91), nil
	})

	result, err := NewExactFeatureExecutor(fixture.local, fixture.runtime).Execute(
		context.Background(),
		fixture.request,
	)
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if len(result.UnknownFields) != 0 {
		t.Fatalf("known-only unknown fields = %v, want none", result.UnknownFields)
	}
}

func TestIssue21ExactFeatureResultPreservesNestedUnknownFieldOrder(t *testing.T) {
	fields := []spineapi.CorrelatedUnknownField{
		unknownField("/datagram/payload/cmd/0/futureAlpha", `{"items":[1,{"ok":true}]}`),
		unknownField("/datagram/payload/cmd/0/futureBeta", `"opaque"`),
		unknownField("/datagram/payload/futureEnvelope", `17`),
	}
	fixture := newExecutorFixture(t, true, false, func(
		_ context.Context,
		request spineapi.CorrelatedRequest,
	) (spineapi.CorrelatedResponse, error) {
		response := readReply(request, 92)
		response.UnknownFields = fields
		return response, nil
	})

	result, err := NewExactFeatureExecutor(fixture.local, fixture.runtime).Execute(
		context.Background(),
		fixture.request,
	)
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	assertUnknownFieldsEqual(t, result.UnknownFields, fields)
}

func TestIssue21ExactFeatureResultDeepCopiesUnknownFields(t *testing.T) {
	fields := []spineapi.CorrelatedUnknownField{
		unknownField("/datagram/payload/cmd/0/futureData", `{"level":2}`),
	}
	fixture := newExecutorFixture(t, true, false, func(
		_ context.Context,
		request spineapi.CorrelatedRequest,
	) (spineapi.CorrelatedResponse, error) {
		response := readReply(request, 93)
		response.UnknownFields = fields
		return response, nil
	})
	executor := NewExactFeatureExecutor(fixture.local, fixture.runtime)

	first, err := executor.Execute(context.Background(), fixture.request)
	if err != nil {
		t.Fatalf("first Execute() error = %v", err)
	}
	if unsafe.StringData(first.UnknownFields[0].Path) ==
		unsafe.StringData(fields[0].Path) {
		t.Fatal("result path shares its backing store with the correlated response")
	}
	if unsafe.SliceData(first.UnknownFields[0].Value) ==
		unsafe.SliceData(fields[0].Value) {
		t.Fatal("result value shares its backing store with the correlated response")
	}

	first.UnknownFields[0].Path = "/mutated"
	first.UnknownFields[0].Value[0] = 'x'
	if fields[0].Path != "/datagram/payload/cmd/0/futureData" ||
		string(fields[0].Value) != `{"level":2}` {
		t.Fatalf("caller mutation changed source response: %v", fields)
	}

	second, err := executor.Execute(context.Background(), fixture.request)
	if err != nil {
		t.Fatalf("second Execute() error = %v", err)
	}
	assertUnknownFieldsEqual(t, second.UnknownFields, fields)
	if unsafe.StringData(second.UnknownFields[0].Path) ==
		unsafe.StringData(first.UnknownFields[0].Path) ||
		unsafe.SliceData(second.UnknownFields[0].Value) ==
			unsafe.SliceData(first.UnknownFields[0].Value) {
		t.Fatal("separate results share unknown field storage")
	}
}

func TestIssue21ExactFeatureResultPreservesUnknownFieldsOnTypedErrors(t *testing.T) {
	t.Run("remote", func(t *testing.T) {
		fields := []spineapi.CorrelatedUnknownField{
			unknownField("/datagram/payload/cmd/0/futureResult", `"denied-detail"`),
		}
		description := model.DescriptionType("rejected")
		remoteError := &spineapi.CorrelatedRemoteError{
			ErrorNumber: model.ErrorNumberTypeCommandNotSupported,
			Description: &description,
		}
		fixture := newExecutorFixture(t, false, true, func(
			_ context.Context,
			request spineapi.CorrelatedRequest,
		) (spineapi.CorrelatedResponse, error) {
			response := writeResult(request, 94)
			response.Cmd.ResultData.ErrorNumber = ptr(model.ErrorNumberTypeCommandNotSupported)
			response.UnknownFields = fields
			return response, remoteError
		})
		fixture.request.Operation = ExactFeatureOperationWrite
		fixture.request.Commands = []model.CmdType{{
			MeasurementListData: &model.MeasurementListDataType{},
		}}

		result, err := NewExactFeatureExecutor(fixture.local, fixture.runtime).Execute(
			context.Background(),
			fixture.request,
		)
		if !errors.Is(err, remoteError) {
			t.Fatalf("Execute() error = %v, want %v", err, remoteError)
		}
		if result.RemoteError != remoteError || result.ProtocolError != nil {
			t.Fatalf(
				"typed errors = remote:%p protocol:%v, want remote:%p",
				result.RemoteError,
				result.ProtocolError,
				remoteError,
			)
		}
		assertUnknownFieldsEqual(t, result.UnknownFields, fields)
		assertUnknownFieldsDoNotAlias(t, result.UnknownFields, fields)
	})

	t.Run("protocol", func(t *testing.T) {
		fields := []spineapi.CorrelatedUnknownField{
			unknownField("/datagram/payload/cmd/0/futureReply", `{"reason":"bad-shape"}`),
		}
		fixture := newExecutorFixture(t, true, false, func(
			_ context.Context,
			request spineapi.CorrelatedRequest,
		) (spineapi.CorrelatedResponse, error) {
			response := readReply(request, 95)
			response.Header.CmdClassifier = ptr(model.CmdClassifierTypeResult)
			response.UnknownFields = fields
			return response, nil
		})

		result, err := NewExactFeatureExecutor(fixture.local, fixture.runtime).Execute(
			context.Background(),
			fixture.request,
		)
		if !errors.Is(err, ErrMalformedExactResponse) {
			t.Fatalf("Execute() error = %v, want %v", err, ErrMalformedExactResponse)
		}
		if result.ProtocolError == nil || result.RemoteError != nil {
			t.Fatalf(
				"typed errors = remote:%v protocol:%v, want protocol only",
				result.RemoteError,
				result.ProtocolError,
			)
		}
		assertUnknownFieldsEqual(t, result.UnknownFields, fields)
		assertUnknownFieldsDoNotAlias(t, result.UnknownFields, fields)
	})
}

func TestIssue21ExactFeatureResultFormattingRedactsUnknownValues(t *testing.T) {
	const secret = "issue21-sensitive-unknown-value"
	fields := []spineapi.CorrelatedUnknownField{
		unknownField("/datagram/payload/cmd/0/futureReply", `"`+secret+`"`),
	}
	fixture := newExecutorFixture(t, true, false, func(
		_ context.Context,
		request spineapi.CorrelatedRequest,
	) (spineapi.CorrelatedResponse, error) {
		response := readReply(request, 96)
		response.Header.CmdClassifier = ptr(model.CmdClassifierTypeResult)
		response.UnknownFields = fields
		return response, nil
	})

	result, err := NewExactFeatureExecutor(fixture.local, fixture.runtime).Execute(
		context.Background(),
		fixture.request,
	)
	if !errors.Is(err, ErrMalformedExactResponse) {
		t.Fatalf("Execute() error = %v, want %v", err, ErrMalformedExactResponse)
	}
	formatted := fmt.Sprintf(
		"%v\n%+v\n%#v\n%q\n%v",
		result,
		result,
		result,
		result.UnknownFields[0],
		err,
	)
	if strings.Contains(formatted, secret) {
		t.Fatalf("formatted result disclosed an unknown value: %s", formatted)
	}
}

func TestIssue21ExactFeatureResultConcurrentCopiesAreIndependent(t *testing.T) {
	fields := []spineapi.CorrelatedUnknownField{
		unknownField("/datagram/payload/cmd/0/futureConcurrent", `{"value":1}`),
	}
	fixture := newExecutorFixture(t, true, false, func(
		_ context.Context,
		request spineapi.CorrelatedRequest,
	) (spineapi.CorrelatedResponse, error) {
		response := readReply(request, 97)
		response.UnknownFields = fields
		return response, nil
	})
	executor := NewExactFeatureExecutor(fixture.local, fixture.runtime)

	const workers = 24
	start := make(chan struct{})
	failures := make(chan error, workers)
	var wait sync.WaitGroup
	wait.Add(workers)
	for worker := 0; worker < workers; worker++ {
		go func(worker int) {
			defer wait.Done()
			<-start
			result, err := executor.Execute(context.Background(), fixture.request)
			if err != nil {
				failures <- fmt.Errorf("worker %d Execute: %w", worker, err)
				return
			}
			if len(result.UnknownFields) != 1 {
				failures <- fmt.Errorf(
					"worker %d unknown field count = %d",
					worker,
					len(result.UnknownFields),
				)
				return
			}
			result.UnknownFields[0].Path = fmt.Sprintf("/worker/%d", worker)
			result.UnknownFields[0].Value[0] = '['
		}(worker)
	}
	close(start)
	wait.Wait()
	close(failures)
	for failure := range failures {
		t.Error(failure)
	}
	if fields[0].Path != "/datagram/payload/cmd/0/futureConcurrent" ||
		string(fields[0].Value) != `{"value":1}` {
		t.Fatalf("concurrent result mutation changed source response: %v", fields)
	}
}

func unknownField(path, value string) spineapi.CorrelatedUnknownField {
	return spineapi.CorrelatedUnknownField{
		Path:  path,
		Value: spineapi.CorrelatedUnknownValue([]byte(value)),
	}
}

func assertUnknownFieldsEqual(
	t *testing.T,
	got []spineapi.CorrelatedUnknownField,
	want []spineapi.CorrelatedUnknownField,
) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("unknown fields = %v, want %v", got, want)
	}
	for index := range want {
		if got[index].Path != want[index].Path ||
			string(got[index].Value) != string(want[index].Value) {
			t.Fatalf("unknown field %d = %v, want %v", index, got[index], want[index])
		}
	}
}

func assertUnknownFieldsDoNotAlias(
	t *testing.T,
	got []spineapi.CorrelatedUnknownField,
	source []spineapi.CorrelatedUnknownField,
) {
	t.Helper()
	for index := range source {
		if unsafe.StringData(got[index].Path) == unsafe.StringData(source[index].Path) {
			t.Fatalf("unknown field %d path aliases the source response", index)
		}
		if unsafe.SliceData(got[index].Value) == unsafe.SliceData(source[index].Value) {
			t.Fatalf("unknown field %d value aliases the source response", index)
		}
	}
}
