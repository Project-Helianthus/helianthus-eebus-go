// Package executor provides exact-address, exact-function full SPINE
// operations without feature-family projections or partial-data caching.
package executor

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"time"

	spineapi "github.com/Project-Helianthus/helianthus-spine-go/api"
	"github.com/Project-Helianthus/helianthus-spine-go/model"
	"github.com/Project-Helianthus/helianthus-spine-go/spine"
)

type ExactFeatureExecutor struct {
	local   spineapi.DeviceLocalInterface
	runtime ExactRemoteRuntime
}

func NewExactFeatureExecutor(
	local spineapi.DeviceLocalInterface,
	runtime ExactRemoteRuntime,
) *ExactFeatureExecutor {
	return &ExactFeatureExecutor{
		local:   local,
		runtime: runtime,
	}
}

func (e *ExactFeatureExecutor) Execute(
	ctx context.Context,
	request ExactFeatureRequest,
) (ExactFeatureResult, error) {
	result := ExactFeatureResult{
		Target:    cloneFeatureTarget(request.Target),
		Operation: request.Operation,
	}
	if ctx == nil {
		return result, fmt.Errorf("%w: context is nil", ErrInvalidExactSource)
	}
	if err := validateFeatureAddress(request.Source); err != nil {
		return result, fmt.Errorf("%w: %w", ErrInvalidExactSource, err)
	}
	if err := validateFeatureAddress(request.Target.Address); err != nil ||
		request.Target.FeatureType == "" ||
		request.Target.Role == "" ||
		request.Target.Function == "" {
		return result, fmt.Errorf("%w: address, type, role, and function are required", ErrInvalidExactTarget)
	}
	if request.Target.RemoteIdentity == "" {
		return result, fmt.Errorf("%w: remote identity is required", ErrInvalidExactTarget)
	}
	if request.Target.ConnectionGeneration == 0 {
		return result, fmt.Errorf("%w: positive connection generation is required", ErrInvalidExactTarget)
	}

	classifier, command, err := commandForRequest(request)
	if err != nil {
		return result, err
	}
	source, err := e.resolveSource(request.Source)
	if err != nil {
		return result, err
	}
	feature, err := e.resolveTarget(request.Target)
	if err != nil {
		return result, err
	}
	if !compatibleFeaturePair(source, feature) {
		return result, fmt.Errorf("%w: source and target feature pair", ErrExactSourceMismatch)
	}
	if err := requireDeclaredOperation(feature, request.Target.Function, request.Operation); err != nil {
		return result, err
	}
	switch request.Operation {
	case ExactFeatureOperationRead:
		command, err = fullReadCommand(feature.Type(), request.Target.Function)
		if err != nil {
			return result, err
		}
	case ExactFeatureOperationWrite:
		if feature.Type() != model.FeatureTypeTypeGeneric &&
			!functionDataAvailable(feature.Type(), request.Target.Function) {
			return result, fmt.Errorf(
				"%w: function %q is not typed for feature %q",
				ErrExactFunctionDataUnavailable,
				request.Target.Function,
				feature.Type(),
			)
		}
	}

	correlatedRequest := spineapi.CorrelatedRequest{
		Classifier:  classifier,
		Source:      cloneFeatureAddress(request.Source),
		Destination: cloneFeatureAddress(request.Target.Address),
		AckRequest:  false,
		Cmd:         command,
	}
	result.Request = command
	result.RequestedAt = time.Now()
	response, roundTripErr := e.runtime.RoundTripIfCurrent(
		ctx,
		ExactRemoteBinding{
			DeviceAddress:        *request.Target.Address.Device,
			RemoteIdentity:       request.Target.RemoteIdentity,
			ConnectionGeneration: request.Target.ConnectionGeneration,
		},
		correlatedRequest,
	)
	result.RespondedAt = time.Now()
	result.CorrelationKey = response.CorrelationKey
	result.Response = response.Cmd
	result.UnknownFields = cloneCorrelatedUnknownFields(response.UnknownFields)
	if roundTripErr != nil {
		setTypedError(&result, roundTripErr)
		return result, roundTripErr
	}
	if responseErr := validateResponse(request, correlatedRequest, response); responseErr != nil {
		setTypedError(&result, responseErr)
		return result, responseErr
	}
	return result, nil
}

func commandForRequest(
	request ExactFeatureRequest,
) (model.CmdClassifierType, model.CmdType, error) {
	if strings.HasSuffix(string(request.Target.Function), "Call") {
		return "", model.CmdType{}, ErrExactOperationNotSupported
	}
	switch request.Operation {
	case ExactFeatureOperationRead:
		if len(request.Commands) != 0 {
			return "", model.CmdType{}, ErrExactPartialOperation
		}
		return model.CmdClassifierTypeRead, model.CmdType{}, nil
	case ExactFeatureOperationWrite:
		if len(request.Commands) != 1 {
			return "", model.CmdType{}, ErrExactCommandCount
		}
		command := request.Commands[0]
		function, err := soleTypedFunction(command)
		if err != nil || function != request.Target.Function {
			return "", model.CmdType{}, ErrExactFunctionMismatch
		}
		if len(command.Filter) != 0 || command.Function != nil {
			return "", model.CmdType{}, ErrExactPartialOperation
		}
		return model.CmdClassifierTypeWrite, command, nil
	default:
		return "", model.CmdType{}, ErrExactOperationNotSupported
	}
}

func (e *ExactFeatureExecutor) resolveSource(
	address model.FeatureAddressType,
) (spineapi.FeatureLocalInterface, error) {
	if e == nil || isNil(e.local) || e.local.Address() == nil ||
		*e.local.Address() != *address.Device {
		return nil, ErrExactSourceNotFound
	}

	var matches []spineapi.FeatureLocalInterface
	for _, entity := range e.local.Entities() {
		if isNil(entity) || entity.Address() == nil ||
			!slices.Equal(entity.Address().Entity, address.Entity) {
			continue
		}
		for _, feature := range entity.Features() {
			if !isNil(feature) && equalFeatureAddress(feature.Address(), &address) {
				matches = append(matches, feature)
			}
		}
	}
	switch len(matches) {
	case 0:
		return nil, ErrExactSourceNotFound
	case 1:
		return matches[0], nil
	default:
		return nil, fmt.Errorf("%w: source feature", ErrExactTargetAmbiguous)
	}
}

func (e *ExactFeatureExecutor) resolveTarget(
	target ExactFeatureTarget,
) (spineapi.FeatureRemoteInterface, error) {
	if e == nil || isNil(e.local) {
		return nil, ErrExactTargetNotFound
	}
	if isNil(e.runtime) {
		return nil, ErrExactRemoteResolverUnavailable
	}

	remote, err := e.runtime.ResolveExactRemoteDevice(*target.Address.Device)
	if err != nil {
		return nil, err
	}
	if isNil(remote) {
		return nil, ErrExactTargetNotFound
	}
	if remote.Address() == nil ||
		*remote.Address() != *target.Address.Device {
		return nil, ErrExactTargetMismatch
	}

	var features []spineapi.FeatureRemoteInterface
	for _, entity := range remote.Entities() {
		if isNil(entity) || entity.Address() == nil ||
			!slices.Equal(entity.Address().Entity, target.Address.Entity) {
			continue
		}
		for _, feature := range entity.Features() {
			if !isNil(feature) && equalFeatureAddress(feature.Address(), &target.Address) {
				features = append(features, feature)
			}
		}
	}
	if len(features) == 0 {
		return nil, ErrExactTargetNotFound
	}
	if len(features) != 1 {
		return nil, fmt.Errorf("%w: remote feature address", ErrExactTargetAmbiguous)
	}

	feature := features[0]
	if feature.Type() != target.FeatureType ||
		feature.Role() != target.Role {
		return nil, ErrExactTargetMismatch
	}
	if operations, found := feature.Operations()[target.Function]; !found || isNil(operations) {
		return nil, ErrExactTargetNotFound
	}
	return feature, nil
}

func compatibleFeaturePair(
	source spineapi.FeatureLocalInterface,
	target spineapi.FeatureRemoteInterface,
) bool {
	if source.Type() != target.Type() && source.Type() != model.FeatureTypeTypeGeneric {
		return false
	}
	switch target.Role() {
	case model.RoleTypeServer:
		return source.Role() == model.RoleTypeClient
	case model.RoleTypeClient:
		return source.Role() == model.RoleTypeServer
	case model.RoleTypeSpecial:
		return source.Role() == model.RoleTypeSpecial
	default:
		return false
	}
}

func requireDeclaredOperation(
	feature spineapi.FeatureRemoteInterface,
	function model.FunctionType,
	operation ExactFeatureOperation,
) error {
	operations := feature.Operations()[function]
	switch operation {
	case ExactFeatureOperationRead:
		if !operations.Read() {
			return ErrExactOperationNotSupported
		}
	case ExactFeatureOperationWrite:
		if !operations.Write() {
			return ErrExactOperationNotSupported
		}
	default:
		return ErrExactOperationNotSupported
	}
	return nil
}

func fullReadCommand(
	featureType model.FeatureTypeType,
	function model.FunctionType,
) (command model.CmdType, err error) {
	defer func() {
		if recover() != nil {
			command = model.CmdType{}
			err = fmt.Errorf("%w: unsupported feature type %q", ErrExactFunctionDataUnavailable, featureType)
		}
	}()

	matches := functionDataMatches(featureType, function)
	if len(matches) != 1 {
		return model.CmdType{}, fmt.Errorf(
			"%w: function %q is not typed for feature %q",
			ErrExactFunctionDataUnavailable,
			function,
			featureType,
		)
	}
	return matches[0].ReadCmdType(nil, nil), nil
}

func functionDataAvailable(
	featureType model.FeatureTypeType,
	function model.FunctionType,
) (available bool) {
	defer func() {
		if recover() != nil {
			available = false
		}
	}()
	return len(functionDataMatches(featureType, function)) == 1
}

func functionDataMatches(
	featureType model.FeatureTypeType,
	function model.FunctionType,
) []spineapi.FunctionDataCmdInterface {
	var matches []spineapi.FunctionDataCmdInterface
	for _, functionData := range spine.CreateFunctionData[spineapi.FunctionDataCmdInterface](featureType) {
		if !isNil(functionData) && functionData.FunctionType() == function {
			matches = append(matches, functionData)
		}
	}
	return matches
}

func soleTypedFunction(command model.CmdType) (model.FunctionType, error) {
	value := reflect.ValueOf(command)
	valueType := value.Type()
	var functions []model.FunctionType
	for index := 0; index < value.NumField(); index++ {
		field := value.Field(index)
		if field.Kind() != reflect.Ptr || field.IsNil() {
			continue
		}
		structField := valueType.Field(index)
		if structField.Name == "Function" {
			continue
		}
		function, found := model.EEBusTags(structField)[model.EEBusTagFunction]
		if found && function != "" {
			functions = append(functions, model.FunctionType(function))
		}
	}
	if len(functions) != 1 {
		return "", ErrExactFunctionMismatch
	}
	return functions[0], nil
}

func validateResponse(
	request ExactFeatureRequest,
	correlatedRequest spineapi.CorrelatedRequest,
	response spineapi.CorrelatedResponse,
) error {
	malformed := func(message string) error {
		return &spineapi.CorrelatedProtocolError{
			Message: message,
			Cause:   ErrMalformedExactResponse,
		}
	}
	if response.CorrelationKey == 0 {
		return malformed("exact correlated response key is missing")
	}
	if response.Header.AddressSource == nil ||
		response.Header.AddressDestination == nil ||
		!reflect.DeepEqual(*response.Header.AddressSource, correlatedRequest.Destination) ||
		!reflect.DeepEqual(*response.Header.AddressDestination, correlatedRequest.Source) {
		return malformed("exact correlated response address mismatch")
	}
	if response.Header.MsgCounterReference == nil ||
		*response.Header.MsgCounterReference != response.CorrelationKey {
		return malformed("exact correlated response key mismatch")
	}
	if response.Header.CmdClassifier == nil {
		return malformed("exact correlated response classifier is missing")
	}

	switch request.Operation {
	case ExactFeatureOperationRead:
		if *response.Header.CmdClassifier != model.CmdClassifierTypeReply ||
			len(response.Cmd.Filter) != 0 ||
			response.Cmd.Function != nil {
			return malformed("exact READ response is not a full reply")
		}
		function, err := soleTypedFunction(response.Cmd)
		if err != nil || function != request.Target.Function {
			return malformed("exact READ response function mismatch")
		}
	case ExactFeatureOperationWrite:
		if *response.Header.CmdClassifier != model.CmdClassifierTypeResult ||
			response.Cmd.ResultData == nil ||
			response.Cmd.ResultData.ErrorNumber == nil ||
			len(response.Cmd.Filter) != 0 ||
			response.Cmd.Function != nil {
			return malformed("exact WRITE response is not a typed result")
		}
		function, err := soleTypedFunction(response.Cmd)
		if err != nil || function != model.FunctionTypeResultData {
			return malformed("exact WRITE response contains mixed typed data")
		}
		if *response.Cmd.ResultData.ErrorNumber != model.ErrorNumberTypeNoError {
			return &spineapi.CorrelatedRemoteError{
				ErrorNumber: *response.Cmd.ResultData.ErrorNumber,
				Description: response.Cmd.ResultData.Description,
			}
		}
	default:
		return malformed("exact response operation is unsupported")
	}
	return nil
}

func setTypedError(result *ExactFeatureResult, err error) {
	var remoteError *spineapi.CorrelatedRemoteError
	if errors.As(err, &remoteError) {
		result.RemoteError = remoteError
	}
	var protocolError *spineapi.CorrelatedProtocolError
	if errors.As(err, &protocolError) {
		result.ProtocolError = protocolError
	}
}

func validateFeatureAddress(address model.FeatureAddressType) error {
	switch {
	case address.Device == nil || *address.Device == "":
		return errors.New("device address is required")
	case address.Entity == nil:
		return errors.New("entity address is required")
	case address.Feature == nil:
		return errors.New("feature address is required")
	default:
		return nil
	}
}

func equalFeatureAddress(left, right *model.FeatureAddressType) bool {
	return left != nil && right != nil &&
		left.Device != nil && right.Device != nil &&
		*left.Device == *right.Device &&
		slices.Equal(left.Entity, right.Entity) &&
		left.Feature != nil && right.Feature != nil &&
		*left.Feature == *right.Feature
}

func cloneFeatureTarget(target ExactFeatureTarget) ExactFeatureTarget {
	target.Address = cloneFeatureAddress(target.Address)
	return target
}

func cloneFeatureAddress(address model.FeatureAddressType) model.FeatureAddressType {
	result := address
	if address.Device != nil {
		device := *address.Device
		result.Device = &device
	}
	result.Entity = append([]model.AddressEntityType(nil), address.Entity...)
	if address.Feature != nil {
		feature := *address.Feature
		result.Feature = &feature
	}
	return result
}

func cloneCorrelatedUnknownFields(
	fields []spineapi.CorrelatedUnknownField,
) []spineapi.CorrelatedUnknownField {
	if len(fields) == 0 {
		return nil
	}
	result := make([]spineapi.CorrelatedUnknownField, len(fields))
	for index, field := range fields {
		result[index] = spineapi.CorrelatedUnknownField{
			Path:  strings.Clone(field.Path),
			Value: spineapi.CorrelatedUnknownValue(field.Value.JSON()),
		}
	}
	return result
}

func isNil(value any) bool {
	if value == nil {
		return true
	}
	reflected := reflect.ValueOf(value)
	switch reflected.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Ptr, reflect.Slice:
		return reflected.IsNil()
	default:
		return false
	}
}
