package contracttests

import (
	"encoding/json"
	"fmt"
	"go/ast"
	"go/importer"
	"go/parser"
	"go/token"
	"go/types"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

const shipPackagePrefix = "github.com/Project-Helianthus/helianthus-ship-go"

var apiPackageObjectAllowlist = map[string]string{
	"Configuration":                          "type:struct",
	"DeviceClassificationClientInterface":    "type:interface",
	"DeviceClassificationCommonInterface":    "type:interface",
	"DeviceClassificationServerInterface":    "type:interface",
	"DeviceConfigurationClientInterface":     "type:interface",
	"DeviceConfigurationCommonInterface":     "type:interface",
	"DeviceConfigurationServerInterface":     "type:interface",
	"DeviceDiagnosisClientInterface":         "type:interface",
	"DeviceDiagnosisCommonInterface":         "type:interface",
	"DeviceDiagnosisServerInterface":         "type:interface",
	"ElectricalConnectionClientInterface":    "type:interface",
	"ElectricalConnectionCommonInterface":    "type:interface",
	"ElectricalConnectionServerInterface":    "type:interface",
	"EntityEventCallback":                    "type:signature",
	"ErrDataForMetadataKeyNotFound":          "var",
	"ErrDataNotAvailable":                    "var",
	"ErrDeviceDisconnected":                  "var",
	"ErrEntityNotFound":                      "var",
	"ErrFunctionNotSupported":                "var",
	"ErrMetadataNotAvailable":                "var",
	"ErrMissingData":                         "var",
	"ErrNoCompatibleEntity":                  "var",
	"ErrNotSupported":                        "var",
	"ErrOperationOnFunctionNotSupported":     "var",
	"ErrUsecCaseNotSupported":                "var",
	"EventType":                              "type:basic",
	"FeatureClientInterface":                 "type:interface",
	"FeatureServerInterface":                 "type:interface",
	"IdentificationClientInterface":          "type:interface",
	"IdentificationCommonInterface":          "type:interface",
	"IdentificationServerInterface":          "type:interface",
	"IncentiveTableClientInterface":          "type:interface",
	"IncentiveTableCommonInterface":          "type:interface",
	"IncentiveTableServerInterface":          "type:interface",
	"LoadControlClientInterface":             "type:interface",
	"LoadControlCommonInterface":             "type:interface",
	"LoadControlServerInterface":             "type:interface",
	"ManufacturerData":                       "type:struct",
	"MeasurementClientInterface":             "type:interface",
	"MeasurementCommonInterface":             "type:interface",
	"MeasurementServerInterface":             "type:interface",
	"NewConfiguration":                       "func",
	"PairingCandidateReader":                 "type:interface",
	"PairingCandidateQueuer":                 "type:interface",
	"RemoteEntityScenarios":                  "type:struct",
	"ServiceInterface":                       "type:interface",
	"ServiceReaderInterface":                 "type:interface",
	"SmartEnergyManagementPsClientInterface": "type:interface",
	"SmartEnergyManagementPsCommonInterface": "type:interface",
	"SmartEnergyManagementPsServerInterface": "type:interface",
	"TimeSeriesClientInterface":              "type:interface",
	"TimeSeriesCommonInterface":              "type:interface",
	"TimeSeriesServerInterface":              "type:interface",
	"UseCaseBaseInterface":                   "type:interface",
	"UseCaseInterface":                       "type:interface",
	"UseCaseScenario":                        "type:struct",
}

var servicePackageObjectAllowlist = map[string]string{
	"ListenerPolicy":                      "type:struct",
	"NewService":                          "func",
	"NewServiceWithOptions":               "func",
	"NewServiceWithOutgoingAttemptBridge": "func",
	"OutgoingAttemptBridgeConfiguration":  "type:struct",
	"Service":                             "type:struct",
	"ServiceOptions":                      "type:struct",
}

var fixtureServicePackageObjectAllowlist = map[string]string{"Service": "type:struct"}

var apiInterfaceMethodAllowlists = map[string]map[string]struct{}{
	"PairingCandidateQueuer": stringSet(
		"QueuePairingCandidate",
	),
	"PairingCandidateReader": stringSet(
		"VisiblePairingCandidatesUpdated",
	),
	"ServiceInterface": stringSet(
		"AddUseCase",
		"CancelPairingWithSKI",
		"Configuration",
		"DisconnectSKI",
		"IsAutoAcceptEnabled",
		"LocalDevice",
		"LocalService",
		"PairingDetailForSki",
		"RegisterRemoteSKI",
		"RemoteServiceForSKI",
		"SetAutoAccept",
		"SetLogging",
		"SetPairingRegistration",
		"Setup",
		"Shutdown",
		"Start",
		"UnregisterRemoteSKI",
	),
	"ServiceReaderInterface": stringSet(
		"RemoteSKIConnected",
		"RemoteSKIDisconnected",
		"ServicePairingDetailUpdate",
		"ServiceShipIDUpdate",
		"VisibleRemoteServicesUpdated",
	),
}

var serviceMethodAllowlist = stringSet(
	"AddUseCase",
	"AllowWaitingForTrust",
	"CancelPairingWithSKI",
	"Configuration",
	"DisconnectSKI",
	"IsAutoAcceptEnabled",
	"LocalDevice",
	"LocalService",
	"OutgoingAttemptConnectionClosed",
	"OutgoingAttemptHandshakeStateUpdate",
	"PairingDetailForSki",
	"QueuePairingCandidate",
	"RegisterRemoteSKI",
	"RemoteSKIConnected",
	"RemoteSKIDisconnected",
	"RemoteServiceForSKI",
	"ServicePairingDetailUpdate",
	"ServiceShipIDUpdate",
	"SetAutoAccept",
	"SetLogging",
	"SetPairingRegistration",
	"Setup",
	"SetupRemoteDevice",
	"Shutdown",
	"Start",
	"StartWithPolicy",
	"UnregisterRemoteSKI",
	"VisibleRemoteServicesUpdated",
	"VisiblePairingCandidatesUpdated",
)

var serviceCapabilityTypeAllowlist = stringSet(
	"listenerPolicyHub",
	"pairingCandidateHub",
	"pairingRegistrationHub",
)

type typedPackage struct {
	pkg   *types.Package
	info  *types.Info
	files []*ast.File
}

func TestOutboundPairingRemovalUsesTypedAPISurface(t *testing.T) {
	// Behavioral tests cannot exhaustively prove that forbidden public symbols remain absent.
	loaded := loadTypedRepositoryPackages(t)
	var violations []string
	violations = append(violations, apiSurfaceViolations(loaded[canonicalModule+"/api"])...)
	violations = append(violations, serviceSurfaceViolations(
		loaded[canonicalModule+"/service"],
		servicePackageObjectAllowlist,
		serviceMethodAllowlist,
		serviceCapabilityTypeAllowlist,
	)...)
	if len(violations) != 0 {
		sort.Strings(violations)
		t.Fatalf("typed outbound pairing API guard failed:\n%s", strings.Join(violations, "\n"))
	}
}

func TestPairingCandidateReaderEndpointRedactedFieldSetIsSupplyChainFrozen(t *testing.T) {
	view := loadTypedRepositoryPackages(t)[canonicalModule+"/api"]
	reader, ok := view.pkg.Scope().Lookup("PairingCandidateReader").(*types.TypeName)
	if !ok {
		t.Fatal("missing api PairingCandidateReader")
	}

	method := types.NewMethodSet(reader.Type()).Lookup(view.pkg, "VisiblePairingCandidatesUpdated")
	if method == nil {
		t.Fatal("missing PairingCandidateReader.VisiblePairingCandidatesUpdated")
	}
	signature, ok := method.Obj().Type().(*types.Signature)
	if !ok || signature.Params().Len() != 2 || signature.Results().Len() != 0 {
		t.Fatalf("PairingCandidateReader callback signature = %s, want two inputs and no outputs", method.Obj().Type())
	}
	if !types.Identical(signature.Params().At(0).Type(), view.pkg.Scope().Lookup("ServiceInterface").Type()) {
		t.Fatalf("PairingCandidateReader first input = %s, want api.ServiceInterface", signature.Params().At(0).Type())
	}

	candidates, ok := types.Unalias(signature.Params().At(1).Type()).(*types.Slice)
	if !ok {
		t.Fatalf("PairingCandidateReader second input = %s, want []shipapi.PairingCandidateRef", signature.Params().At(1).Type())
	}
	candidateRef, ok := types.Unalias(candidates.Elem()).(*types.Named)
	if !ok || candidateRef.Obj().Pkg() == nil || candidateRef.Obj().Pkg().Path() != shipPackagePrefix+"/api" || candidateRef.Obj().Name() != "PairingCandidateRef" {
		t.Fatalf("PairingCandidateReader element = %s, want shipapi.PairingCandidateRef", candidates.Elem())
	}

	fields, ok := candidateRef.Underlying().(*types.Struct)
	if !ok {
		t.Fatalf("PairingCandidateRef underlying type = %T, want struct", candidateRef.Underlying())
	}
	actual := make(map[string]struct{}, fields.NumFields())
	for index := 0; index < fields.NumFields(); index++ {
		field := fields.Field(index)
		if field.Type() != types.Typ[types.String] {
			t.Fatalf("PairingCandidateRef field %s type = %s, want string", field.Name(), field.Type())
		}
		actual[field.Name()] = struct{}{}
	}
	// This exact allowlist is a supply-chain anti-leak gate. Identity fields are
	// untrusted discovery claims; forbidding additions prevents endpoint, path,
	// address, or port material from silently entering this dependency contract.
	if violations := compareStringSets("PairingCandidateRef field", actual, stringSet(
		"CandidateRef", "Name", "SKI", "Identifier", "Brand", "Type", "Model",
	)); len(violations) != 0 {
		t.Fatalf("PairingCandidateRef field freeze failed:\n%s", strings.Join(violations, "\n"))
	}
}

func TestTypedAPIGuardRejectsAdversarialMutations(t *testing.T) {
	shipAPI := checkFixturePackage(t, shipPackagePrefix+"/api", `
package api

type MdnsEntry struct{}

type HubInterface interface {
	RegisterRemoteSKI(ski string)
}

type DiscoveryController interface {
	ReportMdnsEntry(entry MdnsEntry)
}
`, nil)
	imports := fixtureImporter{shipAPI.pkg.Path(): shipAPI.pkg}

	t.Run("renamed exported pairing wrapper", func(t *testing.T) {
		fixture := checkFixturePackage(t, canonicalModule+"/service", `
package service

import shipapi "github.com/Project-Helianthus/helianthus-ship-go/api"

type Service struct {
	hub shipapi.HubInterface
}

func (s *Service) Setup() {}

func (s *Service) PairCandidate(remote string) {
	s.hub.RegisterRemoteSKI(remote)
}
`, imports)
		violations := serviceSurfaceViolations(fixture, fixtureServicePackageObjectAllowlist, stringSet("Setup"), nil)
		assertViolationContains(t, violations, "unexpected Service method PairCandidate")
		assertViolationContains(t, violations, "forwards to SHIP pairing/discovery method RegisterRemoteSKI")
	})

	t.Run("renamed package-level pairing forwarder", func(t *testing.T) {
		fixture := checkFixturePackage(t, canonicalModule+"/service", `
package service

import shipapi "github.com/Project-Helianthus/helianthus-ship-go/api"

type Service struct{}

func (s *Service) Setup() {}

func PairCandidate(hub shipapi.HubInterface, remote string) {
	hub.RegisterRemoteSKI(remote)
}
`, imports)
		violations := serviceSurfaceViolations(fixture, fixtureServicePackageObjectAllowlist, stringSet("Setup"), nil)
		assertViolationContains(t, violations, "unexpected exported service package object PairCandidate (func)")
	})

	t.Run("local capability alias and forwarder", func(t *testing.T) {
		fixture := checkFixturePackage(t, canonicalModule+"/service", `
package service

import shipapi "github.com/Project-Helianthus/helianthus-ship-go/api"

type localDiscovery = shipapi.DiscoveryController

type Service struct {
	discovery localDiscovery
}

func (s *Service) Setup() {}

func (s *Service) relayDiscovery(entry shipapi.MdnsEntry) {
	s.discovery.ReportMdnsEntry(entry)
}
`, imports)
		violations := serviceSurfaceViolations(fixture, fixtureServicePackageObjectAllowlist, stringSet("Setup"), nil)
		assertViolationContains(t, violations, "local capability interface localDiscovery")
		assertViolationContains(t, violations, "unexpected Service method relayDiscovery")
		assertViolationContains(t, violations, "forwards to SHIP pairing/discovery method ReportMdnsEntry")
	})
}

func loadTypedRepositoryPackages(t *testing.T) map[string]*typedPackage {
	t.Helper()
	root := repositoryRoot(t)
	fset := token.NewFileSet()
	result := make(map[string]*typedPackage)
	loadedPackages := make(map[string]*types.Package)
	fallback := importer.ForCompiler(fset, "gc", packageExportLookup(root))
	imports := &repositoryImporter{loaded: loadedPackages, fallback: fallback}
	for _, packagePath := range []string{canonicalModule + "/api", canonicalModule + "/service"} {
		view := checkRepositoryPackage(t, root, packagePath, fset, imports)
		result[packagePath] = view
		loadedPackages[packagePath] = view.pkg
	}
	return result
}

type goListPackage struct {
	Dir        string
	ImportPath string
	GoFiles    []string
	CgoFiles   []string
}

func checkRepositoryPackage(
	t *testing.T,
	root string,
	packagePath string,
	fset *token.FileSet,
	imports types.Importer,
) *typedPackage {
	t.Helper()
	command := exec.Command("go", "list", "-json", packagePath)
	command.Dir = root
	command.Env = append(os.Environ(), "GOFLAGS=-mod=readonly", "GOWORK=off")
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("list package %s: %v\n%s", packagePath, err, output)
	}
	var metadata goListPackage
	if err := json.Unmarshal(output, &metadata); err != nil {
		t.Fatalf("decode go list metadata for %s: %v", packagePath, err)
	}
	if metadata.ImportPath != packagePath {
		t.Fatalf("go list import path = %s; want %s", metadata.ImportPath, packagePath)
	}

	filenames := append(append([]string(nil), metadata.GoFiles...), metadata.CgoFiles...)
	sort.Strings(filenames)
	files := make([]*ast.File, 0, len(filenames))
	for _, filename := range filenames {
		path := filepath.Join(metadata.Dir, filename)
		file, err := parser.ParseFile(fset, path, nil, 0)
		if err != nil {
			t.Fatalf("parse package source %s: %v", path, err)
		}
		files = append(files, file)
	}
	info := &types.Info{
		Types:      make(map[ast.Expr]types.TypeAndValue),
		Defs:       make(map[*ast.Ident]types.Object),
		Uses:       make(map[*ast.Ident]types.Object),
		Selections: make(map[*ast.SelectorExpr]*types.Selection),
	}
	config := &types.Config{Importer: imports}
	pkg, err := config.Check(packagePath, fset, files, info)
	if err != nil {
		t.Fatalf("type-check repository package %s: %v", packagePath, err)
	}
	return &typedPackage{pkg: pkg, info: info, files: files}
}

type repositoryImporter struct {
	loaded   map[string]*types.Package
	fallback types.Importer
}

func (imports *repositoryImporter) Import(path string) (*types.Package, error) {
	if pkg := imports.loaded[path]; pkg != nil {
		return pkg, nil
	}
	return imports.fallback.Import(path)
}

func packageExportLookup(root string) importer.Lookup {
	return func(path string) (io.ReadCloser, error) {
		command := exec.Command("go", "list", "-export", "-f", "{{.Export}}", path)
		command.Dir = root
		command.Env = append(os.Environ(), "GOFLAGS=-mod=readonly", "GOWORK=off")
		output, err := command.CombinedOutput()
		if err != nil {
			return nil, fmt.Errorf("locate export data for %s: %w: %s", path, err, strings.TrimSpace(string(output)))
		}
		exportPath := strings.TrimSpace(string(output))
		if exportPath == "" {
			return nil, fmt.Errorf("go list returned no export data for %s", path)
		}
		return os.Open(exportPath)
	}
}

func apiSurfaceViolations(view *typedPackage) []string {
	violations := compareStringMaps(
		"exported api package object",
		exportedPackageObjects(view.pkg),
		apiPackageObjectAllowlist,
	)
	for typeName, allowlist := range apiInterfaceMethodAllowlists {
		object, ok := view.pkg.Scope().Lookup(typeName).(*types.TypeName)
		if !ok {
			violations = append(violations, fmt.Sprintf("missing api interface %s", typeName))
			continue
		}
		violations = append(violations, compareStringSets(
			"api interface "+typeName+" method",
			methodNames(object.Type()),
			allowlist,
		)...)
	}
	return violations
}

func serviceSurfaceViolations(
	view *typedPackage,
	packageAllowlist map[string]string,
	methodAllowlist, capabilityAllowlist map[string]struct{},
) []string {
	violations := compareStringMaps(
		"exported service package object",
		exportedPackageObjects(view.pkg),
		packageAllowlist,
	)
	serviceObject, ok := view.pkg.Scope().Lookup("Service").(*types.TypeName)
	if !ok {
		return append(violations, "missing service.Service type")
	}

	actualMethods := methodNames(types.NewPointer(types.Unalias(serviceObject.Type()).(*types.Named)))
	violations = append(violations, compareStringSets("Service method", actualMethods, methodAllowlist)...)

	for _, name := range view.pkg.Scope().Names() {
		object, ok := view.pkg.Scope().Lookup(name).(*types.TypeName)
		if !ok || name == "Service" {
			continue
		}
		methods := sensitiveCapabilityMethods(object.Type())
		if len(methods) == 0 {
			continue
		}
		if _, allowed := capabilityAllowlist[name]; allowed {
			continue
		}
		violations = append(violations, fmt.Sprintf(
			"local capability interface %s exposes sensitive pairing/discovery methods: %s",
			name,
			strings.Join(methods, ", "),
		))
	}

	for _, declaration := range serviceMethodDeclarations(view) {
		if _, allowed := methodAllowlist[declaration.object.Name()]; allowed {
			continue
		}
		forwarded := forwardedShipCapabilityMethods(view, declaration)
		for _, method := range forwarded {
			violations = append(violations, fmt.Sprintf(
				"unexpected Service method %s accepts sensitive input and forwards to SHIP pairing/discovery method %s",
				declaration.object.Name(),
				method,
			))
		}
	}
	return violations
}

func exportedPackageObjects(pkg *types.Package) map[string]string {
	result := make(map[string]string)
	for _, name := range pkg.Scope().Names() {
		object := pkg.Scope().Lookup(name)
		if !object.Exported() {
			continue
		}
		switch typed := object.(type) {
		case *types.TypeName:
			result[name] = "type:" + typeKind(typed)
		case *types.Func:
			result[name] = "func"
		case *types.Var:
			result[name] = "var"
		case *types.Const:
			result[name] = "const"
		default:
			result[name] = fmt.Sprintf("%T", object)
		}
	}
	return result
}

type serviceMethodDeclaration struct {
	object *types.Func
	syntax *ast.FuncDecl
}

func serviceMethodDeclarations(view *typedPackage) []serviceMethodDeclaration {
	var declarations []serviceMethodDeclaration
	for _, file := range view.files {
		for _, declaration := range file.Decls {
			function, ok := declaration.(*ast.FuncDecl)
			if !ok || function.Recv == nil || function.Body == nil {
				continue
			}
			object, ok := view.info.Defs[function.Name].(*types.Func)
			if !ok || !isServiceMethod(object) {
				continue
			}
			declarations = append(declarations, serviceMethodDeclaration{object: object, syntax: function})
		}
	}
	return declarations
}

func isServiceMethod(function *types.Func) bool {
	signature, ok := function.Type().(*types.Signature)
	if !ok || signature.Recv() == nil {
		return false
	}
	receiver := signature.Recv().Type()
	if pointer, ok := receiver.(*types.Pointer); ok {
		receiver = pointer.Elem()
	}
	named, ok := types.Unalias(receiver).(*types.Named)
	return ok && named.Obj().Name() == "Service"
}

func forwardedShipCapabilityMethods(view *typedPackage, declaration serviceMethodDeclaration) []string {
	signature := declaration.object.Type().(*types.Signature)
	parameters := make(map[types.Object]struct{}, signature.Params().Len())
	for index := 0; index < signature.Params().Len(); index++ {
		parameters[signature.Params().At(index)] = struct{}{}
	}

	var forwarded []string
	ast.Inspect(declaration.syntax.Body, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		selector, ok := call.Fun.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		selection := view.info.Selections[selector]
		if selection == nil {
			return true
		}
		target, ok := selection.Obj().(*types.Func)
		if !ok || !isShipPairingDiscoveryMethod(target) {
			return true
		}
		if !signatureAcceptsSensitiveInput(signature) && !argumentsUseParameters(view.info, call.Args, parameters) {
			return true
		}
		forwarded = append(forwarded, target.Name())
		return true
	})
	sort.Strings(forwarded)
	return compactStrings(forwarded)
}

func argumentsUseParameters(info *types.Info, arguments []ast.Expr, parameters map[types.Object]struct{}) bool {
	for _, argument := range arguments {
		used := false
		ast.Inspect(argument, func(node ast.Node) bool {
			identifier, ok := node.(*ast.Ident)
			if !ok {
				return true
			}
			if _, found := parameters[info.Uses[identifier]]; found {
				used = true
				return false
			}
			return true
		})
		if used {
			return true
		}
	}
	return false
}

func sensitiveCapabilityMethods(value types.Type) []string {
	methodSet := types.NewMethodSet(types.Unalias(value))
	var found []string
	for index := 0; index < methodSet.Len(); index++ {
		method, ok := methodSet.At(index).Obj().(*types.Func)
		if !ok {
			continue
		}
		signature, ok := method.Type().(*types.Signature)
		if !ok {
			continue
		}
		if signatureAcceptsSensitiveInput(signature) || isShipPairingDiscoveryMethod(method) {
			found = append(found, method.Name())
		}
	}
	sort.Strings(found)
	return compactStrings(found)
}

func signatureAcceptsSensitiveInput(signature *types.Signature) bool {
	for index := 0; index < signature.Params().Len(); index++ {
		parameter := signature.Params().At(index)
		name := strings.ToLower(parameter.Name())
		if strings.Contains(name, "ski") || strings.Contains(name, "endpoint") || strings.Contains(name, "mdns") {
			return true
		}
		if sensitiveType(parameter.Type()) {
			return true
		}
	}
	return false
}

func sensitiveType(value types.Type) bool {
	value = types.Unalias(value)
	switch typed := value.(type) {
	case *types.Named:
		name := strings.ToLower(typed.Obj().Name())
		return strings.Contains(name, "remoteendpoint") || strings.Contains(name, "mdnsentry") || strings.Contains(name, "remoteservice")
	case *types.Pointer:
		return sensitiveType(typed.Elem())
	case *types.Slice:
		return sensitiveType(typed.Elem())
	case *types.Array:
		return sensitiveType(typed.Elem())
	case *types.Map:
		return sensitiveType(typed.Key()) || sensitiveType(typed.Elem())
	default:
		return false
	}
}

func isShipPairingDiscoveryMethod(function *types.Func) bool {
	if function.Pkg() == nil || !strings.HasPrefix(function.Pkg().Path(), shipPackagePrefix) {
		return false
	}
	name := strings.ToLower(function.Name())
	for _, marker := range []string{"ski", "pair", "mdns", "discover", "announce", "remoteendpoint"} {
		if strings.Contains(name, marker) {
			return true
		}
	}
	return false
}

func methodNames(value types.Type) map[string]struct{} {
	methodSet := types.NewMethodSet(types.Unalias(value))
	result := make(map[string]struct{}, methodSet.Len())
	for index := 0; index < methodSet.Len(); index++ {
		result[methodSet.At(index).Obj().Name()] = struct{}{}
	}
	return result
}

func typeKind(object *types.TypeName) string {
	if object.IsAlias() {
		return "alias"
	}
	switch object.Type().Underlying().(type) {
	case *types.Interface:
		return "interface"
	case *types.Struct:
		return "struct"
	case *types.Signature:
		return "signature"
	case *types.Basic:
		return "basic"
	default:
		return fmt.Sprintf("%T", object.Type().Underlying())
	}
}

func compareStringMaps(label string, actual, expected map[string]string) []string {
	var violations []string
	for name, kind := range actual {
		expectedKind, ok := expected[name]
		switch {
		case !ok:
			violations = append(violations, fmt.Sprintf("unexpected %s %s (%s)", label, name, kind))
		case kind != expectedKind:
			violations = append(violations, fmt.Sprintf("%s %s kind = %s; want %s", label, name, kind, expectedKind))
		}
	}
	for name := range expected {
		if _, ok := actual[name]; !ok {
			violations = append(violations, fmt.Sprintf("missing %s %s", label, name))
		}
	}
	return violations
}

func compareStringSets(label string, actual, expected map[string]struct{}) []string {
	var violations []string
	for name := range actual {
		if _, ok := expected[name]; !ok {
			violations = append(violations, fmt.Sprintf("unexpected %s %s", label, name))
		}
	}
	for name := range expected {
		if _, ok := actual[name]; !ok {
			violations = append(violations, fmt.Sprintf("missing %s %s", label, name))
		}
	}
	return violations
}

func stringSet(values ...string) map[string]struct{} {
	result := make(map[string]struct{}, len(values))
	for _, value := range values {
		result[value] = struct{}{}
	}
	return result
}

func compactStrings(values []string) []string {
	if len(values) < 2 {
		return values
	}
	compacted := values[:1]
	for _, value := range values[1:] {
		if value != compacted[len(compacted)-1] {
			compacted = append(compacted, value)
		}
	}
	return compacted
}

type fixtureImporter map[string]*types.Package

func (imports fixtureImporter) Import(path string) (*types.Package, error) {
	if pkg := imports[path]; pkg != nil {
		return pkg, nil
	}
	return nil, fmt.Errorf("fixture import %s is unavailable", path)
}

func checkFixturePackage(t *testing.T, packagePath, source string, imports types.Importer) *typedPackage {
	t.Helper()
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "fixture.go", source, 0)
	if err != nil {
		t.Fatalf("parse fixture %s: %v", packagePath, err)
	}
	info := &types.Info{
		Types:      make(map[ast.Expr]types.TypeAndValue),
		Defs:       make(map[*ast.Ident]types.Object),
		Uses:       make(map[*ast.Ident]types.Object),
		Selections: make(map[*ast.SelectorExpr]*types.Selection),
	}
	config := &types.Config{Importer: imports}
	pkg, err := config.Check(packagePath, fset, []*ast.File{file}, info)
	if err != nil {
		t.Fatalf("type-check fixture %s: %v", packagePath, err)
	}
	return &typedPackage{pkg: pkg, info: info, files: []*ast.File{file}}
}

func assertViolationContains(t *testing.T, violations []string, fragment string) {
	t.Helper()
	for _, violation := range violations {
		if strings.Contains(violation, fragment) {
			return
		}
	}
	t.Fatalf("violations do not contain %q:\n%s", fragment, strings.Join(violations, "\n"))
}
