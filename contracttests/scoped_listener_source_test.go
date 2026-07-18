package contracttests

import (
	"bytes"
	"go/ast"
	"go/format"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const (
	scopedShipHubPath  = "github.com/Project-Helianthus/helianthus-ship-go/hub"
	scopedShipMDNSPath = "github.com/Project-Helianthus/helianthus-ship-go/mdns"
)

func TestScopedListenerPublicSourceContractIsOwnedByEEBusGo(t *testing.T) {
	files, fset := scopedServiceProduction(t)
	policy := scopedType(files, "ListenerPolicy")
	if policy == nil {
		t.Fatal("service.ListenerPolicy is missing")
	}
	if policy.Assign.IsValid() {
		t.Fatal("service.ListenerPolicy must be owned, not a dependency alias")
	}
	structure, ok := policy.Type.(*ast.StructType)
	if !ok {
		t.Fatalf("service.ListenerPolicy type = %T; want struct", policy.Type)
	}
	want := map[string]string{"ListenAddress": "netip.AddrPort", "DiscoveryEnabled": "bool"}
	got := scopedStructFields(t, fset, structure)
	if len(got) != len(want) {
		t.Errorf("service.ListenerPolicy fields = %v; want exactly %v", got, want)
	}
	for name, fieldType := range want {
		if got[name] != fieldType {
			t.Errorf("service.ListenerPolicy.%s type = %q; want %q", name, got[name], fieldType)
		}
	}
	if scopedType(files, "ServiceOptions") == nil {
		t.Error("service.ServiceOptions is missing")
	}
	if scopedFunction(files, "NewServiceWithOptions") == nil {
		t.Error("service.NewServiceWithOptions is missing")
	}
}

func TestScopedListenerSetupForwardsExactPolicyAfterMDNSConstruction(t *testing.T) {
	files, fset := scopedServiceProduction(t)
	var policyFunction *ast.FuncDecl
	var policyCall *ast.CallExpr
	legacyCalls := 0

	for _, file := range files {
		for _, declaration := range file.Decls {
			function, ok := declaration.(*ast.FuncDecl)
			if !ok || function.Body == nil {
				continue
			}
			ast.Inspect(function.Body, func(node ast.Node) bool {
				call, ok := node.(*ast.CallExpr)
				if !ok {
					return true
				}
				switch scopedCallName(call) {
				case "NewHub":
					if scopedImports(file, scopedShipHubPath) {
						legacyCalls++
					}
				case "NewHubWithListenerPolicy":
					if scopedImports(file, scopedShipHubPath) {
						if policyCall != nil {
							t.Fatal("multiple ship.NewHubWithListenerPolicy construction paths")
						}
						policyFunction, policyCall = function, call
					}
				}
				return true
			})
		}
	}
	if legacyCalls != 1 {
		t.Errorf("legacy ship.NewHub calls = %d; want exactly one preserved path", legacyCalls)
	}
	if policyCall == nil {
		t.Fatal("ship.NewHubWithListenerPolicy calls = 0; want exactly one additive path")
	}
	if len(policyCall.Args) != 6 {
		t.Fatalf("ship.NewHubWithListenerPolicy arguments = %d; want six", len(policyCall.Args))
	}
	literal, ok := policyCall.Args[5].(*ast.CompositeLit)
	if !ok || !strings.HasSuffix(scopedRender(t, fset, policyCall.Args[5]), "}") {
		t.Fatalf("listener policy argument = %s; want explicit ship API composite", scopedRender(t, fset, policyCall.Args[5]))
	}
	if !strings.HasSuffix(scopedRender(t, fset, literal.Type), ".ListenerPolicy") {
		t.Errorf("listener policy argument type = %s; want shipapi.ListenerPolicy", scopedRender(t, fset, literal.Type))
	}
	fields := make(map[string]ast.Expr)
	for _, element := range literal.Elts {
		keyed, ok := element.(*ast.KeyValueExpr)
		if !ok {
			t.Fatalf("listener policy contains an unkeyed field: %s", scopedRender(t, fset, element))
		}
		key, ok := keyed.Key.(*ast.Ident)
		if !ok {
			t.Fatalf("listener policy key = %s; want identifier", scopedRender(t, fset, keyed.Key))
		}
		fields[key.Name] = keyed.Value
	}
	if len(fields) != 2 {
		t.Errorf("ship listener policy fields = %v; want only exact address and discovery", fields)
	}
	for _, name := range []string{"ListenAddress", "DiscoveryEnabled"} {
		value, exists := fields[name]
		if !exists {
			t.Errorf("ship listener policy is missing %s", name)
			continue
		}
		selector, ok := value.(*ast.SelectorExpr)
		if !ok || selector.Sel.Name != name {
			t.Errorf("ship listener policy %s = %s; want direct eebus policy forwarding", name, scopedRender(t, fset, value))
		}
	}
	policySource := scopedRender(t, fset, literal)
	for _, forbidden := range []string{"AddrPortFrom", "MustParseAddr", "isPairingPossible"} {
		if strings.Contains(policySource, forbidden) {
			t.Errorf("ship listener policy reconstructs or couples %q: %s", forbidden, policySource)
		}
	}
	if !scopedFunctionAcceptsPolicy(t, fset, policyFunction) {
		t.Errorf("%s must accept eebus-go ListenerPolicy before converting to shipapi.ListenerPolicy", policyFunction.Name.Name)
	}

	setup := scopedServiceMethod(files, "Setup")
	if setup == nil {
		t.Fatal("(*Service).Setup is missing")
	}
	mdnsName, mdnsEnd := scopedMDNSAssignment(files, setup)
	if mdnsName == "" {
		t.Fatal("Setup must create the mDNS manager before scoped hub construction")
	}
	forwarded := false
	ast.Inspect(setup.Body, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok || call.Pos() <= mdnsEnd || !scopedCallHasIdentifier(call, mdnsName) {
			return true
		}
		name := strings.ToLower(scopedCallName(call))
		forwarded = forwarded || len(call.Args) >= 6 || strings.Contains(name, "policy") || strings.Contains(name, "scoped")
		return true
	})
	if !forwarded {
		t.Errorf("Setup creates %s but does not forward it with listener policy afterward", mdnsName)
	}
}

func scopedServiceProduction(t *testing.T) (map[string]*ast.File, *token.FileSet) {
	t.Helper()
	fset := token.NewFileSet()
	packages, err := parser.ParseDir(fset, filepath.Join(repositoryRoot(t), "service"), func(info os.FileInfo) bool {
		return strings.HasSuffix(info.Name(), ".go") && !strings.HasSuffix(info.Name(), "_test.go")
	}, 0)
	if err != nil {
		t.Fatalf("parse service production sources: %v", err)
	}
	if packages["service"] == nil {
		t.Fatal("service production package is missing")
	}
	return packages["service"].Files, fset
}

func scopedType(files map[string]*ast.File, name string) *ast.TypeSpec {
	for _, file := range files {
		for _, declaration := range file.Decls {
			generic, ok := declaration.(*ast.GenDecl)
			if !ok || generic.Tok != token.TYPE {
				continue
			}
			for _, specification := range generic.Specs {
				candidate, ok := specification.(*ast.TypeSpec)
				if ok && candidate.Name.Name == name {
					return candidate
				}
			}
		}
	}
	return nil
}

func scopedFunction(files map[string]*ast.File, name string) *ast.FuncDecl {
	for _, file := range files {
		for _, declaration := range file.Decls {
			function, ok := declaration.(*ast.FuncDecl)
			if ok && function.Recv == nil && function.Name.Name == name {
				return function
			}
		}
	}
	return nil
}

func scopedServiceMethod(files map[string]*ast.File, name string) *ast.FuncDecl {
	for _, file := range files {
		for _, declaration := range file.Decls {
			function, ok := declaration.(*ast.FuncDecl)
			if ok && function.Recv != nil && function.Name.Name == name {
				return function
			}
		}
	}
	return nil
}

func scopedStructFields(t *testing.T, fset *token.FileSet, structure *ast.StructType) map[string]string {
	t.Helper()
	fields := make(map[string]string)
	for _, field := range structure.Fields.List {
		for _, name := range field.Names {
			fields[name.Name] = scopedRender(t, fset, field.Type)
		}
	}
	return fields
}

func scopedImports(file *ast.File, path string) bool {
	for _, spec := range file.Imports {
		if strings.Trim(spec.Path.Value, `"`) == path {
			return true
		}
	}
	return false
}

func scopedCallName(call *ast.CallExpr) string {
	switch function := call.Fun.(type) {
	case *ast.Ident:
		return function.Name
	case *ast.SelectorExpr:
		return function.Sel.Name
	default:
		return ""
	}
}

func scopedFunctionAcceptsPolicy(t *testing.T, fset *token.FileSet, function *ast.FuncDecl) bool {
	t.Helper()
	if function.Type.Params == nil {
		return false
	}
	for _, parameter := range function.Type.Params.List {
		name := scopedRender(t, fset, parameter.Type)
		if name == "ListenerPolicy" || name == "*ListenerPolicy" {
			return true
		}
	}
	return false
}

func scopedMDNSAssignment(files map[string]*ast.File, setup *ast.FuncDecl) (string, token.Pos) {
	var setupFile *ast.File
	for _, file := range files {
		if file.Pos() <= setup.Pos() && setup.End() <= file.End() {
			setupFile = file
		}
	}
	if setupFile == nil || !scopedImports(setupFile, scopedShipMDNSPath) {
		return "", token.NoPos
	}
	var name string
	var end token.Pos
	ast.Inspect(setup.Body, func(node ast.Node) bool {
		assignment, ok := node.(*ast.AssignStmt)
		if !ok || len(assignment.Lhs) != 1 || len(assignment.Rhs) != 1 {
			return true
		}
		call, ok := assignment.Rhs[0].(*ast.CallExpr)
		identifier, named := assignment.Lhs[0].(*ast.Ident)
		if ok && named && scopedCallName(call) == "NewMDNS" {
			name, end = identifier.Name, call.End()
		}
		return true
	})
	return name, end
}

func scopedCallHasIdentifier(call *ast.CallExpr, want string) bool {
	for _, argument := range call.Args {
		if identifier, ok := argument.(*ast.Ident); ok && identifier.Name == want {
			return true
		}
	}
	return false
}

func scopedRender(t *testing.T, fset *token.FileSet, node any) string {
	t.Helper()
	var buffer bytes.Buffer
	if err := format.Node(&buffer, fset, node); err != nil {
		t.Fatalf("format AST node: %v", err)
	}
	return buffer.String()
}
