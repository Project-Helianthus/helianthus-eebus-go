package service

import (
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSetupOwnsOutgoingAttemptGateInstallation(t *testing.T) {
	files := parseProductionServiceFiles(t)
	setup := methodDeclaration(t, files, "Setup")
	start := methodDeclaration(t, files, "Start")

	setterCalls := selectorCalls(setup, "SetOutgoingAttemptGate")
	assert.Len(t, setterCalls, 1, "Setup must install the requested gate exactly once")
	assert.True(t, hasSelector(setup, "OutgoingAttemptGateSetter"), "Setup must require the optional hub setter")
	assert.Zero(t, len(selectorCalls(setup, "Start")), "Setup must not start runtime activity")
	assert.Zero(t, len(selectorCalls(start, "SetOutgoingAttemptGate")), "Start is too late to install the gate")
}

func TestSetupRoutesServiceReaderThroughInjectableProductionHubFactory(t *testing.T) {
	files := parseProductionServiceFiles(t)
	setup := methodDeclaration(t, files, "Setup")
	receiver := receiverName(t, setup)

	factoryCalls := selectorCalls(setup, "connectionsHubFactory")
	require.Len(t, factoryCalls, 1, "Setup must construct exactly one candidate hub through the injectable seam")
	require.NotEmpty(t, factoryCalls[0].Args)
	readerArgument, ok := factoryCalls[0].Args[0].(*ast.Ident)
	require.True(t, ok, "the factory reader argument must be the Service receiver")
	assert.Equal(t, receiver, readerArgument.Name)

	newHubCalls := 0
	forwardedReaderCalls := 0
	for _, file := range files {
		hubQualifier := importQualifier(file, "github.com/Project-Helianthus/helianthus-ship-go/hub")
		if hubQualifier == "" {
			continue
		}
		for _, declaration := range file.Decls {
			function, ok := declaration.(*ast.FuncDecl)
			if !ok || function.Body == nil {
				continue
			}
			parameters := functionParameterNames(function)
			ast.Inspect(function.Body, func(node ast.Node) bool {
				call, ok := node.(*ast.CallExpr)
				if !ok || !isQualifiedSelectorCall(call, hubQualifier, "NewHub") {
					return true
				}
				newHubCalls++
				if len(call.Args) == 0 {
					return true
				}
				reader, ok := call.Args[0].(*ast.Ident)
				if ok && parameters[reader.Name] {
					forwardedReaderCalls++
				}
				return true
			})
		}
	}

	assert.Equal(t, 1, newHubCalls, "production must have one ship.NewHub construction path")
	assert.Equal(t, 1, forwardedReaderCalls, "ship.NewHub must receive the factory reader parameter unchanged")
}

func parseProductionServiceFiles(t *testing.T) []*ast.File {
	t.Helper()
	paths, err := filepath.Glob("*.go")
	require.NoError(t, err)
	fileSet := token.NewFileSet()
	var files []*ast.File
	for _, path := range paths {
		if strings.HasSuffix(path, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(fileSet, path, nil, 0)
		require.NoError(t, err)
		files = append(files, file)
	}
	require.NotEmpty(t, files)
	return files
}

func methodDeclaration(t *testing.T, files []*ast.File, methodName string) *ast.FuncDecl {
	t.Helper()

	for _, file := range files {
		for _, declaration := range file.Decls {
			function, ok := declaration.(*ast.FuncDecl)
			if ok && function.Recv != nil && function.Name.Name == methodName {
				return function
			}
		}
	}
	t.Fatalf("method %s not found", methodName)
	return nil
}

func receiverName(t *testing.T, function *ast.FuncDecl) string {
	t.Helper()
	require.NotNil(t, function.Recv)
	require.Len(t, function.Recv.List, 1)
	require.Len(t, function.Recv.List[0].Names, 1)
	return function.Recv.List[0].Names[0].Name
}

func selectorCalls(function *ast.FuncDecl, selectorName string) []*ast.CallExpr {
	var calls []*ast.CallExpr
	ast.Inspect(function.Body, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		selector, ok := call.Fun.(*ast.SelectorExpr)
		if ok && selector.Sel.Name == selectorName {
			calls = append(calls, call)
		}
		return true
	})
	return calls
}

func hasSelector(function *ast.FuncDecl, selectorName string) bool {
	found := false
	ast.Inspect(function.Body, func(node ast.Node) bool {
		selector, ok := node.(*ast.SelectorExpr)
		if ok && selector.Sel.Name == selectorName {
			found = true
		}
		return !found
	})
	return found
}

func functionParameterNames(function *ast.FuncDecl) map[string]bool {
	result := make(map[string]bool)
	if function.Type.Params == nil {
		return result
	}
	for _, field := range function.Type.Params.List {
		for _, name := range field.Names {
			result[name.Name] = true
		}
	}
	return result
}

func isQualifiedSelectorCall(call *ast.CallExpr, qualifier string, selectorName string) bool {
	selector, ok := call.Fun.(*ast.SelectorExpr)
	if !ok || selector.Sel.Name != selectorName {
		return false
	}
	identifier, ok := selector.X.(*ast.Ident)
	return ok && identifier.Name == qualifier
}

func importQualifier(file *ast.File, importPath string) string {
	for _, specification := range file.Imports {
		path, err := strconv.Unquote(specification.Path.Value)
		if err != nil || path != importPath {
			continue
		}
		if specification.Name != nil {
			return specification.Name.Name
		}
		return filepath.Base(path)
	}
	return ""
}
