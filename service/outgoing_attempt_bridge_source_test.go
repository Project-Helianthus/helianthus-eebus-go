package service

import (
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOutgoingAttemptBridgeKeepsLegacyInterfacesUnchanged(t *testing.T) {
	fileSet := token.NewFileSet()
	file, err := parser.ParseFile(fileSet, filepath.Join("..", "api", "api.go"), nil, 0)
	require.NoError(t, err)

	assert.Equal(t, []string{
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
		"Setup",
		"Shutdown",
		"Start",
		"UnregisterRemoteSKI",
		"UserIsAbleToApproveOrCancelPairingRequests",
	}, interfaceMethodNames(t, file, "ServiceInterface"))
	assert.Equal(t, []string{
		"RemoteSKIConnected",
		"RemoteSKIDisconnected",
		"ServicePairingDetailUpdate",
		"ServiceShipIDUpdate",
		"VisibleRemoteServicesUpdated",
	}, interfaceMethodNames(t, file, "ServiceReaderInterface"))
}

func TestSetupOwnsOutgoingAttemptGateInstallation(t *testing.T) {
	fileSet := token.NewFileSet()
	file, err := parser.ParseFile(fileSet, "service.go", nil, 0)
	require.NoError(t, err)
	setup := methodDeclaration(t, file, "Setup")
	start := methodDeclaration(t, file, "Start")

	setterCalls := selectorCallCount(setup, "SetOutgoingAttemptGate")
	assert.Equal(t, 1, setterCalls, "Setup must install the requested gate exactly once")
	assert.True(t, hasSelector(setup, "OutgoingAttemptGateSetter"), "Setup must require the optional hub setter")
	assert.Zero(t, selectorCallCount(setup, "Start"), "Setup must not start runtime activity")
	assert.Zero(t, selectorCallCount(start, "SetOutgoingAttemptGate"), "Start is too late to install the gate")
}

func interfaceMethodNames(t *testing.T, file *ast.File, interfaceName string) []string {
	t.Helper()

	for _, declaration := range file.Decls {
		general, ok := declaration.(*ast.GenDecl)
		if !ok || general.Tok != token.TYPE {
			continue
		}
		for _, specification := range general.Specs {
			typeSpecification, ok := specification.(*ast.TypeSpec)
			if !ok || typeSpecification.Name.Name != interfaceName {
				continue
			}
			interfaceType, ok := typeSpecification.Type.(*ast.InterfaceType)
			require.True(t, ok, "%s must remain an interface", interfaceName)
			var names []string
			for _, method := range interfaceType.Methods.List {
				for _, name := range method.Names {
					names = append(names, name.Name)
				}
			}
			sort.Strings(names)
			return names
		}
	}

	t.Fatalf("interface %s not found", interfaceName)
	return nil
}

func methodDeclaration(t *testing.T, file *ast.File, methodName string) *ast.FuncDecl {
	t.Helper()

	for _, declaration := range file.Decls {
		function, ok := declaration.(*ast.FuncDecl)
		if ok && function.Recv != nil && function.Name.Name == methodName {
			return function
		}
	}
	t.Fatalf("method %s not found", methodName)
	return nil
}

func selectorCallCount(function *ast.FuncDecl, selectorName string) int {
	count := 0
	ast.Inspect(function.Body, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		selector, ok := call.Fun.(*ast.SelectorExpr)
		if ok && selector.Sel.Name == selectorName {
			count++
		}
		return true
	})
	return count
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
