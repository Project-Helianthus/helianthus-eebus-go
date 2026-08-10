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

func TestScopedListenerPublicSourceContractIsOwnedByEEBusGo(t *testing.T) {
	// Runtime reflection cannot prove that this public type is owned here rather than aliased from a dependency.
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

func scopedRender(t *testing.T, fset *token.FileSet, node any) string {
	t.Helper()
	var buffer bytes.Buffer
	if err := format.Node(&buffer, fset, node); err != nil {
		t.Fatalf("format AST node: %v", err)
	}
	return buffer.String()
}
