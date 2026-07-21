package contracttests

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

func TestProductionAPIRemovesOutboundPairingForwarders(t *testing.T) {
	root := repositoryRoot(t)
	forbidden := map[string]struct{}{
		"OutboundPairingServiceInterface": {},
		"outboundPairingHub":              {},
		"QueueRemoteSKI":                  {},
		"ReportRemoteEndpoint":            {},
	}

	var violations []string
	for _, path := range trackedGoFiles(t, root) {
		if strings.HasSuffix(path, "_test.go") {
			continue
		}
		fset := token.NewFileSet()
		file, err := parser.ParseFile(fset, path, readFile(t, filepath.Join(root, path)), 0)
		if err != nil {
			t.Fatalf("parse production source %s: %v", path, err)
		}
		ast.Inspect(file, func(node ast.Node) bool {
			identifier, ok := node.(*ast.Ident)
			if !ok {
				return true
			}
			if _, found := forbidden[identifier.Name]; found {
				position := fset.Position(identifier.Pos())
				violations = append(violations, fmt.Sprintf("%s:%d:%d: %s", path, position.Line, position.Column, identifier.Name))
			}
			return true
		})
	}

	sort.Strings(violations)
	if len(violations) != 0 {
		t.Errorf("production API retains removed outbound pairing declarations or references:\n%s", strings.Join(violations, "\n"))
	}
}
