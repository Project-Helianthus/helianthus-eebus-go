package contracttests

import (
	"encoding/json"
	"go/parser"
	"go/token"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
)

const (
	scopedListenerShipModule         = "github.com/Project-Helianthus/helianthus-ship-go"
	scopedListenerShipVersion        = "v0.6.1-helianthus.7"
	scopedListenerShipRepository     = "https://github.com/Project-Helianthus/helianthus-ship-go.git"
	scopedListenerShipTagObject      = "abacdc171ffaac9b2d73eedbfe2bab6418c9028a"
	scopedListenerShipCommit         = "9d9175e538c15e3fbbc1abd6f5f27c188c6c86e2"
	scopedListenerShipTree           = "3dbd8cf3dce5759b48ec3b9b42e74969a0034525"
	scopedListenerShipManifestSHA256 = "54f91f18ab094825f68db61cad0423b4fadf2720179a09d2168d7cd988a43097"
	scopedListenerShipLicenseSHA256  = "c853996135802c50b3048937e48022bc00b41ff5f56a31cebe7d686bf91f87db"
)

func TestScopedListenerDependencyClosureRequiresReviewedShipRelease(t *testing.T) {
	root := repositoryRoot(t)
	goMod := string(readFile(t, filepath.Join(root, "go.mod")))
	direct := regexp.MustCompile(
		`(?m)^\s*(?:require\s+)?` + regexp.QuoteMeta(scopedListenerShipModule) +
			`\s+` + regexp.QuoteMeta(scopedListenerShipVersion) + `\s*$`,
	)
	if !direct.MatchString(goMod) {
		t.Errorf("go.mod must directly require reviewed SHIP release %s %s", scopedListenerShipModule, scopedListenerShipVersion)
	}
	if regexp.MustCompile(`(?m)^\s*replace(?:\s|\()`).MatchString(goMod) {
		t.Error("go.mod must not contain replace directives")
	}

	for _, upstream := range scopedListenerForbiddenModules() {
		if strings.Contains(goMod, upstream) {
			t.Errorf("go.mod contains forbidden upstream module identity %s", upstream)
		}
	}

	command := exec.Command("go", "list", "-m", "-mod=readonly", "all")
	command.Dir = root
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("resolve read-only module graph: %v\n%s", err, output)
	}
	graph := string(output)
	wantLine := scopedListenerShipModule + " " + scopedListenerShipVersion
	if !containsExactLine(graph, wantLine) {
		t.Errorf("resolved module graph lacks %q; SHIP entries: %v", wantLine, moduleLines(graph, scopedListenerShipModule))
	}
	for _, upstream := range scopedListenerForbiddenModules() {
		if len(moduleLines(graph, upstream)) != 0 {
			t.Errorf("resolved module graph contains forbidden upstream identity %s: %v", upstream, moduleLines(graph, upstream))
		}
	}
}

func TestScopedListenerProvenanceBindsReviewedShipTag(t *testing.T) {
	manifestPath := filepath.Join(repositoryRoot(t), "provenance", "closure-manifest.json")
	var manifest struct {
		ReviewedDependencies []struct {
			Module     string `json:"module"`
			Version    string `json:"version"`
			Repository string `json:"repository"`
			Ref        string `json:"ref"`
			TagObject  string `json:"tag_object_sha"`
			Commit     string `json:"peeled_commit_sha"`
			Tree       string `json:"tree_sha"`
			License    struct {
				Path   string `json:"path"`
				SHA256 string `json:"sha256"`
			} `json:"license"`
			ProvenanceManifest struct {
				Path   string `json:"path"`
				SHA256 string `json:"sha256"`
			} `json:"provenance_manifest"`
		} `json:"reviewed_dependencies"`
	}
	if err := json.Unmarshal(readFile(t, manifestPath), &manifest); err != nil {
		t.Fatalf("parse provenance manifest: %v", err)
	}

	var matches []int
	for index := range manifest.ReviewedDependencies {
		if manifest.ReviewedDependencies[index].Module == scopedListenerShipModule {
			matches = append(matches, index)
		}
	}
	if len(matches) != 1 {
		t.Fatalf("reviewed SHIP dependency entries = %d; want exactly one", len(matches))
	}
	dependency := manifest.ReviewedDependencies[matches[0]]
	wants := []struct {
		name string
		got  string
		want string
	}{
		{name: "module", got: dependency.Module, want: scopedListenerShipModule},
		{name: "version", got: dependency.Version, want: scopedListenerShipVersion},
		{name: "repository", got: dependency.Repository, want: scopedListenerShipRepository},
		{name: "ref", got: dependency.Ref, want: "refs/tags/" + scopedListenerShipVersion},
		{name: "tag_object_sha", got: dependency.TagObject, want: scopedListenerShipTagObject},
		{name: "peeled_commit_sha", got: dependency.Commit, want: scopedListenerShipCommit},
		{name: "tree_sha", got: dependency.Tree, want: scopedListenerShipTree},
		{name: "license.path", got: dependency.License.Path, want: "LICENSE"},
		{name: "license.sha256", got: dependency.License.SHA256, want: scopedListenerShipLicenseSHA256},
		{name: "provenance_manifest.path", got: dependency.ProvenanceManifest.Path, want: "provenance/closure-manifest.json"},
		{name: "provenance_manifest.sha256", got: dependency.ProvenanceManifest.SHA256, want: scopedListenerShipManifestSHA256},
	}
	for _, check := range wants {
		if check.got != check.want {
			t.Errorf("reviewed SHIP %s = %q; want %q", check.name, check.got, check.want)
		}
	}
}

func TestScopedListenerTrackedImportsRejectUpstreamModuleIdentities(t *testing.T) {
	root := repositoryRoot(t)
	for _, path := range trackedGoSourceFiles(t, root) {
		source := readFile(t, filepath.Join(root, path))
		file, err := parser.ParseFile(token.NewFileSet(), path, source, parser.ImportsOnly)
		if err != nil {
			t.Fatalf("parse imports in %s: %v", path, err)
		}
		for _, spec := range file.Imports {
			importPath, err := strconv.Unquote(spec.Path.Value)
			if err != nil {
				t.Fatalf("decode import in %s: %v", path, err)
			}
			for _, upstream := range scopedListenerForbiddenModules() {
				if hasImportPrefix(importPath, upstream) {
					t.Errorf("%s imports forbidden upstream module identity %s", path, importPath)
				}
			}
		}
	}
}

func scopedListenerForbiddenModules() []string {
	return []string{
		"github.com/enbility/eebus-go",
		"github.com/enbility/ship-go",
		"github.com/enbility/spine-go",
	}
}

func containsExactLine(text, want string) bool {
	for _, line := range strings.Split(strings.TrimSpace(text), "\n") {
		if strings.TrimSpace(line) == want {
			return true
		}
	}
	return false
}

func moduleLines(graph, module string) []string {
	var found []string
	for _, line := range strings.Split(strings.TrimSpace(graph), "\n") {
		fields := strings.Fields(line)
		if len(fields) > 0 && fields[0] == module {
			found = append(found, strings.TrimSpace(line))
		}
	}
	return found
}
