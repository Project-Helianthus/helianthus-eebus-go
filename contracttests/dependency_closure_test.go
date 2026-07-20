package contracttests

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"go/format"
	"go/parser"
	"go/token"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"testing"
)

const (
	canonicalModule   = "github.com/Project-Helianthus/helianthus-eebus-go"
	canonicalShip     = "github.com/Project-Helianthus/helianthus-ship-go"
	canonicalSpine    = "github.com/Project-Helianthus/helianthus-spine-go"
	canonicalShipVer  = "v0.6.1-helianthus.3"
	canonicalSpineVer = "v0.7.1-helianthus.1"
	canonicalVer      = canonicalShipVer
	upstreamSpine     = "github.com/enbility/spine-go"
	upstreamShip      = "github.com/enbility/ship-go"
	upstreamEEBus     = "github.com/enbility/eebus-go"
	productionHash    = "825e679815696d8d130856b0735b3a9ab46a2ff3dbbc3a770a96c3991b93165a"
)

func TestModuleDependencyClosure(t *testing.T) {
	root := repositoryRoot(t)
	goMod := string(readFile(t, filepath.Join(root, "go.mod")))
	moduleLine := regexp.MustCompile(`(?m)^module\s+(\S+)\s*$`).FindStringSubmatch(goMod)
	if len(moduleLine) != 2 || moduleLine[1] != canonicalModule {
		t.Errorf("module directive = %q; want %q", moduleLine, canonicalModule)
	}
	for _, dependency := range []struct{ module, version string }{
		{canonicalShip, canonicalShipVer},
		{canonicalSpine, canonicalSpineVer},
	} {
		direct := regexp.MustCompile(`(?m)^\s*(?:require\s+)?` + regexp.QuoteMeta(dependency.module) + `\s+` + regexp.QuoteMeta(dependency.version) + `\s*$`)
		if !direct.MatchString(goMod) {
			t.Errorf("go.mod lacks direct reviewed dependency %s %s", dependency.module, dependency.version)
		}
	}
	for _, upstream := range []string{upstreamEEBus, upstreamShip, upstreamSpine} {
		if strings.Contains(goMod, upstream) {
			t.Errorf("go.mod still contains forbidden upstream dependency %s", upstream)
		}
	}
	if regexp.MustCompile(`(?m)^\s*replace(?:\s|\()`).MatchString(goMod) {
		t.Error("go.mod contains a replace directive; canonical closure must not use replace")
	}

	cmd := exec.Command("go", "list", "-m", "-mod=readonly", "all")
	cmd.Dir = root
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("inspect module graph: %v", err)
	}
	graph := string(out)
	for _, upstream := range []string{upstreamEEBus, upstreamShip, upstreamSpine} {
		if strings.Contains(graph, upstream+" ") || strings.Contains(graph, upstream+"\n") {
			t.Errorf("module graph still contains forbidden %s", upstream)
		}
	}
	for _, dependency := range []struct{ module, version string }{
		{canonicalShip, canonicalShipVer},
		{canonicalSpine, canonicalSpineVer},
	} {
		if !strings.Contains(graph, dependency.module+" "+dependency.version+"\n") {
			t.Errorf("module graph does not contain reviewed dependency %s %s", dependency.module, dependency.version)
		}
	}
}
func TestTrackedGoImportsUseCanonicalIdentity(t *testing.T) {
	root := repositoryRoot(t)
	var violations []string
	canonicalCounts := map[string]int{canonicalModule: 0, canonicalShip: 0, canonicalSpine: 0}
	for _, path := range trackedGoSourceFiles(t, root) {
		src := readFile(t, filepath.Join(root, path))
		f, err := parser.ParseFile(token.NewFileSet(), path, src, parser.ImportsOnly)
		if err != nil {
			t.Fatalf("parse imports in %s: %v", path, err)
		}
		for _, spec := range f.Imports {
			importPath, err := strconv.Unquote(spec.Path.Value)
			if err != nil {
				t.Fatalf("decode import in %s: %v", path, err)
			}
			switch {
			case hasImportPrefix(importPath, upstreamEEBus):
				violations = append(violations, fmt.Sprintf("%s: %s -> %s", path, importPath, canonicalModule+strings.TrimPrefix(importPath, upstreamEEBus)))
			case hasImportPrefix(importPath, upstreamShip):
				violations = append(violations, fmt.Sprintf("%s: %s -> %s", path, importPath, canonicalShip+strings.TrimPrefix(importPath, upstreamShip)))
			case hasImportPrefix(importPath, upstreamSpine):
				violations = append(violations, fmt.Sprintf("%s: %s -> %s", path, importPath, canonicalSpine+strings.TrimPrefix(importPath, upstreamSpine)))
			case hasImportPrefix(importPath, canonicalModule):
				canonicalCounts[canonicalModule]++
			case hasImportPrefix(importPath, canonicalShip):
				canonicalCounts[canonicalShip]++
			case hasImportPrefix(importPath, canonicalSpine):
				canonicalCounts[canonicalSpine]++
			}
		}
	}
	if len(violations) != 0 {
		shown := violations
		if len(shown) > 12 {
			shown = shown[:12]
		}
		t.Errorf("found %d non-canonical tracked Go imports (first %d):\n%s", len(violations), len(shown), strings.Join(shown, "\n"))
	}
	for _, module := range []string{canonicalModule, canonicalShip, canonicalSpine} {
		if canonicalCounts[module] == 0 {
			t.Errorf("no tracked Go import uses required canonical prefix %s", module)
		}
	}
}
func TestProvenanceManifestBindsUpstream(t *testing.T) {
	path := filepath.Join(repositoryRoot(t), "provenance", "closure-manifest.json")
	var manifest struct {
		Schema string `json:"schema"`
		Module string `json:"module"`
		Fork   struct {
			Origin             string `json:"origin"`
			Lifecycle          string `json:"lifecycle"`
			IntendedPrerelease string `json:"intended_prerelease"`
		} `json:"fork"`
		Upstream struct {
			Remote    string `json:"remote"`
			Ref       string `json:"ref"`
			Tag       string `json:"tag"`
			TagObject string `json:"tag_object_sha"`
			Commit    string `json:"peeled_commit_sha"`
			Tree      string `json:"tree_sha"`
		} `json:"upstream"`
		License struct {
			Path   string `json:"path"`
			SHA256 string `json:"sha256"`
		} `json:"license"`
		NoticeInventory []string `json:"notice_inventory"`
		SourceHeaders   struct {
			Globs   []string `json:"globs"`
			Headers []string `json:"headers"`
		} `json:"source_header_inventory"`
		ReviewedDependencies []struct {
			Module     string `json:"module"`
			Version    string `json:"version"`
			Repository string `json:"repository"`
			Ref        string `json:"ref"`
			Tag        string `json:"tag_object_sha"`
			Commit     string `json:"peeled_commit_sha"`
			Tree       string `json:"tree_sha"`
			License    struct {
				Path   string `json:"path"`
				SHA256 string `json:"sha256"`
			} `json:"license"`
			Manifest struct {
				Path   string `json:"path"`
				SHA256 string `json:"sha256"`
			} `json:"provenance_manifest"`
		} `json:"reviewed_dependencies"`
		DependencyControlInputs []string `json:"dependency_control_inputs"`
	}
	data := readFile(t, path)
	if err := json.Unmarshal(data, &manifest); err != nil {
		t.Fatalf("parse %s: %v", path, err)
	}
	wants := []struct{ name, got, want string }{
		{"schema", manifest.Schema, "helianthus.provenance.closure-manifest.v2"},
		{"module", manifest.Module, canonicalModule},
		{"fork.origin", manifest.Fork.Origin, "https://github.com/Project-Helianthus/helianthus-eebus-go.git"},
		{"fork.lifecycle", manifest.Fork.Lifecycle, "temporary_downstream_patch_carrier"},
		{"fork.intended_prerelease", manifest.Fork.IntendedPrerelease, "v0.7.1-helianthus.2"},
		{"upstream.remote", manifest.Upstream.Remote, "https://github.com/enbility/eebus-go.git"},
		{"upstream.ref", manifest.Upstream.Ref, "refs/tags/v0.7.0"},
		{"upstream.tag", manifest.Upstream.Tag, "v0.7.0"},
		{"upstream.tag_object_sha", manifest.Upstream.TagObject, "e4677eb9c46f1cc46c2559027c35fbf39766bcfb"},
		{"upstream.peeled_commit_sha", manifest.Upstream.Commit, "99f07ff79819b728dd2fe37472c4a26865d8076c"},
		{"upstream.tree_sha", manifest.Upstream.Tree, "fee9de0ecb34dcb7c4165922fd49fedd42d8df23"},
		{"license.path", manifest.License.Path, "LICENSE"},
		{"license.sha256", manifest.License.SHA256, "0871acb60d194272cd91ad02dcaf0102d8047a993f1b00973da4c9c2cba845a4"},
	}
	for _, check := range wants {
		if check.got != check.want {
			t.Errorf("manifest %s = %q; want %q", check.name, check.got, check.want)
		}
	}
	if len(manifest.NoticeInventory) != 0 || len(manifest.SourceHeaders.Headers) != 0 || !strings.EqualFold(strings.Join(manifest.SourceHeaders.Globs, ","), "**/*.go") {
		t.Errorf("manifest notice/source-header inventories are not explicitly closed: notices=%v source_headers=%v", manifest.NoticeInventory, manifest.SourceHeaders)
	}
	if len(manifest.ReviewedDependencies) != 2 {
		t.Fatalf("manifest reviewed_dependencies = %d; want exactly reviewed SHIP and SPINE", len(manifest.ReviewedDependencies))
	}
	for _, dependency := range []struct {
		name, module, version, tag, commit, tree, manifestDigest string
	}{
		{"ship", canonicalShip, canonicalShipVer, "8c3365c1de1c5dbe40064efad908c9b3d937bd08", "3b7d2e7156632a42244a3aab61330b2fd081dce7", "6905923dfa808976c93794d4f5f74a37e0dc13eb", "54f91f18ab094825f68db61cad0423b4fadf2720179a09d2168d7cd988a43097"},
		{"spine", canonicalSpine, canonicalSpineVer, "2722d31718aa89b1d31faf16b5c14bbee692e2de", "c85a449cc44c7e1fd2a44f8b10724d81e89bb260", "02236304d8c74914a701be3896eaaf94be32e2d6", "39cf9ffc85ce1466d73f8d911a9046640ec4335876b460460c94781749017a4e"},
	} {
		var reviewed *struct {
			Module     string `json:"module"`
			Version    string `json:"version"`
			Repository string `json:"repository"`
			Ref        string `json:"ref"`
			Tag        string `json:"tag_object_sha"`
			Commit     string `json:"peeled_commit_sha"`
			Tree       string `json:"tree_sha"`
			License    struct {
				Path   string `json:"path"`
				SHA256 string `json:"sha256"`
			} `json:"license"`
			Manifest struct {
				Path   string `json:"path"`
				SHA256 string `json:"sha256"`
			} `json:"provenance_manifest"`
		}
		for i := range manifest.ReviewedDependencies {
			if manifest.ReviewedDependencies[i].Module == dependency.module {
				reviewed = &manifest.ReviewedDependencies[i]
			}
		}
		if reviewed == nil {
			t.Errorf("manifest lacks reviewed dependency %s", dependency.module)
			continue
		}
		dependencyWants := []struct{ name, got, want string }{
			{"module", reviewed.Module, dependency.module}, {"version", reviewed.Version, dependency.version},
			{"repository", reviewed.Repository, "https://github.com/Project-Helianthus/helianthus-" + dependency.name + "-go.git"},
			{"ref", reviewed.Ref, "refs/tags/" + dependency.version},
			{"tag_object_sha", reviewed.Tag, dependency.tag}, {"peeled_commit_sha", reviewed.Commit, dependency.commit},
			{"tree_sha", reviewed.Tree, dependency.tree}, {"license.path", reviewed.License.Path, "LICENSE"},
			{"license.sha256", reviewed.License.SHA256, "c853996135802c50b3048937e48022bc00b41ff5f56a31cebe7d686bf91f87db"},
			{"provenance_manifest.path", reviewed.Manifest.Path, "provenance/closure-manifest.json"},
			{"provenance_manifest.sha256", reviewed.Manifest.SHA256, dependency.manifestDigest},
		}
		for _, check := range dependencyWants {
			if check.got != check.want {
				t.Errorf("manifest %s.%s = %q; want %q", dependency.name, check.name, check.got, check.want)
			}
		}
	}
	wantInputs := []string{".github/workflows/default.yml", "go.mod", "go.sum"}
	if strings.Join(manifest.DependencyControlInputs, "\n") != strings.Join(wantInputs, "\n") {
		t.Errorf("manifest dependency_control_inputs = %v; want closed inventory %v", manifest.DependencyControlInputs, wantInputs)
	}
}
func TestCommittedClosureVerifierIsExecutable(t *testing.T) {
	path := filepath.Join(repositoryRoot(t), "scripts", "verify_dependency_closure.py")
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("required committed closure verifier %s: %v", path, err)
	}
	if !info.Mode().IsRegular() {
		t.Errorf("closure verifier is not a regular file: mode %s", info.Mode())
	}
	if info.Mode().Perm()&0o111 == 0 {
		t.Errorf("closure verifier is not executable: mode %s", info.Mode())
	}
}
func TestWorkflowSupportsReleaseBranchAndSARIF(t *testing.T) {
	path := filepath.Join(repositoryRoot(t), ".github", "workflows", "default.yml")
	workflow := string(readFile(t, path))
	branch, topLevelPermissions := workflowContract(workflow)
	if !branch {
		t.Error("workflow push branches do not include helianthus-v0.7")
	}
	if len(topLevelPermissions) != 0 {
		t.Errorf("top-level workflow permissions = %v; permissions must be isolated per job", topLevelPermissions)
	}
	build := workflowJobSection(t, workflow, "build")
	security := workflowJobSection(t, workflow, "security")
	assertExactPermissions(t, "build", workflowJobPermissions(build), map[string]string{"contents": "read"})
	assertExactPermissions(t, "security", workflowJobPermissions(security), map[string]string{"contents": "read", "security-events": "write"})
	if strings.Contains(build, "security-events:") || strings.Contains(build, "Upload SARIF file") {
		t.Error("build job must not receive or use SARIF write authority")
	}
	if !strings.Contains(security, "Upload SARIF file") || strings.Contains(security, "coverallsapp/github-action@") {
		t.Error("security job must exclusively own SARIF upload and must not run Coveralls")
	}
	if strings.Count(workflow, "persist-credentials: false") != 2 {
		t.Error("every checkout must disable persisted credentials")
	}
	required := []string{
		"scripts/verify_dependency_closure.py",
		"gofmt -l",
		"GOWORK: \"off\"",
		"GOTOOLCHAIN: local",
		"GOFLAGS: -mod=readonly",
		"go list -m all",
		"go mod graph",
		"go list -deps ./...",
		"resolved-graph-closure.json",
		"coverage.out",
		"name: coverage",
		"path: coverage.out",
		"if-no-files-found: error",
		"git rev-parse HEAD",
		"go version",
		"actions/upload-artifact",
	}
	for _, fragment := range required {
		if !strings.Contains(workflow, fragment) {
			t.Errorf("workflow lacks required closure fragment %q", fragment)
		}
	}
	if strings.Index(workflow, "scripts/verify_dependency_closure.py") > strings.Index(workflow, "go list -m all") {
		t.Error("workflow runs resolved graph commands before tracked closure verifier")
	}
	mutableRef := "@" + "m" + "aster"
	if strings.Contains(workflow, "--issues-exit-code=0") || strings.Contains(workflow, "-no-fail") || strings.Contains(workflow, "version: latest") || strings.Contains(workflow, mutableRef) {
		t.Error("workflow retains a lint bypass or mutable golangci selection")
	}
	coverageStart := strings.Index(workflow, "- name: Send coverage")
	if coverageStart < 0 {
		t.Error("workflow lacks external coverage reporting step")
	} else if coverage := workflow[coverageStart:]; !strings.Contains(coverage, "coverallsapp/github-action@") || !strings.Contains(coverage, "continue-on-error: true") {
		t.Error("external Coveralls reporting must remain immutable-pinned and explicitly non-blocking")
	}
	if !strings.Contains(workflow, "- name: Retain coverage artifact") {
		t.Error("workflow must retain local coverage as an authoritative artifact before external reporting")
	}
	for _, fragment := range []string{"id: gosec", "continue-on-error: true", "if: always()", "steps.gosec.outcome", "Enforce Gosec outcome"} {
		if !strings.Contains(security, fragment) {
			t.Errorf("security job does not preserve SARIF and enforce the original gosec outcome: missing %q", fragment)
		}
	}
	if strings.Count(build, "continue-on-error: true") != 1 || !strings.Contains(build, "- name: Send coverage") {
		t.Error("Coveralls must be the build job's only advisory step")
	}
	uses := regexp.MustCompile(`(?m)^\s*uses:\s+[^\s]+@([0-9a-f]{40})\s+#\s+v\S+\s*$`).FindAllStringSubmatch(workflow, -1)
	if len(uses) != 10 {
		t.Errorf("workflow immutable action pins = %d; want 10 full commit pins with tag comments", len(uses))
	}
}
func TestProductionSourcesMatchUpstreamApartFromImportIdentity(t *testing.T) {
	root := repositoryRoot(t)
	h := sha256.New()
	for _, path := range trackedGoFiles(t, root) {
		if strings.HasSuffix(path, "_test.go") || strings.HasPrefix(path, "contracttests/") {
			continue
		}
		src := normalizeCanonicalImports(t, path, readFile(t, filepath.Join(root, path)))
		src, err := format.Source(src)
		if err != nil {
			t.Fatalf("format normalized production source %s: %v", path, err)
		}
		fmt.Fprintf(h, "%s\x00", path)
		h.Write(src)
		h.Write([]byte{0})
	}
	got := hex.EncodeToString(h.Sum(nil))
	if got != productionHash {
		t.Errorf("normalized production source digest = %s; want bound eebus v0.7 closure baseline %s; only canonical import identity changes are allowed", got, productionHash)
	}
}
func repositoryRoot(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("locate contract test source")
	}
	return filepath.Dir(filepath.Dir(file))
}

func readFile(t *testing.T, path string) []byte {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return data
}

func trackedGoFiles(t *testing.T, root string) []string {
	t.Helper()
	cmd := exec.Command("git", "ls-files", "-z", "--", "*.go")
	cmd.Dir = root
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("list tracked Go files: %v", err)
	}
	paths := strings.Split(strings.TrimSuffix(string(out), "\x00"), "\x00")
	sort.Strings(paths)
	return paths
}

func trackedGoSourceFiles(t *testing.T, root string) []string {
	t.Helper()
	cmd := exec.Command("git", "ls-files", "-z")
	cmd.Dir = root
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("list tracked sources: %v", err)
	}
	var paths []string
	for _, path := range strings.Split(strings.TrimSuffix(string(out), "\x00"), "\x00") {
		if regexp.MustCompile(`\.go(?:$|[._-])`).MatchString(filepath.Base(path)) {
			paths = append(paths, path)
		}
	}
	sort.Strings(paths)
	return paths
}

func hasImportPrefix(path, prefix string) bool {
	return path == prefix || strings.HasPrefix(path, prefix+"/")
}

func workflowContract(data string) (bool, map[string]string) {
	permissions := make(map[string]string)
	section := ""
	inPush := false
	inBranches := false
	branch := false
	for _, raw := range strings.Split(data, "\n") {
		trimmed := strings.TrimSpace(strings.SplitN(raw, "#", 2)[0])
		if trimmed == "" {
			continue
		}
		indent := len(raw) - len(strings.TrimLeft(raw, " "))
		if indent == 0 {
			section = strings.Trim(strings.TrimSuffix(trimmed, ":"), "\"'")
			inPush, inBranches = false, false
			continue
		}
		if section == "on" {
			if indent == 2 {
				inPush = strings.Trim(trimmed, "\"'") == "push:"
				inBranches = false
			} else if inPush && indent == 4 {
				inBranches = strings.Trim(trimmed, "\"'") == "branches:"
			} else if inBranches && indent >= 6 && strings.HasPrefix(trimmed, "-") {
				branch = branch || strings.Trim(strings.TrimSpace(strings.TrimPrefix(trimmed, "-")), "\"'") == "helianthus-v0.7"
			}
		}
		if section == "permissions" && indent == 2 {
			parts := strings.SplitN(trimmed, ":", 2)
			if len(parts) == 2 {
				permissions[strings.TrimSpace(parts[0])] = strings.Trim(strings.TrimSpace(parts[1]), "\"'")
			}
		}
	}
	return branch, permissions
}

func workflowJobSection(t *testing.T, data, name string) string {
	t.Helper()
	lines := strings.Split(data, "\n")
	start := -1
	for index, line := range lines {
		if line == "  "+name+":" {
			start = index
			break
		}
	}
	if start < 0 {
		t.Fatalf("workflow job %s is absent", name)
	}
	end := len(lines)
	for index := start + 1; index < len(lines); index++ {
		line := lines[index]
		if strings.HasPrefix(line, "  ") && !strings.HasPrefix(line, "    ") && strings.HasSuffix(strings.TrimSpace(line), ":") {
			end = index
			break
		}
	}
	return strings.Join(lines[start:end], "\n")
}

func workflowJobPermissions(section string) map[string]string {
	permissions := make(map[string]string)
	inPermissions := false
	for _, raw := range strings.Split(section, "\n") {
		trimmed := strings.TrimSpace(strings.SplitN(raw, "#", 2)[0])
		indent := len(raw) - len(strings.TrimLeft(raw, " "))
		if indent == 4 {
			inPermissions = trimmed == "permissions:"
			continue
		}
		if !inPermissions || indent != 6 {
			continue
		}
		parts := strings.SplitN(trimmed, ":", 2)
		if len(parts) == 2 {
			permissions[parts[0]] = strings.Trim(strings.TrimSpace(parts[1]), "\"'")
		}
	}
	return permissions
}

func assertExactPermissions(t *testing.T, job string, got, want map[string]string) {
	t.Helper()
	if len(got) != len(want) {
		t.Errorf("%s job permissions = %v; want only %v", job, got, want)
		return
	}
	for name, value := range want {
		if got[name] != value {
			t.Errorf("%s job permission %s = %q; want %q", job, name, got[name], value)
		}
	}
}

func normalizeCanonicalImports(t *testing.T, path string, src []byte) []byte {
	t.Helper()
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, path, src, parser.ImportsOnly)
	if err != nil {
		t.Fatalf("parse production imports in %s: %v", path, err)
	}
	type replacement struct {
		start, end int
		value      string
	}
	var replacements []replacement
	for _, spec := range f.Imports {
		start := fset.Position(spec.Path.Pos()).Offset
		end := fset.Position(spec.Path.End()).Offset
		importPath, err := strconv.Unquote(string(src[start:end]))
		if err != nil {
			t.Fatalf("decode production import in %s: %v", path, err)
		}
		normalized := importPath
		if hasImportPrefix(importPath, canonicalModule) {
			normalized = upstreamEEBus + strings.TrimPrefix(importPath, canonicalModule)
		} else if hasImportPrefix(importPath, canonicalShip) {
			normalized = upstreamShip + strings.TrimPrefix(importPath, canonicalShip)
		} else if hasImportPrefix(importPath, canonicalSpine) {
			normalized = upstreamSpine + strings.TrimPrefix(importPath, canonicalSpine)
		}
		if normalized != importPath {
			replacements = append(replacements, replacement{start, end, strconv.Quote(normalized)})
		}
	}
	out := append([]byte(nil), src...)
	for i := len(replacements) - 1; i >= 0; i-- {
		r := replacements[i]
		out = append(append(append([]byte(nil), out[:r.start]...), r.value...), out[r.end:]...)
	}
	return out
}
