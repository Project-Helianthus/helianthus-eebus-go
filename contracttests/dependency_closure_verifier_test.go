package contracttests

import (
	"archive/zip"
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
)

const (
	verifierCLI     = "python3 scripts/verify_dependency_closure.py --repo . --manifest provenance/closure-manifest.json --inventory-output <path> --evidence-output <path>"
	canonicalEEBus  = canonicalModule
	reviewedSpine   = "v0.7.1-helianthus.1"
	reviewedEEBus   = "v0.7.0-helianthus.1"
	privateSentinel = "PRIVATE-CONTENT-must-not-leak-8f2c6f71"
)

type closureFixtureFile struct {
	data       string
	executable bool
	symlink    string
}

type closureResult struct {
	err                 error
	stdout, stderr      []byte
	inventory, evidence []byte
}

type closureCase struct {
	name          string
	edit          func(map[string]closureFixtureFile)
	wantPass      bool
	wantPath      string
	wantClass     string
	wantReason    string
	secret        string
	deterministic bool
	supportEdit   func(*testing.T, string)
}

type fixtureIdentity struct {
	module, version, repository, ref string
	licenseDigest, manifestDigest    string
	tag, commit, tree                string
}

type closureProvenanceFixture struct {
	upstream     fixtureIdentity
	dependencies []fixtureIdentity
}

func TestDependencyClosureVerifierFixtures(t *testing.T) {
	verifier := filepath.Join(repositoryRoot(t), "scripts", "verify_dependency_closure.py")
	if _, err := os.Stat(verifier); err != nil {
		t.Fatalf("dependency closure verifier is absent: %v; expected executable CLI: %s", err, verifierCLI)
	}

	cases := []closureCase{
		{name: "valid canonical fixture", wantPass: true, deterministic: true},
		{
			name: "unrelated third party pseudo version", wantPass: true,
			edit: replaceFixtureText("go.mod", "v0.0.0-20260716000000-0123456789ab", "v1.2.4-0.20260716000000-abcdefabcdef"),
		},
		{
			name: "canonical to canonical replace", wantPath: "go.mod", wantClass: "go_module", wantReason: "replace_directive",
			edit: appendFixtureText("go.mod", "\nreplace "+canonicalShip+" "+canonicalVer+" => "+canonicalShip+" "+canonicalVer+"\n"),
		},
		{
			name: "local filesystem replace", wantPath: "go.mod", wantClass: "go_module", wantReason: "replace_directive",
			edit: appendFixtureText("go.mod", "\nreplace "+canonicalShip+" => ./third_party/ship-go\n"),
		},
		{
			name: "workspace replace despite GOWORK off", wantPath: "go.work", wantClass: "workspace", wantReason: "replace_directive",
			edit: setFixtureText("go.work", "go 1.22.0\n\nreplace "+canonicalShip+" => "+canonicalShip+" "+canonicalVer+"\n"),
		},
		{
			name: "committed workspace use current module", wantPath: "go.work", wantClass: "workspace", wantReason: "workspace_local_selection",
			edit: setFixtureText("go.work", "go 1.22.0\n\nuse .\n"),
		},
		{
			name: "committed workspace use local module", wantPath: "go.work", wantClass: "workspace", wantReason: "workspace_local_selection",
			edit: setFixtureText("go.work", "go 1.22.0\n\nuse ./local/ship-go\n"),
		},
		{
			name: "unclassified tracked dependency control", wantPath: "dependency-control.lockx", wantClass: "unclassified", wantReason: "unclassified_dependency_control",
			edit: setFixtureText("dependency-control.lockx", "module="+canonicalShip+"\nversion="+canonicalVer+"\n"),
		},
		{
			name: "private contents stay private", wantPath: "release/release.json", wantClass: "release_config", wantReason: "upstream_module_identity", secret: privateSentinel,
			edit: setFixtureText("release/release.json", `{"private":"`+privateSentinel+`","module":"`+upstreamShip+`","release_inputs":["release/nested.json"]}`+"\n"),
		},
	}

	hidden := []struct{ name, path, class, data string }{
		{"go mod", "go.mod", "go_module", "\nrequire " + upstreamShip + " v0.6.0\n"},
		{"go work", "go.work", "workspace", "go 1.22.0\n\nreplace example.com/ship => " + upstreamShip + " v0.6.0\n"},
		{"go work sum", "go.work.sum", "workspace_checksum", upstreamShip + " v0.6.0/go.mod h1:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=\n"},
		{"vendor manifest", "vendor/modules.txt", "vendor_manifest", "# " + upstreamShip + " v0.6.0\n"},
		{"workflow", ".github/workflows/release.yml", "workflow", "\n# " + upstreamShip + "\n"},
		{"local action", ".github/actions/release/action.yml", "local_action", "\n# " + upstreamShip + "\n"},
		{"release script", "scripts/release.sh", "build_release_control", "\n# " + upstreamShip + "\n"},
		{"release config", "release/release.json", "release_config", `{"module":"` + upstreamShip + `","release_inputs":["release/nested.json"]}` + "\n"},
		{"recursive release input", "release/nested.json", "release_config", `{"module":"` + upstreamShip + `"}` + "\n"},
		{"upstream eebus", "build/dependencies.json", "build_release_control", `{"module":"` + upstreamEEBus + `"}` + "\n"},
	}
	for _, item := range hidden {
		edit := setFixtureText(item.path, item.data)
		if baseClosureContains(item.path) && item.path != "release/release.json" && item.path != "release/nested.json" {
			edit = appendFixtureText(item.path, item.data)
		}
		cases = append(cases, closureCase{
			name: "upstream identity in " + item.name, edit: edit,
			wantPath: item.path, wantClass: item.class, wantReason: "upstream_module_identity",
		})
	}

	forks := []struct{ name, module, reviewed string }{
		{"ship", canonicalShip, canonicalVer},
		{"spine", canonicalSpine, reviewedSpine},
		{"eebus", canonicalEEBus, reviewedEEBus},
	}
	for _, fork := range forks {
		badVersions := []struct{ name, version string }{
			{"pseudo version", strings.TrimSuffix(fork.reviewed, "-helianthus.1") + "-0.20260716000000-0123456789ab"},
			{"branch selection", "helianthus-v0.7"},
			{"main query", "main"},
			{"dev query", "dev"},
			{"latest query", "latest"},
			{"non reviewed tag", strings.TrimSuffix(fork.reviewed, ".1") + ".2"},
		}
		for _, bad := range badVersions {
			cases = append(cases, closureCase{
				name:     "unreviewed " + fork.name + " " + bad.name,
				edit:     replaceFixtureText("go.mod", fork.module+" "+fork.reviewed, fork.module+" "+bad.version),
				wantPath: "go.mod", wantClass: "go_module", wantReason: "unreviewed_project_fork_version",
			})
		}
	}

	surfaces := []struct{ name, path, class string }{
		{"nested go mod", "nested/module/go.mod", "go_module"},
		{"nested go sum", "nested/module/go.sum", "go_checksum"},
		{"nested go work", "nested/work/go.work", "workspace"},
		{"nested go work sum", "nested/work/go.work.sum", "workspace_checksum"},
		{"nested vendor manifest", "nested/vendor/modules.txt", "vendor_manifest"},
		{"arbitrary script", "scripts/check.py", "build_release_control"},
		{"build input", "build/closure.txt", "build_release_control"},
		{"release input", "release/closure.txt", "build_release_control"},
		{"config input", "config/closure.conf", "config"},
		{"root makefile", "Makefile", "makefile"},
		{"nested makefile", "tools/Makefile", "makefile"},
		{"make include", "tools/closure.mk", "makefile"},
		{"dockerfile", "Dockerfile.release", "container_build"},
		{"nested containerfile", "images/Containerfile.build", "container_build"},
		{"taskfile", "Taskfile.release.yml", "taskfile"},
		{"build yaml", "ci/package-build.yaml", "build_release_config"},
		{"release toml", "ci/package-release.toml", "build_release_config"},
		{"goreleaser", ".goreleaser.yaml", "build_release_config"},
		{"backup go source", "model/escaped.go_temp", "source_identity"},
	}
	for _, surface := range surfaces {
		cases = append(cases, closureCase{
			name:     "closed surface " + surface.name,
			edit:     setFixtureText(surface.path, upstreamShip+"\n"),
			wantPath: surface.path, wantClass: surface.class, wantReason: "upstream_module_identity",
		})
	}

	cases = append(cases,
		closureCase{
			name: "structured JSON split module and version", edit: setFixtureText("release/release.json", `{"module":"`+canonicalShip+`","version":"main"}`+"\n"),
			wantPath: "release/release.json", wantClass: "release_config", wantReason: "unreviewed_project_fork_version",
		},
		closureCase{
			name: "structured YAML split module and version", edit: setFixtureText("config/dependency.yml", "module: "+canonicalShip+"\nversion: latest\n"),
			wantPath: "config/dependency.yml", wantClass: "config", wantReason: "unreviewed_project_fork_version",
		},
		closureCase{
			name: "module query syntax", edit: appendFixtureText("scripts/release.sh", "\n# "+canonicalShip+"?ref=dev\n"),
			wantPath: "scripts/release.sh", wantClass: "build_release_control", wantReason: "unreviewed_project_fork_version",
		},
		closureCase{
			name: "recursive JSON local reference", edit: func(files map[string]closureFixtureFile) {
				files["release/release.json"] = closureFixtureFile{data: `{"release_inputs":["assets/nested.json"]}` + "\n"}
				files["assets/nested.json"] = closureFixtureFile{data: `{"module":"` + upstreamShip + `"}` + "\n"}
			},
			wantPath: "assets/nested.json", wantClass: "referenced_input", wantReason: "upstream_module_identity",
		},
		closureCase{
			name: "relative recursive JSON local reference", edit: func(files map[string]closureFixtureFile) {
				files["release/release.json"] = closureFixtureFile{data: `{"release_inputs":["../assets/nested.json"]}` + "\n"}
				files["assets/nested.json"] = closureFixtureFile{data: `{"module":"` + upstreamShip + `"}` + "\n"}
			},
			wantPath: "assets/nested.json", wantClass: "referenced_input", wantReason: "upstream_module_identity",
		},
		closureCase{
			name: "tracked directory prefix expansion", edit: func(files map[string]closureFixtureFile) {
				files["release/release.json"] = closureFixtureFile{data: `{"release_inputs":["assets/bundle"]}` + "\n"}
				files["assets/bundle/nested.json"] = closureFixtureFile{data: `{"module":"` + upstreamShip + `"}` + "\n"}
			},
			wantPath: "assets/bundle/nested.json", wantClass: "referenced_input", wantReason: "upstream_module_identity",
		},
		closureCase{
			name: "recursive script local reference", edit: func(files map[string]closureFixtureFile) {
				files["scripts/release.sh"] = closureFixtureFile{data: "#!/bin/sh\ncat assets/module.txt\n", executable: true}
				files["assets/module.txt"] = closureFixtureFile{data: upstreamShip + "\n"}
			},
			wantPath: "assets/module.txt", wantClass: "referenced_input", wantReason: "upstream_module_identity",
		},
		closureCase{
			name: "recursive YAML local reference", edit: func(files map[string]closureFixtureFile) {
				files["config/dependency.yml"] = closureFixtureFile{data: "input: assets/module.json\n"}
				files["assets/module.json"] = closureFixtureFile{data: `{"module":"` + upstreamShip + `"}` + "\n"}
			},
			wantPath: "assets/module.json", wantClass: "referenced_input", wantReason: "upstream_module_identity",
		},
		closureCase{
			name: "templated workflow static reference", edit: func(files map[string]closureFixtureFile) {
				files[".github/workflows/release.yml"] = closureFixtureFile{data: "name: Release\ninput: ${{ github.workspace }}/hidden/dependency.json\n"}
				files["hidden/dependency.json"] = closureFixtureFile{data: `{"module":"` + upstreamShip + `"}` + "\n"}
			},
			wantPath: "hidden/dependency.json", wantClass: "referenced_input", wantReason: "upstream_module_identity",
		},
		closureCase{
			name: "bare sibling reference", edit: func(files map[string]closureFixtureFile) {
				files[".github/workflows/release.yml"] = closureFixtureFile{data: "name: Release\ninput: hidden.json\n"}
				files[".github/workflows/hidden.json"] = closureFixtureFile{data: `{"module":"` + upstreamShip + `"}` + "\n"}
			},
			wantPath: ".github/workflows/hidden.json", wantClass: "referenced_input", wantReason: "upstream_module_identity",
		},
		closureCase{
			name: "duplicate root and sibling references are both scanned", edit: func(files map[string]closureFixtureFile) {
				files[".github/workflows/release.yml"] = closureFixtureFile{data: "name: Release\ninput: duplicate.json\n"}
				files["duplicate.json"] = closureFixtureFile{data: "{}\n"}
				files[".github/workflows/duplicate.json"] = closureFixtureFile{data: `{"module":"` + upstreamShip + `"}` + "\n"}
			},
			wantPath: ".github/workflows/duplicate.json", wantClass: "referenced_input", wantReason: "upstream_module_identity",
		},
		closureCase{
			name: "unreferenced fixture control is excluded", wantPass: true,
			edit: setFixtureText("testdata/go.mod", "module example.com/fixture\n\nrequire "+upstreamShip+" v0.6.0\n"),
		},
		closureCase{
			name: "production reference reaches fixture control", edit: func(files map[string]closureFixtureFile) {
				files[".github/workflows/release.yml"] = closureFixtureFile{data: "name: Release\ninput: testdata/go.mod\n"}
				files["testdata/go.mod"] = closureFixtureFile{data: "module example.com/fixture\n\nrequire " + upstreamShip + " v0.6.0\n"}
			},
			wantPath: "testdata/go.mod", wantClass: "referenced_input", wantReason: "upstream_module_identity",
		},
		closureCase{
			name: "untracked local reference", edit: setFixtureText("release/release.json", `{"release_inputs":["assets/missing.json"]}`+"\n"),
			wantPath: "release/release.json", wantClass: "release_config", wantReason: "referenced_input_untracked",
		},
		closureCase{
			name: "outside repository reference", edit: setFixtureText("release/release.json", `{"release_inputs":["../../private.json"]}`+"\n"),
			wantPath: "release/release.json", wantClass: "release_config", wantReason: "reference_outside_repo",
		},
		closureCase{
			name: "malformed declared JSON", edit: setFixtureText("release/release.json", "{\"release_inputs\":[\n"),
			wantPath: "release/release.json", wantClass: "release_config", wantReason: "malformed_control_input",
		},
		closureCase{
			name: "malformed YAML control", edit: setFixtureText("config/dependency.yml", "inputs: [unterminated\n"),
			wantPath: "config/dependency.yml", wantClass: "config", wantReason: "malformed_control_input",
		},
		closureCase{
			name: "tracked symlink", edit: func(files map[string]closureFixtureFile) {
				files["build/dependency-link"] = closureFixtureFile{symlink: "../go.mod"}
			},
			wantPath: "build/dependency-link", wantClass: "build_release_control", wantReason: "tracked_symlink",
		},
	)

	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			root := writeClosureFixtureWithSupport(t, test.edit, test.supportEdit)
			result := runFixtureVerifier(t, verifier, root)
			assertFixtureResult(t, root, result, test)
			if test.deterministic {
				again := runFixtureVerifier(t, verifier, root)
				assertFixtureResult(t, root, again, test)
				if !bytes.Equal(result.inventory, again.inventory) || !bytes.Equal(result.evidence, again.evidence) {
					t.Error("identical runs produced different inventory or evidence")
				}
			}
		})
	}
}

func baseClosureContains(path string) bool {
	switch path {
	case ".github/actions/release/action.yml", ".github/workflows/release.yml", "go.mod", "go.sum", "main.go", "LICENSE", "release/nested.json", "release/release.json", "scripts/release.sh", "vendor/modules.txt":
		return true
	default:
		return false
	}
}

func baseClosureFixture(provenance closureProvenanceFixture) map[string]closureFixtureFile {
	reviewed := make([]map[string]any, 0, len(provenance.dependencies))
	for _, dependency := range provenance.dependencies {
		reviewed = append(reviewed, map[string]any{
			"license":             map[string]string{"path": "LICENSE", "sha256": dependency.licenseDigest},
			"module":              dependency.module,
			"peeled_commit_sha":   dependency.commit,
			"provenance_manifest": map[string]string{"path": "provenance/closure-manifest.json", "sha256": dependency.manifestDigest},
			"ref":                 dependency.ref,
			"repository":          dependency.repository,
			"tag_object_sha":      dependency.tag,
			"tree_sha":            dependency.tree,
			"version":             dependency.version,
		})
	}
	manifest := map[string]any{
		"dependency_control_inputs": []string{".github/actions/release/action.yml", ".github/workflows/release.yml", "scripts/release.sh", "release/release.json"},
		"fork": map[string]string{
			"intended_prerelease": "v0.0.1-helianthus.1",
			"lifecycle":           "temporary_downstream_patch_carrier",
			"origin":              "https://github.com/Project-Helianthus/dependency-closure-fixture.git",
		},
		"license":                 map[string]string{"path": "LICENSE", "sha256": fmt.Sprintf("%x", sha256.Sum256([]byte("fixture license\n")))},
		"module":                  "github.com/Project-Helianthus/dependency-closure-fixture",
		"notice_inventory":        []string{},
		"reviewed_dependencies":   reviewed,
		"schema":                  "helianthus.provenance.closure-manifest.v2",
		"source_header_inventory": map[string]any{"globs": []string{"**/*.go"}, "headers": []string{}},
		"upstream": map[string]string{
			"peeled_commit_sha": provenance.upstream.commit,
			"ref":               provenance.upstream.ref,
			"remote":            provenance.upstream.repository,
			"tag":               provenance.upstream.version,
			"tag_object_sha":    provenance.upstream.tag,
			"tree_sha":          provenance.upstream.tree,
		},
	}
	manifestData, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		panic(err)
	}
	manifestData = append(manifestData, '\n')

	return map[string]closureFixtureFile{
		".github/actions/release/action.yml": {data: "name: Release\nruns:\n  using: composite\n  steps:\n    - shell: bash\n      run: scripts/release.sh release/release.json\n"},
		".github/workflows/release.yml":      {data: "name: Release\njobs:\n  release:\n    steps:\n      - uses: ./.github/actions/release\n"},
		"go.mod": {data: `module github.com/Project-Helianthus/dependency-closure-fixture

go 1.22.0

require (
	github.com/Project-Helianthus/helianthus-ship-go ` + canonicalVer + `
	github.com/Project-Helianthus/helianthus-spine-go v0.7.1-helianthus.1
	github.com/Project-Helianthus/helianthus-eebus-go v0.7.0-helianthus.1
	example.com/unrelated v0.0.0-20260716000000-0123456789ab
)
`},
		"go.sum":                           {},
		"main.go":                          {data: "package fixture\n\nimport _ \"github.com/Project-Helianthus/helianthus-ship-go/api\"\n"},
		"provenance/closure-manifest.json": {data: string(manifestData)},
		"LICENSE":                          {data: "fixture license\n"},
		"release/nested.json":              {data: `{"module":"github.com/Project-Helianthus/helianthus-ship-go@` + canonicalVer + `"}` + "\n"},
		"release/release.json":             {data: `{"release_inputs":["release/nested.json"]}` + "\n"},
		"scripts/release.sh":               {data: "#!/bin/sh\nset -eu\ntest -f \"${1:?release config required}\"\n", executable: true},
		"vendor/modules.txt":               {data: "# github.com/Project-Helianthus/helianthus-ship-go " + canonicalVer + "\n## explicit; go 1.22\ngithub.com/Project-Helianthus/helianthus-ship-go/api\n"},
	}
}

func setFixtureText(path, data string) func(map[string]closureFixtureFile) {
	return func(files map[string]closureFixtureFile) { files[path] = closureFixtureFile{data: data} }
}

func appendFixtureText(path, data string) func(map[string]closureFixtureFile) {
	return func(files map[string]closureFixtureFile) {
		file := files[path]
		file.data += data
		files[path] = file
	}
}

func replaceFixtureText(path, old, new string) func(map[string]closureFixtureFile) {
	return func(files map[string]closureFixtureFile) {
		file := files[path]
		file.data = strings.Replace(file.data, old, new, 1)
		files[path] = file
	}
}

func mutateFixtureManifest(mutate func(map[string]any)) func(map[string]closureFixtureFile) {
	return func(files map[string]closureFixtureFile) {
		file := files["provenance/closure-manifest.json"]
		var manifest map[string]any
		if err := json.Unmarshal([]byte(file.data), &manifest); err != nil {
			panic(err)
		}
		mutate(manifest)
		data, err := json.MarshalIndent(manifest, "", "  ")
		if err != nil {
			panic(err)
		}
		file.data = string(append(data, '\n'))
		files["provenance/closure-manifest.json"] = file
	}
}

func manifestObject(parent map[string]any, key string) map[string]any {
	value, ok := parent[key].(map[string]any)
	if !ok {
		panic("manifest object missing: " + key)
	}
	return value
}

func firstReviewedDependency(manifest map[string]any) map[string]any {
	dependencies, ok := manifest["reviewed_dependencies"].([]any)
	if !ok || len(dependencies) == 0 {
		panic("reviewed dependency missing")
	}
	dependency, ok := dependencies[0].(map[string]any)
	if !ok {
		panic("reviewed dependency malformed")
	}
	return dependency
}

func writeClosureFixture(t *testing.T, edit func(map[string]closureFixtureFile)) string {
	return writeClosureFixtureWithSupport(t, edit, nil)
}

func writeClosureFixtureWithSupport(t *testing.T, edit func(map[string]closureFixtureFile), supportEdit func(*testing.T, string)) string {
	t.Helper()
	root := t.TempDir()
	t.Cleanup(func() { removeFixtureModuleCache(root) })
	runFixtureGit(t, root, "init", "-q")
	exclude := filepath.Join(root, ".git", "info", "exclude")
	if err := os.WriteFile(exclude, []byte(".artifact-repos/\n.artifact-work/\n.gomodcache/\n.module-proxy/\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	provenance := createClosureProvenanceFixture(t, root)
	files := baseClosureFixture(provenance)
	goMod := files["go.mod"]
	if err := os.WriteFile(filepath.Join(root, "go.mod"), []byte(goMod.data), 0o644); err != nil {
		t.Fatal(err)
	}
	writeFixtureGoSum(t, root, provenance.dependencies)
	goSum, err := os.ReadFile(filepath.Join(root, "go.sum"))
	if err != nil {
		t.Fatal(err)
	}
	files["go.sum"] = closureFixtureFile{data: string(goSum)}
	if edit != nil {
		edit(files)
	}
	paths := make([]string, 0, len(files))
	for path := range files {
		paths = append(paths, path)
	}
	sort.Strings(paths)
	for _, path := range paths {
		file := files[path]
		fullPath := filepath.Join(root, filepath.FromSlash(path))
		if err := os.MkdirAll(filepath.Dir(fullPath), 0o755); err != nil {
			t.Fatal(err)
		}
		mode := os.FileMode(0o644)
		if file.executable {
			mode = 0o755
		}
		if file.symlink != "" {
			if err := os.Symlink(file.symlink, fullPath); err != nil {
				t.Fatal(err)
			}
		} else if err := os.WriteFile(fullPath, []byte(file.data), mode); err != nil {
			t.Fatal(err)
		}
	}
	if supportEdit != nil {
		supportEdit(t, root)
		writeFixtureGoSum(t, root, provenance.dependencies)
	}
	runFixtureGit(t, root, "add", "--all")
	runFixtureGit(t, root, "-c", "user.name=Fixture", "-c", "user.email=fixture@example.invalid", "-c", "commit.gpgSign=false", "commit", "-q", "-m", "fixture")
	return root
}

func mutateFixtureModuleArtifact(module, version, path, replacement string) func(*testing.T, string) {
	return func(t *testing.T, root string) {
		t.Helper()
		zipPath := filepath.Join(root, ".module-proxy", escapeModulePath(module), "@v", version+".zip")
		reader, err := zip.OpenReader(zipPath)
		if err != nil {
			t.Fatal(err)
		}
		files := make(map[string][]byte)
		prefix := module + "@" + version + "/"
		for _, entry := range reader.File {
			input, err := entry.Open()
			if err != nil {
				t.Fatal(err)
			}
			data, err := io.ReadAll(input)
			_ = input.Close()
			if err != nil {
				t.Fatal(err)
			}
			relative := strings.TrimPrefix(entry.Name, prefix)
			files[relative] = data
		}
		if err := reader.Close(); err != nil {
			t.Fatal(err)
		}
		if _, ok := files[path]; !ok {
			t.Fatalf("module fixture lacks %s", path)
		}
		files[path] = []byte(replacement)
		identity := fixtureIdentity{module: module, version: version}
		writeModuleProxyVersion(t, root, identity, files)
	}
}

func createClosureProvenanceFixture(t *testing.T, root string) closureProvenanceFixture {
	t.Helper()
	upstream := createFixtureIdentity(t, root, "upstream", "example.com/upstream", "v0.0.0", false)
	dependencies := []fixtureIdentity{
		createFixtureIdentity(t, root, "eebus", canonicalEEBus, reviewedEEBus, true),
		createFixtureIdentity(t, root, "ship", canonicalShip, canonicalVer, true),
		createFixtureIdentity(t, root, "spine", canonicalSpine, reviewedSpine, true),
	}
	return closureProvenanceFixture{upstream: upstream, dependencies: dependencies}
}

func createFixtureIdentity(t *testing.T, root, name, module, version string, moduleArtifact bool) fixtureIdentity {
	t.Helper()
	work := filepath.Join(root, ".artifact-work", name)
	repository := filepath.Join(root, ".artifact-repos", name+".git")
	if err := os.MkdirAll(filepath.Join(work, "provenance"), 0o755); err != nil {
		t.Fatal(err)
	}
	license := []byte("fixture dependency license\n")
	manifest := []byte(fmt.Sprintf("{\"module\":%q,\"schema\":\"fixture.provenance.v1\"}\n", module))
	if err := os.WriteFile(filepath.Join(work, "LICENSE"), license, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(work, "provenance", "closure-manifest.json"), manifest, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(work, "go.mod"), []byte("module "+module+"\n\ngo 1.22.0\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	runFixtureGit(t, work, "init", "-q")
	runFixtureGit(t, work, "add", "--all")
	runFixtureGit(t, work, "-c", "user.name=Fixture", "-c", "user.email=fixture@example.invalid", "-c", "commit.gpgSign=false", "commit", "-q", "-m", "fixture")
	runFixtureGit(t, work, "-c", "user.name=Fixture", "-c", "user.email=fixture@example.invalid", "-c", "tag.gpgSign=false", "tag", "-a", version, "-m", "fixture tag")
	if err := os.MkdirAll(filepath.Dir(repository), 0o755); err != nil {
		t.Fatal(err)
	}
	runFixtureGit(t, root, "clone", "--quiet", "--bare", work, repository)
	identity := fixtureIdentity{
		module:         module,
		version:        version,
		repository:     filepath.ToSlash(filepath.Join(".artifact-repos", name+".git")),
		ref:            "refs/tags/" + version,
		licenseDigest:  fmt.Sprintf("%x", sha256.Sum256(license)),
		manifestDigest: fmt.Sprintf("%x", sha256.Sum256(manifest)),
		tag:            fixtureGitOutput(t, work, "rev-parse", version),
		commit:         fixtureGitOutput(t, work, "rev-parse", version+"^{commit}"),
		tree:           fixtureGitOutput(t, work, "rev-parse", version+"^{tree}"),
	}
	if moduleArtifact {
		writeModuleProxyVersion(t, root, identity, map[string][]byte{
			"LICENSE":                          license,
			"go.mod":                           []byte("module " + module + "\n\ngo 1.22.0\n"),
			"provenance/closure-manifest.json": manifest,
		})
	}
	return identity
}

func writeModuleProxyVersion(t *testing.T, root string, identity fixtureIdentity, files map[string][]byte) {
	t.Helper()
	directory := filepath.Join(root, ".module-proxy", escapeModulePath(identity.module), "@v")
	if err := os.MkdirAll(directory, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(directory, "list"), []byte(identity.version+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	info := []byte(fmt.Sprintf("{\"Version\":%q,\"Time\":\"2026-07-16T00:00:00Z\"}\n", identity.version))
	if err := os.WriteFile(filepath.Join(directory, identity.version+".info"), info, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(directory, identity.version+".mod"), files["go.mod"], 0o644); err != nil {
		t.Fatal(err)
	}
	zipPath := filepath.Join(directory, identity.version+".zip")
	zipFile, err := os.Create(zipPath)
	if err != nil {
		t.Fatal(err)
	}
	writer := zip.NewWriter(zipFile)
	paths := make([]string, 0, len(files))
	for path := range files {
		paths = append(paths, path)
	}
	sort.Strings(paths)
	for _, path := range paths {
		entry, err := writer.Create(identity.module + "@" + identity.version + "/" + path)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := entry.Write(files[path]); err != nil {
			t.Fatal(err)
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	if err := zipFile.Close(); err != nil {
		t.Fatal(err)
	}
}

func writeFixtureGoSum(t *testing.T, root string, dependencies []fixtureIdentity) {
	t.Helper()
	removeFixtureModuleCache(root)
	if err := os.Remove(filepath.Join(root, "go.sum")); err != nil && !os.IsNotExist(err) {
		t.Fatal(err)
	}
	var lines []string
	for _, dependency := range dependencies {
		cmd := exec.Command("go", "mod", "download", "-json", dependency.module+"@"+dependency.version)
		cmd.Dir = root
		cmd.Env = fixtureGoEnvironment(root)
		output, err := cmd.Output()
		if err != nil {
			t.Fatalf("prepare module fixture %s: %v", dependency.module, err)
		}
		var downloaded struct {
			Sum, GoModSum string
		}
		if err := json.Unmarshal(output, &downloaded); err != nil {
			t.Fatal(err)
		}
		lines = append(lines,
			dependency.module+" "+dependency.version+" "+downloaded.Sum,
			dependency.module+" "+dependency.version+"/go.mod "+downloaded.GoModSum,
		)
	}
	sort.Strings(lines)
	if err := os.WriteFile(filepath.Join(root, "go.sum"), []byte(strings.Join(lines, "\n")+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	removeFixtureModuleCache(root)
}

func fixtureGoEnvironment(root string) []string {
	return append(os.Environ(),
		"GOFLAGS=-mod=readonly",
		"GOMODCACHE="+filepath.Join(root, ".gomodcache"),
		"GONOSUMDB=*",
		"GOPROXY=file://"+filepath.Join(root, ".module-proxy"),
		"GOSUMDB=off",
		"GOWORK=off",
	)
}

func escapeModulePath(path string) string {
	var escaped strings.Builder
	for _, character := range path {
		if character >= 'A' && character <= 'Z' {
			escaped.WriteByte('!')
			escaped.WriteRune(character + ('a' - 'A'))
			continue
		}
		escaped.WriteRune(character)
	}
	return escaped.String()
}

func fixtureGitOutput(t *testing.T, root string, args ...string) string {
	t.Helper()
	cmd := exec.Command("git", args...)
	cmd.Dir = root
	output, err := cmd.Output()
	if err != nil {
		t.Fatalf("git %s: %v", strings.Join(args, " "), err)
	}
	return strings.TrimSpace(string(output))
}

func runFixtureGit(t *testing.T, root string, args ...string) {
	t.Helper()
	cmd := exec.Command("git", args...)
	cmd.Dir = root
	cmd.Env = append(os.Environ(), "GIT_AUTHOR_DATE=2026-07-16T00:00:00Z", "GIT_COMMITTER_DATE=2026-07-16T00:00:00Z")
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("git %s: %v\n%s", strings.Join(args, " "), err, output)
	}
}

func runFixtureVerifier(t *testing.T, verifier, root string) closureResult {
	t.Helper()
	removeFixtureModuleCache(root)
	outputDir := t.TempDir()
	inventoryPath := filepath.Join(outputDir, "tracked.nul")
	evidencePath := filepath.Join(outputDir, "evidence.json")
	cmd := exec.Command("python3", verifier, "--repo", ".", "--manifest", "provenance/closure-manifest.json", "--inventory-output", inventoryPath, "--evidence-output", evidencePath)
	cmd.Dir = root
	cmd.Env = fixtureGoEnvironment(root)
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	err := cmd.Run()
	return closureResult{err: err, stdout: stdout.Bytes(), stderr: stderr.Bytes(), inventory: readFixtureOutput(t, inventoryPath), evidence: readFixtureOutput(t, evidencePath)}
}

func removeFixtureModuleCache(root string) {
	cache := filepath.Join(root, ".gomodcache")
	_ = filepath.Walk(cache, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return os.Chmod(path, 0o700)
		}
		return os.Chmod(path, 0o600)
	})
	_ = os.RemoveAll(cache)
}

func readFixtureOutput(t *testing.T, path string) []byte {
	t.Helper()
	data, err := os.ReadFile(path)
	if err == nil {
		return data
	}
	if os.IsNotExist(err) {
		return nil
	}
	t.Fatal(err)
	return nil
}

func assertFixtureResult(t *testing.T, root string, result closureResult, test closureCase) {
	t.Helper()
	wantInventory := fixtureInventory(t, root)
	if !bytes.Equal(result.inventory, wantInventory) || len(result.inventory) == 0 || result.inventory[len(result.inventory)-1] != 0 {
		t.Errorf("inventory is not exact NUL-delimited git ls-files output: got %q want %q", result.inventory, wantInventory)
	}
	evidence := decodeCanonicalEvidence(t, result.evidence)
	digest := sha256.Sum256(wantInventory)
	inventory, ok := evidence["tracked_inventory"].(map[string]any)
	if evidence["schema"] != "helianthus.dependency-closure-evidence.v3" || !ok || inventory["sha256"] != fmt.Sprintf("%x", digest) {
		t.Errorf("evidence identity/digest mismatch: %s", result.evidence)
	}
	assertEvidenceFieldsAreStable(t, evidence)

	if test.wantPass {
		if result.err != nil || evidence["result"] != "pass" {
			t.Fatalf("verifier rejected valid fixture: %v\nstdout=%s\nstderr=%s\nevidence=%s", result.err, result.stdout, result.stderr, result.evidence)
		}
		if violations, ok := evidence["violations"].([]any); !ok || len(violations) != 0 {
			t.Errorf("passing evidence violations = %v; want empty array", evidence["violations"])
		}
		if len(result.stdout) != 0 || len(result.stderr) != 0 {
			t.Errorf("passing verifier must be quiet: stdout=%q stderr=%q", result.stdout, result.stderr)
		}
		return
	}

	if result.err == nil || evidence["result"] != "fail" {
		t.Fatalf("verifier accepted forbidden fixture; want reason=%s path=%s class=%s", test.wantReason, test.wantPath, test.wantClass)
	}
	wantDiagnostic := fmt.Sprintf("dependency-closure: FAIL reason=%s path=%s class=%s", test.wantReason, test.wantPath, test.wantClass)
	if len(result.stdout) != 0 || !strings.Contains(string(result.stderr), wantDiagnostic) {
		t.Errorf("diagnostics do not contain stable rejection %q: stdout=%q stderr=%q", wantDiagnostic, result.stdout, result.stderr)
	}
	linePattern := regexp.MustCompile(`^dependency-closure: FAIL reason=[a-z0-9_]+ path=[A-Za-z0-9_./-]+ class=[a-z0-9_]+$`)
	for _, line := range strings.Split(strings.TrimSpace(string(result.stderr)), "\n") {
		if !linePattern.MatchString(line) {
			t.Errorf("diagnostic contains data beyond reason/path/class: %q", line)
		}
	}
	if bytes.Contains(result.stderr, []byte(root)) {
		t.Error("diagnostics contain unstable absolute fixture path")
	}
	foundViolation := false
	if violations, ok := evidence["violations"].([]any); ok {
		for _, value := range violations {
			violation, ok := value.(map[string]any)
			if ok && violation["reason"] == test.wantReason && violation["path"] == test.wantPath && violation["class"] == test.wantClass {
				foundViolation = true
			}
		}
	}
	if !foundViolation {
		t.Errorf("evidence lacks reason=%s path=%s class=%s: %s", test.wantReason, test.wantPath, test.wantClass, result.evidence)
	}
	if test.secret != "" {
		for name, output := range map[string][]byte{"stdout": result.stdout, "stderr": result.stderr, "inventory": result.inventory, "evidence": result.evidence} {
			if bytes.Contains(output, []byte(test.secret)) {
				t.Errorf("%s discloses private file contents", name)
			}
		}
	}
}

func decodeCanonicalEvidence(t *testing.T, data []byte) map[string]any {
	t.Helper()
	var evidence map[string]any
	if err := json.Unmarshal(data, &evidence); err != nil {
		t.Fatalf("evidence is not JSON: %v\n%s", err, data)
	}
	canonical, err := json.Marshal(evidence)
	if err != nil {
		t.Fatal(err)
	}
	canonical = append(canonical, '\n')
	if !bytes.Equal(data, canonical) {
		t.Errorf("evidence is not compact sorted-key JSON with one trailing newline: got %q want %q", data, canonical)
	}
	return evidence
}

func assertEvidenceFieldsAreStable(t *testing.T, evidence map[string]any) {
	t.Helper()
	wantFields := []string{"artifacts", "commands", "git_refs", "inputs", "manifest", "result", "schema", "source_sha", "tracked_inventory", "verifier", "violations"}
	if len(evidence) != len(wantFields) {
		t.Errorf("evidence has unexpected top-level shape: %v", evidence)
	}
	for _, field := range wantFields {
		if _, ok := evidence[field]; !ok {
			t.Errorf("evidence lacks %s", field)
		}
	}
	sourceSHA, ok := evidence["source_sha"].(string)
	if !ok || !regexp.MustCompile(`^[0-9a-f]{40}$`).MatchString(sourceSHA) {
		t.Errorf("evidence source_sha = %v; want full commit", evidence["source_sha"])
	}
	for _, field := range []string{"artifacts", "git_refs", "inputs", "violations"} {
		values, ok := evidence[field].([]any)
		if !ok {
			t.Errorf("evidence %s is %T, want array", field, evidence[field])
			continue
		}
		for _, value := range values {
			object, ok := value.(map[string]any)
			if !ok {
				t.Errorf("evidence %s entry is %T, want object", field, value)
				continue
			}
			wantKeys := 4
			if field == "git_refs" {
				wantKeys = 6
			}
			if field == "violations" {
				wantKeys = 3
			}
			valid := len(object) == wantKeys
			switch field {
			case "artifacts", "inputs":
				valid = valid && object["path"] != nil && object["class"] != nil && object["sha256"] != nil && object["source_sha"] == sourceSHA
			case "git_refs":
				valid = valid && object["repository"] != nil && object["ref"] != nil && object["tag_object_sha"] != nil && object["peeled_commit_sha"] != nil && object["tree_sha"] != nil && object["source_sha"] == sourceSHA
			case "violations":
				valid = valid && object["path"] != nil && object["class"] != nil && object["reason"] != nil
			}
			if !valid {
				t.Errorf("evidence %s entry has an unstable shape: %v", field, object)
			}
		}
	}
	for _, field := range []string{"manifest", "verifier", "tracked_inventory"} {
		object, ok := evidence[field].(map[string]any)
		if !ok || object["sha256"] == nil || object["source_sha"] != sourceSHA {
			t.Errorf("evidence %s digest is not source-bound: %v", field, evidence[field])
		}
	}
	commands, ok := evidence["commands"].([]any)
	if !ok || len(commands) != 7 {
		t.Errorf("evidence commands = %v; want seven versioned commands", evidence["commands"])
	}
}

func fixtureInventory(t *testing.T, root string) []byte {
	t.Helper()
	cmd := exec.Command("git", "ls-files", "-z")
	cmd.Dir = root
	output, err := cmd.Output()
	if err != nil {
		t.Fatal(err)
	}
	return output
}
