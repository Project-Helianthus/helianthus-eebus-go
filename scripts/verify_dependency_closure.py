#!/usr/bin/env python3
"""Verify tracked dependency controls and emit deterministic closure evidence."""

from __future__ import annotations

import argparse
import ast
import hashlib
import json
import os
import platform
import posixpath
import re
import shlex
import subprocess
import sys
import tempfile
from collections import deque
from pathlib import Path, PurePosixPath
from typing import Any


EVIDENCE_SCHEMA = "helianthus.dependency-closure-evidence.v3"
MANIFEST_SCHEMA = "helianthus.provenance.closure-manifest.v2"
VERIFIER_PATH = "scripts/verify_dependency_closure.py"
UPSTREAM_MODULES = tuple(
    "github.com/enbility/" + name for name in ("ship-go", "spine-go", "eebus-go")
)
PROJECT_MODULES = tuple(
    "github.com/Project-Helianthus/helianthus-" + name
    for name in ("ship-go", "spine-go", "eebus-go")
)
PORTABLE_PATH_RE = re.compile(r"^[A-Za-z0-9_./-]+$")
SHA40_RE = re.compile(r"^[0-9a-f]{40}$")
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
GO_SOURCE_RE = re.compile(r"\.go(?:$|[._-])")
REPLACE_RE = re.compile(
    r"(?mi)^\s*(?:replace\b|[\"']?replace[\"']?\s*[:=])"
)
WORKSPACE_USE_RE = re.compile(r"(?m)^\s*use(?:\s|\()")
LOCAL_OVERRIDE_RE = re.compile(
    r"(?mi)(?:=>|\b(?:path|source)\s*[:=])\s*[\"']?(?:\.\.?/)"
)
CONTROL_HINT_RE = re.compile(
    r"(?:^|[-_.])(build|dependencies|dependency|package|publish|release)(?:$|[-_.])",
    re.IGNORECASE,
)
PATH_TOKEN_RE = re.compile(
    r"\$?(?:\.\.?/)?(?:[A-Za-z0-9_.-]+/)+[A-Za-z0-9_.-]+"
)
BARE_FILE_RE = re.compile(r"(?<![A-Za-z0-9_./-])[A-Za-z0-9_-]+\.[A-Za-z0-9_.-]+")
BUILD_CONFIG_RE = re.compile(
    r"(?:build|release).*\.(?:json|toml|yaml|yml)$", re.IGNORECASE
)
TASKFILE_RE = re.compile(r"^Taskfile.*\.ya?ml$", re.IGNORECASE)
CONTAINERFILE_RE = re.compile(r"^(?:Dockerfile|Containerfile)", re.IGNORECASE)
CONFIG_NAMES = {
    ".mockery.yaml",
    ".mockery.yml",
}
FIXTURE_COMPONENTS = {
    "fixture",
    "fixtures",
    "test-fixtures",
    "testdata",
    "test-data",
    "testfixtures",
}
MANIFEST_KEYS = {
    "dependency_control_inputs",
    "fork",
    "license",
    "module",
    "notice_inventory",
    "reviewed_dependencies",
    "schema",
    "source_header_inventory",
    "upstream",
}


class CommandFailure(Exception):
    """A bounded subprocess failure that must not expose raw stderr."""


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", required=True)
    parser.add_argument("--manifest", required=True)
    parser.add_argument("--inventory-output", required=True)
    parser.add_argument("--evidence-output", required=True)
    return parser.parse_args()


def run_command(repo: Path, argv: list[str]) -> bytes:
    try:
        return subprocess.run(
            argv,
            cwd=repo,
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
        ).stdout
    except (OSError, subprocess.SubprocessError) as error:
        raise CommandFailure from error


def write_bytes(path: Path, data: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(data)


def sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def sanitize_path(value: str) -> str:
    safe = re.sub(r"[^A-Za-z0-9_./-]", "_", value)[:160]
    return safe or "invalid-path"


def normalize_repo_path(value: str) -> str | None:
    if not value or "\0" in value or "\\" in value:
        return None
    path = PurePosixPath(value)
    if path.is_absolute() or ".." in path.parts or not PORTABLE_PATH_RE.fullmatch(value):
        return None
    normalized = path.as_posix()
    if normalized in {"", "."}:
        return None
    return normalized.removeprefix("./")


def split_inventory(inventory: bytes) -> tuple[list[str], list[str]]:
    paths: list[str] = []
    invalid: list[str] = []
    for raw in inventory.rstrip(b"\0").split(b"\0") if inventory else []:
        try:
            value = raw.decode("utf-8")
        except UnicodeDecodeError:
            invalid.append("invalid-filename-" + sha256(raw)[:12])
            continue
        normalized = normalize_repo_path(value)
        if normalized is None:
            invalid.append(sanitize_path(value))
            continue
        paths.append(normalized)
    return paths, invalid


def split_modes(data: bytes) -> dict[str, str]:
    modes: dict[str, str] = {}
    for raw in data.rstrip(b"\0").split(b"\0") if data else []:
        try:
            metadata, raw_path = raw.split(b"\t", 1)
            path = raw_path.decode("utf-8")
            mode = metadata.split(b" ", 1)[0].decode("ascii")
        except (ValueError, UnicodeError):
            continue
        normalized = normalize_repo_path(path)
        if normalized is not None:
            modes[normalized] = mode
    return modes


def auto_class(path: str) -> str | None:
    parts = PurePosixPath(path).parts
    name = parts[-1]
    if (
        path.startswith("contracttests/")
        or name.endswith("_test.go")
        or any(part.lower() in FIXTURE_COMPONENTS for part in parts)
    ):
        return None
    if name == "go.mod":
        return "go_module"
    if name == "go.sum":
        return "go_checksum"
    if name == "go.work":
        return "workspace"
    if name == "go.work.sum":
        return "workspace_checksum"
    if len(parts) >= 2 and parts[-2:] == ("vendor", "modules.txt"):
        return "vendor_manifest"
    if path.startswith(".github/workflows/") and name.endswith((".yml", ".yaml")):
        return "workflow"
    if path.startswith(".github/actions/"):
        return "local_action"
    if name in {"GNUmakefile", "Makefile", "makefile"} or name.endswith(".mk"):
        return "makefile"
    if parts[0] in {"scripts", "build"}:
        return "build_release_control"
    if parts[0] == "release":
        return "release_config" if name.endswith((".json", ".toml", ".yaml", ".yml")) else "build_release_control"
    if parts[0] == "config" or name in CONFIG_NAMES:
        return "config"
    if CONTAINERFILE_RE.match(name):
        return "container_build"
    if TASKFILE_RE.match(name):
        return "taskfile"
    if name.startswith(".goreleaser.") or BUILD_CONFIG_RE.search(name):
        return "build_release_config"
    if GO_SOURCE_RE.search(name):
        return "source_identity"
    return None


def looks_unclassified_control(path: str) -> bool:
    parts = PurePosixPath(path).parts
    if path.startswith(("contracttests/", "provenance/")) or any(
        part.lower() in FIXTURE_COMPONENTS for part in parts
    ):
        return False
    return CONTROL_HINT_RE.search(PurePosixPath(path).name) is not None


def add_violation(
    violations: set[tuple[str, str, str]], path: str, input_class: str, reason: str
) -> None:
    violations.add((sanitize_path(path), input_class, reason))


def is_string_list(value: Any) -> bool:
    return isinstance(value, list) and all(isinstance(item, str) for item in value)


def valid_digest_record(value: Any, *, with_path: bool) -> bool:
    keys = {"path", "sha256"} if with_path else {"sha256"}
    if not isinstance(value, dict) or set(value) != keys:
        return False
    if with_path and normalize_repo_path(value.get("path", "")) is None:
        return False
    return isinstance(value.get("sha256"), str) and SHA256_RE.fullmatch(value["sha256"]) is not None


def validate_manifest(value: Any) -> tuple[bool, dict[str, str], list[str]]:
    if not isinstance(value, dict) or set(value) != MANIFEST_KEYS:
        return False, {}, []
    if value.get("schema") != MANIFEST_SCHEMA or not isinstance(value.get("module"), str):
        return False, {}, []

    fork = value.get("fork")
    if (
        not isinstance(fork, dict)
        or set(fork) != {"intended_prerelease", "lifecycle", "origin"}
        or fork.get("lifecycle") != "temporary_downstream_patch_carrier"
        or not isinstance(fork.get("origin"), str)
        or not isinstance(fork.get("intended_prerelease"), str)
    ):
        return False, {}, []

    upstream = value.get("upstream")
    if not isinstance(upstream, dict) or set(upstream) != {
        "peeled_commit_sha",
        "ref",
        "remote",
        "tag",
        "tag_object_sha",
        "tree_sha",
    }:
        return False, {}, []
    if not all(
        isinstance(upstream.get(field), str) and SHA40_RE.fullmatch(upstream[field])
        for field in ("peeled_commit_sha", "tag_object_sha", "tree_sha")
    ) or not all(
        isinstance(upstream.get(field), str) and upstream[field]
        for field in ("ref", "remote", "tag")
    ):
        return False, {}, []
    if upstream["ref"] != "refs/tags/" + upstream["tag"]:
        return False, {}, []

    if not valid_digest_record(value.get("license"), with_path=True):
        return False, {}, []
    if not is_string_list(value.get("notice_inventory")):
        return False, {}, []
    source_headers = value.get("source_header_inventory")
    if (
        not isinstance(source_headers, dict)
        or set(source_headers) != {"globs", "headers"}
        or not is_string_list(source_headers.get("globs"))
        or not is_string_list(source_headers.get("headers"))
    ):
        return False, {}, []

    controls = value.get("dependency_control_inputs")
    if not is_string_list(controls) or any(normalize_repo_path(path) is None for path in controls):
        return False, {}, []

    reviewed: dict[str, str] = {}
    dependencies = value.get("reviewed_dependencies")
    if not isinstance(dependencies, list):
        return False, {}, []
    dependency_keys = {
        "license",
        "module",
        "peeled_commit_sha",
        "provenance_manifest",
        "ref",
        "repository",
        "tag_object_sha",
        "tree_sha",
        "version",
    }
    for dependency in dependencies:
        if not isinstance(dependency, dict) or set(dependency) != dependency_keys:
            return False, {}, []
        module = dependency.get("module")
        version = dependency.get("version")
        if (
            module not in PROJECT_MODULES
            or not isinstance(version, str)
            or module in reviewed
            or not isinstance(dependency.get("repository"), str)
            or not dependency["repository"]
            or dependency.get("ref") != "refs/tags/" + version
            or not all(
                isinstance(dependency.get(field), str)
                and SHA40_RE.fullmatch(dependency[field])
                for field in ("peeled_commit_sha", "tag_object_sha", "tree_sha")
            )
            or not valid_digest_record(dependency.get("license"), with_path=True)
            or not valid_digest_record(dependency.get("provenance_manifest"), with_path=True)
        ):
            return False, {}, []
        reviewed[module] = version
    return True, reviewed, controls


def safe_read(
    repo: Path,
    path: str,
    modes: dict[str, str],
    violations: set[tuple[str, str, str]],
    input_class: str,
) -> bytes | None:
    if modes.get(path) == "120000":
        add_violation(violations, path, input_class, "tracked_symlink")
        return None
    candidate = repo / PurePosixPath(path)
    try:
        resolved = candidate.resolve(strict=True)
        resolved.relative_to(repo)
        if candidate.is_symlink() or not resolved.is_file():
            add_violation(violations, path, input_class, "unsafe_tracked_path")
            return None
        return resolved.read_bytes()
    except (OSError, RuntimeError, ValueError):
        add_violation(violations, path, input_class, "unreadable_input")
        return None


def decode_control(
    data: bytes,
    path: str,
    input_class: str,
    violations: set[tuple[str, str, str]],
) -> str | None:
    try:
        return data.decode("utf-8")
    except UnicodeDecodeError:
        add_violation(violations, path, input_class, "invalid_utf8")
        return None


def structured_strings(value: Any) -> list[str]:
    values: list[str] = []
    if isinstance(value, dict):
        for child in value.values():
            values.extend(structured_strings(child))
    elif isinstance(value, list):
        for child in value:
            values.extend(structured_strings(child))
    elif isinstance(value, str):
        values.append(value)
    return values


def structured_selections(value: Any) -> list[tuple[str, str]]:
    selections: list[tuple[str, str]] = []
    if isinstance(value, dict):
        module = value.get("module")
        version = value.get("version")
        if isinstance(module, str) and isinstance(version, str):
            selections.append((module, version))
        for child in value.values():
            selections.extend(structured_selections(child))
    elif isinstance(value, list):
        for child in value:
            selections.extend(structured_selections(child))
    return selections


def yaml_is_malformed(text: str) -> bool:
    stack: list[str] = []
    pairs = {"]": "[", "}": "{"}
    quote = ""
    escaped = False
    for character in text:
        if escaped:
            escaped = False
            continue
        if character == "\\" and quote:
            escaped = True
            continue
        if character in {"\"", "'"}:
            if not quote:
                quote = character
            elif quote == character:
                quote = ""
            continue
        if quote:
            continue
        if character in "[{":
            stack.append(character)
        elif character in "]}":
            if not stack or stack.pop() != pairs[character]:
                return True
    if stack or quote:
        return True
    return any(line.startswith("\t") for line in text.splitlines())


def yaml_selections(text: str) -> list[tuple[str, str]]:
    modules: dict[int, str] = {}
    selections: list[tuple[str, str]] = []
    for line in text.splitlines():
        clean = line.split("#", 1)[0].rstrip()
        if not clean:
            continue
        indent = len(clean) - len(clean.lstrip(" "))
        match = re.match(r"\s*(?:-\s*)?module\s*:\s*[\"']?([^\s\"']+)", clean)
        if match:
            modules[indent] = match.group(1)
            continue
        match = re.match(r"\s*(?:-\s*)?version\s*:\s*[\"']?([^\s\"']+)", clean)
        if match and indent in modules:
            selections.append((modules[indent], match.group(1)))
    return selections


def extract_references(
    text: str,
    path: str,
    input_class: str,
    violations: set[tuple[str, str, str]],
) -> tuple[list[str], list[tuple[str, str]]]:
    suffix = PurePosixPath(path).suffix.lower()
    references: list[str] = []
    selections: list[tuple[str, str]] = []
    if input_class == "source_identity":
        return [], []
    if suffix == ".json":
        try:
            value = json.loads(text)
        except json.JSONDecodeError:
            add_violation(violations, path, input_class, "malformed_control_input")
            return [], []
        references.extend(structured_strings(value))
        selections.extend(structured_selections(value))
    elif suffix in {".yaml", ".yml"}:
        if yaml_is_malformed(text):
            add_violation(violations, path, input_class, "malformed_control_input")
            return [], []
        for line in text.splitlines():
            templated = re.search(r"(?:\$\{\{|\{\{).*?\}\}", line) is not None
            static_line = re.sub(r"(?:\$\{\{|\{\{).*?\}\}", "", line)
            line_references = PATH_TOKEN_RE.findall(static_line)
            line_references.extend(BARE_FILE_RE.findall(static_line))
            for reference in line_references:
                if templated:
                    reference = reference.lstrip("/")
                    while reference.startswith("../"):
                        reference = reference[3:]
                if reference:
                    references.append(reference)
        selections.extend(yaml_selections(text))
    elif suffix == ".py":
        try:
            tree = ast.parse(text)
        except SyntaxError:
            add_violation(violations, path, input_class, "malformed_control_input")
            return [], []
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call) or not node.args:
                continue
            function = node.func
            name = function.id if isinstance(function, ast.Name) else function.attr if isinstance(function, ast.Attribute) else ""
            first = node.args[0]
            if name in {"open", "Path", "read_bytes", "read_text"} and isinstance(first, ast.Constant) and isinstance(first.value, str):
                references.append(first.value)
    elif suffix in {".bash", ".sh", ".zsh"} or input_class == "makefile":
        for line in text.splitlines():
            if not line.strip() or line.lstrip().startswith("#"):
                continue
            try:
                references.extend(shlex.split(line, comments=True, posix=True))
            except ValueError:
                add_violation(violations, path, input_class, "malformed_control_input")
                return [], []
    else:
        references.extend(PATH_TOKEN_RE.findall(text))
        references.extend(BARE_FILE_RE.findall(text))
    return references, selections


def normalize_reference(
    raw: str, referring_path: str, tracked: set[str]
) -> tuple[list[str], str | None]:
    value = raw.strip().strip("\"'`()[]{}:,;")
    if (
        not value
        or value in {".", "..", "./..."}
        or value.startswith(("$", "${{", "http://", "https://", "github.com/"))
        or "@" in value and not value.startswith(("./", "../"))
        or re.fullmatch(r"[0-9a-f]{40,64}", value)
        or re.fullmatch(r"v?\d+(?:\.\d+)+(?:[-+].*)?", value)
    ):
        return [], None
    if value.startswith("/"):
        return [], "reference_outside_repo"

    explicit = value.startswith(("./", "../"))
    if value.startswith("../"):
        candidates = [PurePosixPath(referring_path).parent / value]
    elif value.startswith("./"):
        candidates = [PurePosixPath(value)]
    else:
        candidates = [
            PurePosixPath(value),
            PurePosixPath(referring_path).parent / value,
        ]
    resolved_paths: set[str] = set()
    for candidate in candidates:
        normalized = posixpath.normpath(candidate.as_posix()).removeprefix("./")
        if normalized == ".." or normalized.startswith("../"):
            return [], "reference_outside_repo"
        if normalized in tracked:
            resolved_paths.add(normalized)
        prefix = normalized.rstrip("/") + "/"
        resolved_paths.update(path for path in tracked if path.startswith(prefix))

    if resolved_paths:
        return sorted(resolved_paths), None

    first_component = value.removeprefix("./").split("/", 1)[0]
    if not value.startswith(("./", "../")) and "." in first_component and "/" in value:
        return [], None
    if re.fullmatch(r"[A-Z][A-Z0-9_]*", first_component):
        return [], None

    looks_like_file = PurePosixPath(value).suffix != ""
    if explicit or looks_like_file and "/" in value and not value.startswith(("go", "python", "bash")):
        return [], "referenced_input_untracked"
    return [], None


def scan_module_versions(
    text: str,
    structured: list[tuple[str, str]],
    path: str,
    input_class: str,
    reviewed: dict[str, str],
    violations: set[tuple[str, str, str]],
) -> None:
    selections = list(structured)
    for module in PROJECT_MODULES:
        pattern = re.compile(
            re.escape(module) + r"(?:@|\?ref=|[ \t]+)([^\s\"'`,;#)\]}]+)"
        )
        for match in pattern.finditer(text):
            version = match.group(1).removesuffix("/go.mod")
            selections.append((module, version))

    plain_module = re.findall(
        r"(?mi)^\s*module\s*[:=]\s*[\"']?([^\s\"']+)", text
    )
    plain_version = re.findall(
        r"(?mi)^\s*version\s*[:=]\s*[\"']?([^\s\"']+)", text
    )
    if len(plain_module) == 1 and len(plain_version) == 1:
        selections.append((plain_module[0], plain_version[0]))

    for module, version in selections:
        if module in PROJECT_MODULES and reviewed.get(module) != version:
            add_violation(
                violations, path, input_class, "unreviewed_project_fork_version"
            )


def read_module_artifact(module_dir: str, path: str) -> bytes | None:
    normalized = normalize_repo_path(path)
    if normalized is None:
        return None
    try:
        root = Path(module_dir).resolve(strict=True)
        candidate = (root / PurePosixPath(normalized)).resolve(strict=True)
        candidate.relative_to(root)
        if candidate.is_symlink() or not candidate.is_file():
            return None
        return candidate.read_bytes()
    except (OSError, RuntimeError, ValueError):
        return None


def download_module(repo: Path, module: str, version: str) -> dict[str, Any] | None:
    try:
        with tempfile.TemporaryDirectory(prefix="helianthus-closure-go-") as temporary:
            temporary_path = Path(temporary)
            (temporary_path / "go.mod").write_text(
                "module helianthus.local/dependency-verifier\n\ngo 1.22.0\n",
                encoding="utf-8",
            )
            output = run_command(
                temporary_path,
                ["go", "mod", "download", "-json", module + "@" + version],
            )
        downloaded = json.loads(output.decode("utf-8"))
    except (CommandFailure, UnicodeError, json.JSONDecodeError, OSError):
        return None
    return downloaded if isinstance(downloaded, dict) else None


def go_sum_entries(data: bytes) -> dict[tuple[str, str], str]:
    entries: dict[tuple[str, str], str] = {}
    try:
        lines = data.decode("utf-8").splitlines()
    except UnicodeError:
        return entries
    for line in lines:
        fields = line.split()
        if len(fields) == 3:
            entries[(fields[0], fields[1])] = fields[2]
    return entries


def verify_git_ref(
    repo: Path,
    repository: str,
    ref: str,
    expected: dict[str, str],
    violation_path: str,
    violations: set[tuple[str, str, str]],
) -> dict[str, str] | None:
    try:
        with tempfile.TemporaryDirectory(prefix="helianthus-closure-git-") as temporary:
            git_dir = str(Path(temporary) / "objects.git")
            run_command(repo, ["git", "init", "--bare", "--quiet", git_dir])
            run_command(
                repo,
                [
                    "git",
                    "--git-dir",
                    git_dir,
                    "fetch",
                    "--quiet",
                    "--depth=1",
                    "--no-tags",
                    repository,
                    "+" + ref + ":refs/verify/source",
                ],
            )
            object_type = run_command(
                repo, ["git", "--git-dir", git_dir, "cat-file", "-t", "refs/verify/source"]
            ).decode("ascii").strip()
            actual = {
                "tag_object_sha": run_command(
                    repo, ["git", "--git-dir", git_dir, "rev-parse", "refs/verify/source"]
                ).decode("ascii").strip(),
                "peeled_commit_sha": run_command(
                    repo, ["git", "--git-dir", git_dir, "rev-parse", "refs/verify/source^{commit}"]
                ).decode("ascii").strip(),
                "tree_sha": run_command(
                    repo, ["git", "--git-dir", git_dir, "rev-parse", "refs/verify/source^{tree}"]
                ).decode("ascii").strip(),
            }
    except (CommandFailure, UnicodeError, OSError):
        add_violation(violations, violation_path, "git_ref", "git_ref_fetch_failed")
        return None

    if object_type != "tag":
        add_violation(violations, violation_path, "git_ref", "tag_object_type_mismatch")
    for field, reason in (
        ("tag_object_sha", "tag_object_mismatch"),
        ("peeled_commit_sha", "peeled_commit_mismatch"),
        ("tree_sha", "tree_mismatch"),
    ):
        if actual[field] != expected[field]:
            add_violation(violations, violation_path, "git_ref", reason)
    return {
        "peeled_commit_sha": actual["peeled_commit_sha"],
        "ref": ref,
        "repository": repository,
        "tag_object_sha": actual["tag_object_sha"],
        "tree_sha": actual["tree_sha"],
    }


def verify_manifest_bindings(
    repo: Path,
    manifest_path: str,
    manifest: dict[str, Any],
    tracked: set[str],
    modes: dict[str, str],
    violations: set[tuple[str, str, str]],
) -> tuple[list[dict[str, str]], list[dict[str, str]]]:
    artifacts: list[dict[str, str]] = []
    git_refs: list[dict[str, str]] = []
    sums_data = safe_read(repo, "go.sum", modes, violations, "go_checksum") or b""
    sums = go_sum_entries(sums_data)

    license_record = manifest["license"]
    license_path = license_record["path"]
    if license_path not in tracked:
        add_violation(violations, license_path, "local_license", "artifact_untracked")
    local_license = safe_read(repo, license_path, modes, violations, "local_license")
    if local_license is not None:
        digest = sha256(local_license)
        artifacts.append({"class": "local_license", "path": license_path, "sha256": digest})
        if digest != license_record["sha256"]:
            add_violation(violations, license_path, "local_license", "artifact_digest_mismatch")

    upstream = manifest["upstream"]
    upstream_ref = verify_git_ref(
        repo,
        upstream["remote"],
        upstream["ref"],
        upstream,
        manifest_path,
        violations,
    )
    if upstream_ref is not None:
        git_refs.append(upstream_ref)

    for dependency in manifest["reviewed_dependencies"]:
        module = dependency["module"]
        version = dependency["version"]
        artifact_root = module + "@" + version + "/"
        downloaded = download_module(repo, module, version)
        if downloaded is None:
            add_violation(violations, manifest_path, "dependency_module", "module_download_failed")

        if (
            not isinstance(downloaded, dict)
            or downloaded.get("Path") != module
            or downloaded.get("Version") != version
            or not isinstance(downloaded.get("Dir"), str)
        ):
            if downloaded is not None:
                add_violation(violations, manifest_path, "dependency_module", "module_identity_mismatch")
        else:
            if (
                sums.get((module, version)) != downloaded.get("Sum")
                or sums.get((module, version + "/go.mod")) != downloaded.get("GoModSum")
            ):
                add_violation(violations, manifest_path, "dependency_module", "module_checksum_mismatch")
            for input_class, field in (
                ("dependency_license", "license"),
                ("dependency_provenance", "provenance_manifest"),
            ):
                record = dependency[field]
                data = read_module_artifact(downloaded["Dir"], record["path"])
                if data is None:
                    add_violation(violations, manifest_path, input_class, "artifact_unreadable")
                    continue
                digest = sha256(data)
                artifacts.append(
                    {
                        "class": input_class,
                        "path": artifact_root + record["path"],
                        "sha256": digest,
                    }
                )
                if digest != record["sha256"]:
                    add_violation(violations, manifest_path, input_class, "artifact_digest_mismatch")

        dependency_ref = verify_git_ref(
            repo,
            dependency["repository"],
            dependency["ref"],
            dependency,
            manifest_path,
            violations,
        )
        if dependency_ref is not None:
            git_refs.append(dependency_ref)

    return sorted(artifacts, key=lambda item: (item["class"], item["path"])), sorted(
        git_refs, key=lambda item: (item["repository"], item["ref"])
    )


def evidence_bytes(
    result: str,
    source_sha: str,
    inventory: bytes,
    manifest_path: str,
    manifest_data: bytes,
    verifier_data: bytes,
    inputs: dict[str, tuple[str, str]],
    artifacts: list[dict[str, str]],
    git_refs: list[dict[str, str]],
    violations: set[tuple[str, str, str]],
    git_version: str,
    go_version: str,
) -> bytes:
    digest = lambda data: {"sha256": sha256(data), "source_sha": source_sha}
    evidence = {
        "artifacts": [dict(artifact, source_sha=source_sha) for artifact in artifacts],
        "commands": [
            {
                "argv": ["git", "ls-files", "-z"],
                "name": "tracked_inventory",
                "tool_version": git_version,
            },
            {
                "argv": ["git", "ls-files", "-s", "-z"],
                "name": "tracked_modes",
                "tool_version": git_version,
            },
            {
                "argv": ["git", "rev-parse", "HEAD"],
                "name": "source_sha",
                "tool_version": git_version,
            },
            {
                "argv": ["git", "diff", "--quiet", "HEAD", "--"],
                "name": "source_content_match",
                "tool_version": git_version,
            },
            {
                "argv": ["go", "mod", "download", "-json", "MODULE@VERSION"],
                "name": "dependency_module_artifacts",
                "tool_version": go_version,
            },
            {
                "argv": ["git", "fetch", "--depth=1", "--no-tags", "REPOSITORY", "REF"],
                "name": "annotated_tag_identity",
                "tool_version": git_version,
            },
            {
                "argv": [
                    "python3",
                    VERIFIER_PATH,
                    "--repo",
                    ".",
                    "--manifest",
                    manifest_path,
                    "--inventory-output",
                    "OUTPUT_PATH",
                    "--evidence-output",
                    "OUTPUT_PATH",
                ],
                "name": "dependency_closure",
                "tool_version": platform.python_version(),
            },
        ],
        "git_refs": [dict(reference, source_sha=source_sha) for reference in git_refs],
        "inputs": [
            {
                "class": input_class,
                "path": path,
                "sha256": content_sha,
                "source_sha": source_sha,
            }
            for path, (input_class, content_sha) in sorted(inputs.items())
        ],
        "manifest": {
            "path": manifest_path,
            **digest(manifest_data),
        },
        "result": result,
        "schema": EVIDENCE_SCHEMA,
        "source_sha": source_sha,
        "tracked_inventory": digest(inventory),
        "verifier": {"path": VERIFIER_PATH, **digest(verifier_data)},
        "violations": [
            {"class": input_class, "path": path, "reason": reason}
            for path, input_class, reason in sorted(violations)
        ],
    }
    return (json.dumps(evidence, sort_keys=True, separators=(",", ":")) + "\n").encode()


def verify(args: argparse.Namespace) -> int:
    repo = Path(args.repo).resolve()
    violations: set[tuple[str, str, str]] = set()
    inventory = b""
    mode_data = b""
    source_sha = ""
    git_version = "unavailable"
    go_version = "unavailable"

    try:
        git_version = run_command(repo, ["git", "--version"]).decode("ascii").strip()
    except (CommandFailure, UnicodeError):
        add_violation(violations, "git", "repository", "git_unavailable")
    try:
        go_version = run_command(repo, ["go", "version"]).decode("ascii").strip()
    except (CommandFailure, UnicodeError):
        pass
    try:
        inventory = run_command(repo, ["git", "ls-files", "-z"])
    except CommandFailure:
        add_violation(violations, "git-ls-files", "repository", "git_inventory_failed")
    write_bytes(Path(args.inventory_output), inventory)
    try:
        mode_data = run_command(repo, ["git", "ls-files", "-s", "-z"])
    except CommandFailure:
        add_violation(violations, "git-ls-files", "repository", "git_modes_failed")
    try:
        source_sha = run_command(repo, ["git", "rev-parse", "HEAD"]).decode("ascii").strip()
        if SHA40_RE.fullmatch(source_sha) is None:
            raise ValueError
    except (CommandFailure, UnicodeError, ValueError):
        source_sha = ""
        add_violation(violations, "git-head", "repository", "source_sha_unavailable")
    try:
        run_command(repo, ["git", "diff", "--quiet", "HEAD", "--"])
    except CommandFailure:
        add_violation(
            violations,
            "git-head",
            "repository",
            "tracked_content_not_at_head",
        )

    paths, invalid_paths = split_inventory(inventory)
    tracked = set(paths)
    modes = split_modes(mode_data)
    for path in invalid_paths:
        add_violation(violations, path, "repository", "nonportable_tracked_path")
    for path, mode in modes.items():
        if mode == "120000":
            add_violation(violations, path, auto_class(path) or "tracked_file", "tracked_symlink")
        elif mode == "160000":
            add_violation(violations, path, "tracked_file", "unsupported_tracked_mode")

    manifest_path = normalize_repo_path(args.manifest) or "invalid-manifest-path"
    manifest_data = b""
    manifest_value: Any = None
    reviewed: dict[str, str] = {}
    declared: list[str] = []
    artifacts: list[dict[str, str]] = []
    git_refs: list[dict[str, str]] = []
    if manifest_path not in tracked:
        add_violation(violations, manifest_path, "provenance", "invalid_manifest")
    else:
        manifest_data = safe_read(repo, manifest_path, modes, violations, "provenance") or b""
        try:
            manifest_value = json.loads(manifest_data.decode("utf-8"))
        except (UnicodeError, json.JSONDecodeError):
            add_violation(violations, manifest_path, "provenance", "invalid_manifest")
        valid, reviewed, declared = validate_manifest(manifest_value)
        if not valid:
            add_violation(violations, manifest_path, "provenance", "invalid_manifest_schema")
        else:
            artifacts, git_refs = verify_manifest_bindings(
                repo, manifest_path, manifest_value, tracked, modes, violations
            )

    inputs: dict[str, str] = {}
    for path in paths:
        classified = auto_class(path)
        if classified is not None:
            inputs[path] = classified
        elif looks_unclassified_control(path):
            add_violation(
                violations, path, "unclassified", "unclassified_dependency_control"
            )
    for path in declared:
        normalized = normalize_repo_path(path)
        if normalized is None:
            add_violation(violations, manifest_path, "provenance", "invalid_declared_input")
        elif normalized not in tracked:
            add_violation(violations, normalized, "declared_input", "declared_input_untracked")
        else:
            inputs[normalized] = auto_class(normalized) or "declared_input"

    queue = deque(sorted(inputs))
    scanned: dict[str, tuple[str, str]] = {}
    while queue:
        path = queue.popleft()
        if path in scanned:
            continue
        input_class = inputs[path]
        data = safe_read(repo, path, modes, violations, input_class)
        if data is None:
            continue
        scanned[path] = (input_class, sha256(data))
        text = decode_control(data, path, input_class, violations)
        if text is None:
            continue
        if any(module in text for module in UPSTREAM_MODULES):
            add_violation(violations, path, input_class, "upstream_module_identity")
        if REPLACE_RE.search(text):
            add_violation(violations, path, input_class, "replace_directive")
        if input_class == "workspace" and WORKSPACE_USE_RE.search(text):
            add_violation(violations, path, input_class, "workspace_local_selection")
        if any(module in text for module in PROJECT_MODULES) and LOCAL_OVERRIDE_RE.search(text):
            add_violation(violations, path, input_class, "local_path_override")

        references, structured = extract_references(
            text, path, input_class, violations
        )
        scan_module_versions(
            text, structured, path, input_class, reviewed, violations
        )
        for raw_reference in references:
            resolved, reason = normalize_reference(raw_reference, path, tracked)
            if reason is not None:
                add_violation(violations, path, input_class, reason)
                continue
            for referenced_path in resolved:
                if referenced_path in {manifest_path}:
                    continue
                inputs.setdefault(
                    referenced_path,
                    auto_class(referenced_path) or "referenced_input",
                )
                queue.append(referenced_path)

    try:
        verifier_data = Path(__file__).resolve().read_bytes()
    except OSError:
        verifier_data = b""
        add_violation(violations, VERIFIER_PATH, "verifier", "unreadable_input")

    result = "fail" if violations else "pass"
    evidence = evidence_bytes(
        result,
        source_sha,
        inventory,
        manifest_path,
        manifest_data,
        verifier_data,
        scanned,
        artifacts,
        git_refs,
        violations,
        git_version,
        go_version,
    )
    write_bytes(Path(args.evidence_output), evidence)
    if violations:
        for path, input_class, reason in sorted(violations):
            print(
                f"dependency-closure: FAIL reason={reason} path={path} class={input_class}",
                file=sys.stderr,
            )
        return 1
    return 0


def main() -> int:
    args = parse_args()
    try:
        return verify(args)
    except Exception:
        print(
            "dependency-closure: FAIL reason=internal_error path=verifier class=verifier",
            file=sys.stderr,
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
