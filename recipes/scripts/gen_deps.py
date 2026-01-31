#!/usr/bin/env python3
"""Generate minimal dependency graph data for ABI rebuild tracking.

Stores:
- host_deps: packages this links against (graph edges)
- run_exports: ABI compatibility pattern (e.g., upper_bound="x.x" means <major.minor+1)
- version: current version (used to compute actual constraint from pattern)
"""

import json
import re
import subprocess
import sys
from pathlib import Path
from typing import Any

PKG_SPEC_PAT = re.compile(r"^([a-zA-Z0-9_-]+)\s*(.*)")


def extract_pkg_spec(dep: Any) -> tuple[str, str] | None:
    """Extract package name and version constraint from dependency spec."""
    if isinstance(dep, str):
        m = PKG_SPEC_PAT.match(dep.strip())
        if m:
            return m.group(1), m.group(2).strip()
        return None
    if isinstance(dep, dict) and "pin_subpackage" in dep:
        ps = dep["pin_subpackage"]
        name = ps.get("name")
        if not name:
            return None
        # Record constraint pattern for pin_subpackage
        if ps.get("exact"):
            return name, "exact"
        parts = []
        if lb := ps.get("lower_bound"):
            parts.append(f"lb={lb}")
        if ub := ps.get("upper_bound"):
            parts.append(f"ub={ub}")
        return name, ",".join(parts) if parts else ""
    return None


def extract_pkg_name(dep: Any) -> str | None:
    """Extract package name only (for backward compat)."""
    if spec := extract_pkg_spec(dep):
        return spec[0]
    return None


def parse_run_export(dep: Any) -> dict | None:
    """Parse run_export to structured format."""
    if isinstance(dep, dict) and "pin_subpackage" in dep:
        ps = dep["pin_subpackage"]
        return {
            "name": ps.get("name"),
            "exact": ps.get("exact", False),
            "lower_bound": ps.get("lower_bound"),  # e.g., "x.x.x.x.x.x"
            "upper_bound": ps.get("upper_bound"),  # e.g., "x.x"
        }
    if isinstance(dep, str):
        return {"name": extract_pkg_name(dep), "spec": dep}
    return None


def get_variant_configs(recipe_dir: Path, workspace: Path) -> list[Path]:
    configs = []
    for name in ["variant_config.yaml", "variants.yaml"]:
        if (candidate := recipe_dir / name).exists():
            configs.append(candidate)
    if (global_cfg := workspace / "variants.yaml").exists():
        configs.append(global_cfg)
    return configs


def resolve_recipe(recipe_path: Path, workspace: Path) -> list[dict]:
    cmd = [
        "rattler-build", "build", "--render-only", "--experimental",
        "--recipe", str(recipe_path),
    ]
    for vc in get_variant_configs(recipe_path.parent, workspace):
        cmd.extend(["--variant-config", str(vc)])

    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        return []

    outputs = []
    for item in json.loads(result.stdout):
        recipe = item.get("recipe", {})
        pkg = recipe.get("package", {})
        build = recipe.get("build", {})
        reqs = recipe.get("requirements", {})

        # Merge cache + output host deps (preserve version constraints)
        cache = recipe.get("cache", {})
        cache_reqs = cache.get("requirements", {})
        host_raw = (cache_reqs.get("host") or []) + (reqs.get("host") or [])
        host_deps = {}
        for d in host_raw:
            if spec := extract_pkg_spec(d):
                name, constraint = spec
                # Keep more specific constraint if same package appears multiple times
                if name not in host_deps or (constraint and not host_deps[name]):
                    host_deps[name] = constraint

        # Parse run_exports with version pattern
        run_exports = []
        for kind, deps in (reqs.get("run_exports") or {}).items():
            for dep in deps:
                parsed = parse_run_export(dep)
                if parsed:
                    parsed["kind"] = kind  # weak/strong
                    run_exports.append(parsed)

        outputs.append({
            "name": pkg.get("name", ""),
            "version": str(pkg.get("version", "")),
            "build_string": build.get("string", ""),
            "host_deps": host_deps,
            "run_exports": run_exports,
        })

    return outputs


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("packages", nargs="*")
    parser.add_argument("--output", choices=["table", "json"], default="table")
    args = parser.parse_args()

    workspace = Path.cwd()
    recipes = (
        [workspace / p / "recipe.yaml" for p in args.packages if (workspace / p / "recipe.yaml").exists()]
        if args.packages else sorted(workspace.glob("*/recipe.yaml"))
    )

    all_pkgs = []
    for recipe_path in recipes:
        try:
            all_pkgs.extend(resolve_recipe(recipe_path, workspace))
        except Exception as e:
            print(f"# Failed: {recipe_path.parent.name}: {e}", file=sys.stderr)

    if args.output == "json":
        result = {}
        for pkg in all_pkgs:
            name = pkg["name"]
            if name not in result:
                result[name] = {
                    "version": pkg["version"],
                    "host_deps": pkg["host_deps"],
                    "run_exports": pkg["run_exports"],
                }
        print(json.dumps(result, indent=2))
    else:
        for pkg in all_pkgs:
            print(f"\033[1m{pkg['name']}\033[0m {pkg['version']}")
            if pkg["host_deps"]:
                deps_str = ", ".join(
                    f"{n} {c}" if c else n for n, c in sorted(pkg["host_deps"].items())
                )
                print(f"  \033[33mhost_deps:\033[0m {deps_str}")
            for exp in pkg["run_exports"]:
                ub = exp.get("upper_bound", "")
                kind = exp.get("kind", "weak")
                prefix = f"{kind}:" if kind != "weak" else ""
                print(f"  \033[35mrun_exports:\033[0m {prefix}{exp['name']} (upper_bound={ub})")


if __name__ == "__main__":
    main()
