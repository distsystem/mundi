#!/usr/bin/env python3
"""Dependency graph POC using Lance storage + lance_graph Cypher queries.

Data flow:
    gen_deps.py JSON -> PyArrow Tables -> Lance Datasets -> Cypher Queries

Graph model:
    - Nodes: all package names (both built packages and their dependencies)
    - Edges: (src)-[:DEP]->(dst) where src depends on dst
"""

import json
import subprocess
from pathlib import Path

import lance
import pyarrow as pa
from lance_graph import CypherQuery, GraphConfig

CACHE_DIR = Path(".cache/dep-graph")

PACKAGES_SCHEMA = pa.schema([
    ("name", pa.string()),
    ("version", pa.string()),
    ("host_deps", pa.string()),
    ("run_exports", pa.string()),
])

EDGES_SCHEMA = pa.schema([
    ("src", pa.string()),
    ("dst", pa.string()),
    ("constraint", pa.string()),
])

GRAPH_CONFIG = (
    GraphConfig.builder()
    .with_node_label("Pkg", "name")
    .with_relationship("DEP", "src", "dst")
    .build()
)

# ============================================================================
# Build
# ============================================================================

def build_graph(json_path: Path | None = None) -> tuple[lance.LanceDataset, lance.LanceDataset]:
    """Build Lance datasets from gen_deps.py output."""
    if json_path and json_path.exists():
        data = json.loads(json_path.read_text())
    else:
        result = subprocess.run(
            ["python", "scripts/gen_deps.py", "--output", "json"],
            capture_output=True, text=True, cwd=Path.cwd(),
        )
        data = json.loads(result.stdout) if result.returncode == 0 else {}

    pkg_rows, edge_rows = [], []
    for name, info in data.items():
        pkg_rows.append({
            "name": name,
            "version": info.get("version", ""),
            "host_deps": json.dumps(info.get("host_deps", {})),
            "run_exports": json.dumps(info.get("run_exports", [])),
        })
        for dep, constraint in info.get("host_deps", {}).items():
            edge_rows.append({"src": name, "dst": dep, "constraint": constraint})

    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    packages_ds = lance.write_dataset(
        pa.Table.from_pylist(pkg_rows, schema=PACKAGES_SCHEMA),
        CACHE_DIR / "packages.lance", mode="overwrite",
    )
    edges_ds = lance.write_dataset(
        pa.Table.from_pylist(edge_rows, schema=EDGES_SCHEMA),
        CACHE_DIR / "edges.lance", mode="overwrite",
    )
    return packages_ds, edges_ds


def open_graph() -> tuple[lance.LanceDataset, lance.LanceDataset]:
    return (
        lance.dataset(CACHE_DIR / "packages.lance"),
        lance.dataset(CACHE_DIR / "edges.lance"),
    )

# ============================================================================
# Cypher Queries
# ============================================================================

def _all_nodes(packages_ds: lance.LanceDataset, edges_ds: lance.LanceDataset) -> pa.Table:
    """Build complete node table (union of src and dst)."""
    names = set(packages_ds.to_table()["name"].to_pylist())
    names.update(edges_ds.to_table()["dst"].to_pylist())
    return pa.table({"name": list(names)})


def cypher(query_str: str, packages_ds: lance.LanceDataset, edges_ds: lance.LanceDataset) -> pa.Table:
    """Execute Cypher query against the graph."""
    query = CypherQuery(query_str).with_config(GRAPH_CONFIG)
    return query.execute({
        "Pkg": _all_nodes(packages_ds, edges_ds),
        "DEP": edges_ds.to_table(),
    })


def rdeps(packages_ds: lance.LanceDataset, edges_ds: lance.LanceDataset, name: str) -> list[str]:
    """MATCH (p)-[:DEP]->(d) WHERE d.name = $name RETURN p.name"""
    result = cypher(f"MATCH (p:Pkg)-[:DEP]->(d:Pkg) WHERE d.name = '{name}' RETURN DISTINCT p.name", packages_ds, edges_ds)
    return sorted(result["p.name"].to_pylist())


def deps(packages_ds: lance.LanceDataset, edges_ds: lance.LanceDataset, name: str) -> list[str]:
    """MATCH (p)-[:DEP]->(d) WHERE p.name = $name RETURN d.name"""
    result = cypher(f"MATCH (p:Pkg)-[:DEP]->(d:Pkg) WHERE p.name = '{name}' RETURN DISTINCT d.name", packages_ds, edges_ds)
    return sorted(result["d.name"].to_pylist())


def dep_chain_2hop(packages_ds: lance.LanceDataset, edges_ds: lance.LanceDataset, name: str) -> list[tuple[str, str, str]]:
    """MATCH (p)-[:DEP]->(d1)-[:DEP]->(d2) WHERE p.name = $name RETURN path"""
    result = cypher(
        f"MATCH (p:Pkg)-[:DEP]->(d1:Pkg)-[:DEP]->(d2:Pkg) WHERE p.name = '{name}' RETURN p.name, d1.name, d2.name",
        packages_ds, edges_ds,
    )
    d = result.to_pydict()
    return list(zip(d["p.name"], d["d1.name"], d["d2.name"], strict=True))

# ============================================================================
# Check Rebuilds
# ============================================================================

def parse_version(ver: str) -> tuple[int, ...]:
    parts = []
    for p in ver.replace("-", ".").split("."):
        num = "".join(c for c in p if c.isdigit())
        parts.append(int(num) if num else 0)
    return tuple(parts)


def version_bound(version: str, upper_bound: str) -> tuple[int, ...]:
    n = upper_bound.count("x")
    return () if n == 0 else (parse_version(version) + (0,) * n)[:n]


def check_rebuilds(packages_ds: lance.LanceDataset, edges_ds: lance.LanceDataset, upstream: dict | None = None) -> list[tuple[str, str, str]]:
    """Find packages needing rebuild due to upstream ABI changes."""
    if upstream is None:
        cache = Path(".cache/conda-forge/deps.json")
        upstream = json.loads(cache.read_text()) if cache.exists() else {}

    rebuilds, seen = [], set()
    pkgs = packages_ds.to_table().to_pydict()

    for i in range(len(pkgs["name"])):
        name, local_ver = pkgs["name"][i], pkgs["version"][i]
        run_exports = json.loads(pkgs["run_exports"][i])

        if name not in upstream or not run_exports:
            continue
        upstream_ver = upstream[name].get("version", "")
        if not upstream_ver or local_ver == upstream_ver:
            continue

        for exp in run_exports:
            ub = exp.get("upper_bound", "")
            if not ub or version_bound(local_ver, ub) == version_bound(upstream_ver, ub):
                continue
            for dep in rdeps(packages_ds, edges_ds, name):
                if (dep, name) not in seen:
                    seen.add((dep, name))
                    rebuilds.append((dep, name, f"{name} {local_ver} -> {upstream_ver} (ub={ub})"))

    return sorted(rebuilds)

# ============================================================================
# Demo
# ============================================================================

if __name__ == "__main__":
    packages_ds, edges_ds = build_graph()
    print(f"Built: {packages_ds.count_rows()} packages, {edges_ds.count_rows()} edges")
    print(f"Total nodes: {_all_nodes(packages_ds, edges_ds).num_rows}\n")

    print("=== rdeps('python') ===")
    print(rdeps(packages_ds, edges_ds, "python")[:10], "...\n")

    print("=== deps('aider-chat') ===")
    print(deps(packages_ds, edges_ds, "aider-chat"), "\n")

    print("=== 2-hop chain from 'libblockdev' ===")
    chains = dep_chain_2hop(packages_ds, edges_ds, "libblockdev")
    for p, d1, d2 in chains[:5]:
        print(f"  {p} -> {d1} -> {d2}")

    print(f"\n=== Rebuilds needed: {len(check_rebuilds(packages_ds, edges_ds))} ===")
