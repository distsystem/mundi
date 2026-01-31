#!/usr/bin/env python3
"""Sync conda-forge package metadata using sharded repodata (CEP-16).

Downloads only the shards for packages we depend on, instead of the full 70MB repodata.
Shards are content-addressable and cached locally by hash.

Usage:
    pixi run python scripts/sync_conda_forge.py                  # sync all host_deps
    pixi run python scripts/sync_conda_forge.py --list-changed   # list changed shards (no download)
    pixi run python scripts/sync_conda_forge.py --check          # download and check version updates
    pixi run python scripts/sync_conda_forge.py --packages zlib openssl
"""

import hashlib
import json
import subprocess
import sys
import urllib.request
from pathlib import Path

import msgpack
import zstandard

CHANNEL_URL = "https://conda.anaconda.org/conda-forge/linux-64"
CACHE_DIR = Path(".cache/conda-forge")
SHARDS_INDEX_CACHE = CACHE_DIR / "repodata_shards.msgpack.zst"
SHARDS_DIR = CACHE_DIR / "shards"
DEPS_CACHE = CACHE_DIR / "deps.json"
INDEX_ETAG_CACHE = CACHE_DIR / "shards_index_etag"
TRACKED_HASHES_CACHE = CACHE_DIR / "tracked_hashes.json"  # hash of each tracked package


def download_shards_index(force: bool = False) -> tuple[dict, bool]:
    """Download shards index with ETag caching. Returns (data, was_updated)."""
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    url = f"{CHANNEL_URL}/repodata_shards.msgpack.zst"

    headers = {}
    if not force and INDEX_ETAG_CACHE.exists() and SHARDS_INDEX_CACHE.exists():
        headers["If-None-Match"] = INDEX_ETAG_CACHE.read_text().strip()

    req = urllib.request.Request(url, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            size_kb = (resp.length or 0) // 1024
            print(f"Downloading shards index ({size_kb}KB)...")
            data = resp.read()
            SHARDS_INDEX_CACHE.write_bytes(data)
            if etag := resp.headers.get("ETag"):
                INDEX_ETAG_CACHE.write_text(etag)
            decompressed = zstandard.decompress(data)
            return msgpack.unpackb(decompressed, raw=False), True
    except urllib.error.HTTPError as e:
        if e.code == 304:
            print("Shards index unchanged (ETag match)")
            decompressed = zstandard.decompress(SHARDS_INDEX_CACHE.read_bytes())
            return msgpack.unpackb(decompressed, raw=False), False
        raise


def get_shard_url(index_info: dict, shard_hash: bytes) -> str:
    """Construct shard URL from index info and hash."""
    hash_hex = shard_hash.hex()
    base_url = index_info.get("shards_base_url", "")
    if base_url:
        return f"{base_url}{hash_hex}.msgpack.zst"
    return f"{CHANNEL_URL}/{hash_hex}.msgpack.zst"


def download_shard(url: str, shard_hash: bytes) -> dict:
    """Download and verify a shard. Uses content-addressed caching."""
    SHARDS_DIR.mkdir(parents=True, exist_ok=True)
    hash_hex = shard_hash.hex()
    cache_file = SHARDS_DIR / f"{hash_hex}.msgpack.zst"

    # Content-addressable: if file exists with matching name, it's valid
    if cache_file.exists():
        decompressed = zstandard.decompress(cache_file.read_bytes())
        return msgpack.unpackb(decompressed, raw=False)

    # Download
    with urllib.request.urlopen(url, timeout=30) as resp:
        data = resp.read()

    # Verify hash
    actual_hash = hashlib.sha256(data).digest()
    if actual_hash != shard_hash:
        msg = f"Hash mismatch: expected {hash_hex}, got {actual_hash.hex()}"
        raise ValueError(msg)

    cache_file.write_bytes(data)
    decompressed = zstandard.decompress(data)
    return msgpack.unpackb(decompressed, raw=False)


def parse_version(ver: str) -> tuple[int, ...]:
    """Parse version string to comparable tuple."""
    parts = []
    for p in ver.replace("-", ".").split("."):
        # Extract leading digits
        num = ""
        for c in p:
            if c.isdigit():
                num += c
            else:
                break
        parts.append(int(num) if num else 0)
    return tuple(parts)


def extract_latest_from_shard(shard: dict) -> dict | None:
    """Extract latest version info from a shard."""
    packages = shard.get("packages", {})
    packages.update(shard.get("packages.conda", {}))

    if not packages:
        return None

    # Find latest by (parsed_version, build_number)
    latest = None
    latest_key: tuple[tuple[int, ...], int] = ((), 0)
    for info in packages.values():
        ver = info.get("version", "")
        bn = info.get("build_number", 0)
        key = (parse_version(ver), bn)
        if key > latest_key:
            latest_key = key
            latest = info

    if latest:
        return {
            "version": latest.get("version", ""),
            "build_number": latest.get("build_number", 0),
            "build": latest.get("build", ""),
            "depends": latest.get("depends", []),
        }
    return None


def get_local_host_deps() -> set[str]:
    """Get all host_deps from local recipes."""
    result = subprocess.run(
        ["python", "scripts/gen_deps.py", "--output", "json"],
        capture_output=True,
        text=True,
        cwd=Path.cwd(),
    )
    if result.returncode != 0:
        return set()

    data = json.loads(result.stdout)
    deps = set()
    for pkg in data.values():
        deps.update(pkg.get("host_deps", []))
    return deps


def find_updates(old_deps: dict, new_deps: dict) -> list[dict]:
    """Find version changes."""
    updates = []
    for name, new_info in new_deps.items():
        old_ver = old_deps.get(name, {}).get("version", "(new)")
        new_ver = new_info["version"]
        if old_ver != new_ver:
            updates.append({"name": name, "old": old_ver, "new": new_ver})
    return updates


def main():
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--packages", nargs="*", help="Specific packages to sync")
    parser.add_argument("--check", action="store_true", help="Check for updates only")
    parser.add_argument("--list-changed", action="store_true", help="List changed shards without downloading")
    parser.add_argument("--force", action="store_true", help="Force re-download index")
    args = parser.parse_args()

    # Get packages to track
    if args.packages:
        package_names = set(args.packages)
    else:
        print("Collecting host_deps from local recipes...")
        package_names = get_local_host_deps()
        print(f"Tracking {len(package_names)} dependencies")

    if not package_names:
        print("No packages to sync")
        return

    # Load old cache
    old_deps = {}
    if DEPS_CACHE.exists():
        old_deps = json.loads(DEPS_CACHE.read_text())

    old_hashes = {}
    if TRACKED_HASHES_CACHE.exists():
        old_hashes = json.loads(TRACKED_HASHES_CACHE.read_text())

    # Download shards index
    index, index_updated = download_shards_index(force=args.force)
    shards_map = index.get("shards", {})
    index_info = index.get("info", {})

    # Determine which shards need fetching
    new_deps = dict(old_deps)  # Start with old data
    new_hashes = {}
    missing = []
    changed = []
    unchanged = 0

    for name in sorted(package_names):
        if name not in shards_map:
            missing.append(name)
            continue

        shard_hash = shards_map[name]
        hash_hex = shard_hash.hex()
        new_hashes[name] = hash_hex

        # Skip if hash unchanged (content-addressable = same content)
        if old_hashes.get(name) == hash_hex and name in old_deps:
            unchanged += 1
            continue

        changed.append(name)

    # List changed shards without downloading
    if args.list_changed:
        print(f"\n{len(changed)} changed shard(s), {unchanged} unchanged:")
        for name in changed:
            old_hash = old_hashes.get(name, "(new)")[:12]
            new_hash = new_hashes[name][:12]
            print(f"  {name}: {old_hash} → {new_hash}")
        if missing:
            print(f"\nNot found: {', '.join(missing[:10])}" + ("..." if len(missing) > 10 else ""))
        return

    # Download changed shards
    for name in changed:
        shard_hash = shards_map[name]
        url = get_shard_url(index_info, shard_hash)

        try:
            shard = download_shard(url, shard_hash)
            if info := extract_latest_from_shard(shard):
                new_deps[name] = info
        except Exception as e:
            print(f"  Failed to fetch {name}: {e}", file=sys.stderr)

    if changed:
        print(f"Fetched {len(changed)} changed shard(s), {unchanged} unchanged")
    elif unchanged:
        print(f"All {unchanged} shards unchanged")

    if missing:
        print(f"Not found in conda-forge: {', '.join(missing[:10])}" + ("..." if len(missing) > 10 else ""))

    # Find updates
    updates = find_updates(old_deps, new_deps)

    if args.check:
        if updates:
            print(f"\n{len(updates)} update(s) available:")
            for u in updates:
                print(f"  {u['name']}: {u['old']} → {u['new']}")
        else:
            print("No updates")
        return

    # Save
    DEPS_CACHE.write_text(json.dumps(new_deps, indent=2))
    TRACKED_HASHES_CACHE.write_text(json.dumps(new_hashes, indent=2))
    print(f"Synced {len(new_deps)}/{len(package_names)} packages to {DEPS_CACHE}")

    if updates:
        print(f"\n{len(updates)} version update(s):")
        for u in updates:
            print(f"  {u['name']}: {u['old']} → {u['new']}")


if __name__ == "__main__":
    main()
