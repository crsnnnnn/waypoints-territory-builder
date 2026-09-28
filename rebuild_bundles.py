#!/usr/bin/env python3
"""Rebuild every stored city bundle, then remove the entries rebuilds leave.

The Worker rebuilds a city only when someone asks for it, so after a builder
revision the cities nobody opens keep their old bundle, and the first explorer
there gets old areas while it rebuilds. Run this after every revision bump:

    python3 rebuild_bundles.py rebuild   # start a build for each outdated city
    python3 rebuild_bundles.py clean     # once the builds have finished

`rebuild` starts builds the way the Worker does: the coordinate goes into the
private bucket under a random id and the public workflow gets only that id, so
no coordinate or place name reaches the public run logs.

A rebuild on a newer Overture release can publish a city under a new id, which
leaves the old id's manifest, bundles and index entries behind. `clean`
deletes a city whose manifest is below the current revision only when the
Worker already answers its point with a current bundle.

Needs only the standard library, a `npx wrangler login` session for the
bucket and an authenticated `gh` for the workflow. `--dry-run` lists what
would happen without changing anything.
"""

from __future__ import annotations

import argparse
import datetime
import gzip
import json
import math
import re
import subprocess
import sys
import urllib.error
import urllib.parse
import urllib.request
import uuid
from pathlib import Path
from typing import Any, Iterator

HERE = Path(__file__).resolve().parent
API = "https://api.cloudflare.com/client/v4"
WORKFLOW = "territory-bundle.yml"
# Cloudflare rejects the default Python user agent on workers.dev.
USER_AGENT = "waypoints-territory-rebuild"


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("command", choices=("rebuild", "clean"))
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    config = (HERE / "wrangler.toml").read_text()
    worker = setting(config, r'^name\s*=\s*"([^"]+)"')
    bucket = setting(config, r'bucket_name\s*=\s*"([^"]+)"')
    repository = setting(config, r'GITHUB_REPOSITORY\s*=\s*"([^"]+)"')
    ref = setting(config, r'GITHUB_REF\s*=\s*"([^"]+)"')
    revision = int(
        setting((HERE / "build_bundle.py").read_text(), r"^BUNDLE_REVISION\s*=\s*(\d+)")
    )

    cloudflare = Cloudflare(wrangler_token())
    objects = f"accounts/{cloudflare.account}/r2/buckets/{bucket}/objects"
    manifests = [
        cloudflare.json(f"{objects}/{key}")
        for key in cloudflare.keys(objects, "manifests/")
    ]
    outdated = [m for m in manifests if bundle_revision(m["version"]) < revision]
    print(
        f"{len(manifests)} cities stored, {len(outdated)} below revision {revision}"
    )

    if args.command == "rebuild":
        for manifest in outdated:
            rebuild(cloudflare, objects, repository, ref, manifest, args.dry_run)
        if outdated and not args.dry_run:
            print(f"Started {len(outdated)} builds. Run `clean` once they finish.")
        return

    subdomain = cloudflare.json(
        f"accounts/{cloudflare.account}/workers/subdomain"
    )["subdomain"]
    endpoint = f"https://{worker}.{subdomain}.workers.dev/v1/bundle"
    for manifest in outdated:
        served = served_version(endpoint, manifest)
        if served is None or bundle_revision(served) < revision:
            # Not rebuilt yet, or the rebuild failed. The old bundle is still
            # what explorers there get, so it stays.
            print(f"kept {manifest['cityId']}: its point is served {served}")
            continue
        clean(cloudflare, objects, manifest, args.dry_run)


def rebuild(
    cloudflare: Cloudflare,
    objects: str,
    repository: str,
    ref: str,
    manifest: dict[str, Any],
    dry_run: bool,
) -> None:
    latitude = float(manifest["latitude"])
    longitude = float(manifest["longitude"])
    x, y = tile(latitude, longitude, 12)
    if dry_run:
        print(f"would rebuild {manifest['cityId']} ({manifest['version']})")
        return
    request_id = str(uuid.uuid4())
    record = {
        "requestKey": f"12/{x}/{y}",
        "latitude": latitude,
        "longitude": longitude,
        "requestedAt": datetime.datetime.now(datetime.timezone.utc).isoformat(),
    }
    cloudflare.put(f"{objects}/requests/by-id/{request_id}.json", record)
    subprocess.run(
        [
            "gh", "workflow", "run", WORKFLOW,
            "--repo", repository,
            "--ref", ref,
            "--field", f"request_id={request_id}",
        ],
        check=True,
        capture_output=True,
    )
    print(f"rebuilding {manifest['cityId']} ({manifest['version']})")


def clean(
    cloudflare: Cloudflare,
    objects: str,
    manifest: dict[str, Any],
    dry_run: bool,
) -> None:
    city = manifest["cityId"]
    keys = list(cloudflare.keys(objects, f"bundles/{city}/"))
    listed = manifest.get("tiles")
    if isinstance(listed, list):
        keys.extend(f"index/12/{x}/{y}/{city}.json" for x, y in listed)
    else:
        # Manifests from before the tile list was written: find the city's
        # entries in the index instead.
        keys.extend(
            key
            for key in cloudflare.keys(objects, "index/12/")
            if key.endswith(f"/{city}.json")
        )
    # The manifest goes last, so an interrupted run can be repeated.
    keys.append(f"manifests/{city}.json")
    for key in keys:
        if dry_run:
            print(f"would delete {key}")
        else:
            cloudflare.delete(f"{objects}/{key}")
    print(f"{'would remove' if dry_run else 'removed'} {city}: {len(keys)} objects")


def served_version(endpoint: str, manifest: dict[str, Any]) -> str | None:
    query = urllib.parse.urlencode(
        {"lat": manifest["latitude"], "lon": manifest["longitude"]}
    )
    request = urllib.request.Request(
        f"{endpoint}?{query}", headers={"User-Agent": USER_AGENT}
    )
    try:
        with urllib.request.urlopen(request, timeout=60) as response:
            if response.status != 200:
                return None
            body = response.read()
    except urllib.error.URLError:
        return None
    if body[:2] == b"\x1f\x8b":
        body = gzip.decompress(body)
    return json.loads(body).get("datasetVersion")


class Cloudflare:
    def __init__(self, token: str) -> None:
        self.token = token
        accounts = self.json("accounts")
        if len(accounts) != 1:
            sys.exit("Expected exactly one Cloudflare account for this login")
        self.account = accounts[0]["id"]

    def call(self, method: str, path: str, body: bytes | None = None) -> bytes:
        request = urllib.request.Request(
            f"{API}/{path}",
            data=body,
            method=method,
            headers={
                "Authorization": f"Bearer {self.token}",
                "Content-Type": "application/json",
                "User-Agent": USER_AGENT,
            },
        )
        with urllib.request.urlopen(request, timeout=60) as response:
            return response.read()

    def json(self, path: str) -> Any:
        payload = json.loads(self.call("GET", path))
        # Object reads return the object itself, API calls an envelope.
        if isinstance(payload, dict) and "success" in payload and "result" in payload:
            return payload["result"]
        return payload

    def keys(self, objects: str, prefix: str) -> Iterator[str]:
        cursor = None
        while True:
            query = {"prefix": prefix, "per_page": "1000"}
            if cursor:
                query["cursor"] = cursor
            page = json.loads(
                self.call("GET", f"{objects}?{urllib.parse.urlencode(query)}")
            )
            yield from (entry["key"] for entry in page["result"])
            cursor = (page.get("result_info") or {}).get("cursor")
            if not cursor or not (page.get("result_info") or {}).get("is_truncated"):
                return

    def put(self, path: str, value: Any) -> None:
        self.call("PUT", path, json.dumps(value).encode("utf-8"))

    def delete(self, path: str) -> None:
        self.call("DELETE", path)


def wrangler_token() -> str:
    result = subprocess.run(
        ["npx", "wrangler", "auth", "token", "--json"],
        cwd=HERE,
        check=True,
        capture_output=True,
        text=True,
    )
    return json.loads(result.stdout)["token"]


def setting(text: str, pattern: str) -> str:
    match = re.search(pattern, text, re.MULTILINE)
    if match is None:
        sys.exit(f"Missing setting {pattern}")
    return match.group(1)


def bundle_revision(version: str) -> int:
    match = re.search(r"-r(\d+)$", version)
    return int(match.group(1)) if match else 0


def tile(latitude: float, longitude: float, zoom: int) -> tuple[int, int]:
    count = 2**zoom
    x = min(count - 1, max(0, math.floor((longitude + 180) / 360 * count)))
    bounded = min(85.0511287798066, max(-85.0511287798066, latitude))
    radians = math.radians(bounded)
    y = math.floor(
        (1 - math.log(math.tan(radians) + 1 / math.cos(radians)) / math.pi) / 2 * count
    )
    return x, min(count - 1, max(0, y))


if __name__ == "__main__":
    main()
