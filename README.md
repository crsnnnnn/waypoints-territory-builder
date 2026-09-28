# Waypoints territory builder

Builds the territory bundles the Waypoints app uses to name the place an
explorer stands in, list its districts, villages, or neighbouring places,
and count the streets and ground explored in each.

The app calls `GET /v1/bundle?lat=<latitude>&lon=<longitude>` on the
Cloudflare Worker in `worker.js`. The Worker checks the R2 spatial index and
returns the compressed bundle of the city holding that point. When no bundle
covers it, the Worker starts this repository's workflow once for that zoom-12
lookup tile. The builder reads boundaries from the latest Overture release and
named streets from OpenStreetMap, projects the streets onto the app's zoom-20
exploration grid, uploads the bundle to R2, and indexes it for the whole city.
Outlines under 5 ha, such as plazas tagged as neighbourhoods, are left out.
The ground the outlines leave open is filled so every part of a place has a
named local area: first the place's named neighbourhood points claim the
blocks around them, cut along main roads, rail lines and large water, then
named land such as parks, golf courses, cemeteries, campuses and industrial
estates claims what is left, and the rest is cut along main roads into areas
of about 1 km2, each named after named land filling much of it or after the
crossroads of its two main roads, such as "Courtney & Dewdney". Areas of one name
that overlap or lie within 100 m are one place mapped twice and are merged. A
bundle carries at most 500 areas, which leaves large cities room to fill the
gaps their outlines leave, and a fifth of the room left after the outlines is kept
for named land and road areas. Bundles older than 35 days, or built under an older revision, keep being
served while a fresh one builds in the background. A bundle of an older revision
carries a `Retry-After` header while it rebuilds, so the app asks once more
and picks up the new one without a relaunch. Once the fresh bundle is
published, the city's older bundles and the lookup tiles it no longer
covers are deleted, so the bucket holds one bundle per city.

## Rebuilding every city after a revision

The Worker rebuilds a city only when someone asks for it, so after
`BUNDLE_REVISION` goes up, a city nobody opens keeps its old bundle and the
next explorer there first gets the old areas. After deploying a new revision,
rebuild every stored city, then remove what the rebuilds left:

```
python3 rebuild_bundles.py rebuild
python3 rebuild_bundles.py clean
```

Run `clean` once the builds have finished. A rebuild on a newer Overture
release can publish a city under a new id, and `clean` deletes the old id's
manifest, bundles and index entries, but only when the Worker already answers
that city's point with a current bundle. Both commands take `--dry-run`. The
script needs a `npx wrangler login` session and an authenticated `gh`, and it
starts builds the way the Worker does, so coordinates stay out of the public
logs.

## Privacy of public logs

This repository is public so that GitHub Actions minutes cost nothing, which
makes its run logs public too. The Worker therefore never passes a coordinate
to the workflow. It stores the request in the private bucket under a random id
and passes only that id. Builds started that way print no coordinate, search
box, or place name, and report a failure only by its error type. The stored
request is deleted when the build ends, whether it published a bundle or
failed.

## One-time setup

The Worker binds the `waypoints-territory-bundles` R2 bucket as `BUCKET` and
needs one Worker secret, set with `npx wrangler secret put GITHUB_TOKEN`: a
fine-grained GitHub token with Actions read and write access to this
repository.

This repository needs three Actions secrets from an R2 API token with Object
Read and Write access to the bundle bucket only:

- `R2_ACCOUNT_ID`
- `R2_ACCESS_KEY_ID`
- `R2_SECRET_ACCESS_KEY`

Deploy the Worker with `npx wrangler deploy` from this directory.

`MAX_DAILY_BUILDS` caps new builds per UTC day at 500, which only guards the
public Worker against abuse. Requests for one tile are deduplicated for six
hours, and existing bundles are always served.

## Building locally

```
pip install -r requirements.txt
python build_bundle.py --latitude 48.8584 --longitude 2.2945 --output bundle.json
```

A local build writes the bundle to a file instead of publishing it and prints
which areas it considered for the place.
