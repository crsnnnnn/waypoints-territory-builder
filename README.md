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
Where the remaining outlines cover less than half a place, the ground they
leave open is split between the place's named neighbourhood points along its
main roads, rail lines and large water, so its explorers still get named
local areas. Bundles older than 35 days, or built under an older revision, keep being
served while a fresh one builds in the background.

## Privacy of public logs

This repository is public so that GitHub Actions minutes cost nothing, which
makes its run logs public too. The Worker therefore never passes a coordinate
to the workflow. It stores the request in the private bucket under a random id
and passes only that id. Builds started that way print no coordinate, search
box, or place name, and report a failure only by its error type.

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
