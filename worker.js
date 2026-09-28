const INDEX_ZOOM = 12;
const REQUEST_COOLDOWN_MS = 6 * 60 * 60 * 1000;
const BUNDLE_REFRESH_MS = 35 * 24 * 60 * 60 * 1000;
const DEFAULT_DAILY_BUILD_LIMIT = 25;
// A new city takes under a minute to build, and the app waits on it with the
// sheet open, so it asks again soon enough to show the bundle within seconds
// of it landing. Each ask is one small index read.
const DEFAULT_RETRY_AFTER_SECONDS = 5;
// A bundle of an older builder revision is served with this Retry-After while
// its rebuild runs, so an app that already holds it asks once more after the
// rebuild lands instead of keeping the old areas until it is relaunched.
const REBUILD_RETRY_AFTER_SECONDS = 30;
const REQUIRED_BUNDLE_REVISION = 6;
// Revision the builder writes now. An older bundle that still meets the
// required revision is served as it is and rebuilt in the background, the way
// an expired bundle is, so a builder change reaches every city without leaving
// anyone without data while it rebuilds. Keep in sync with BUNDLE_REVISION in
// build_bundle.py.
const CURRENT_BUNDLE_REVISION = 14;
// Covers the builder's CITY_MATCH_TOLERANCE_DEGREES, so a coastal request just
// past a city's land boundary finds the bundle built for it.
const CITY_MATCH_TOLERANCE_METERS = 250;
const METERS_PER_DEGREE = 111_320;
// Outlines within this share of each other's size are one place stored under
// two ids, and the newer of them is served.
const SAME_PLACE_AREA_RATIO = 1.02;

export default {
  async fetch(request, env, context) {
    if (request.method !== "GET") {
      return json({ error: "method_not_allowed" }, 405, { Allow: "GET" });
    }

    const url = new URL(request.url);
    if (url.pathname === "/health") {
      return json({ status: "ok" });
    }
    if (url.pathname !== "/v1/bundle") {
      return json({ error: "not_found" }, 404);
    }

    const latitude = coordinateParameter(url.searchParams.get("lat"));
    const longitude = coordinateParameter(url.searchParams.get("lon"));
    if (!validCoordinate(latitude, longitude)) {
      return json({ error: "invalid_coordinate" }, 400);
    }

    const tile = tileForCoordinate(latitude, longitude, INDEX_ZOOM);
    const candidate = await findCandidate(env.BUCKET, tile, latitude, longitude);
    if (
      candidate &&
      bundleRevision(candidate.version) >= REQUIRED_BUNDLE_REVISION
    ) {
      const outdated =
        bundleRevision(candidate.version) < CURRENT_BUNDLE_REVISION;
      if (
        Date.now() - Number(candidate.updatedAt || 0) > BUNDLE_REFRESH_MS ||
        outdated
      ) {
        context.waitUntil(enqueueBuild(env, tile, latitude, longitude));
      }
      const rebuildHeaders = outdated
        ? { "Retry-After": String(REBUILD_RETRY_AFTER_SECONDS) }
        : {};
      const currentVersion = url.searchParams.get("version");
      if (currentVersion && currentVersion === candidate.version) {
        return new Response(null, {
          status: 304,
          headers: corsHeaders({
            "Cache-Control": "no-store",
            ...rebuildHeaders,
          }),
        });
      }
      const object = await env.BUCKET.get(candidate.bundleKey);
      if (object) {
        const headers = new Headers(rebuildHeaders);
        object.writeHttpMetadata(headers);
        headers.set("ETag", object.httpEtag);
        headers.set("Cache-Control", "no-store");
        addCors(headers);
        return new Response(object.body, { headers });
      }
    }

    const queued = await enqueueBuild(env, tile, latitude, longitude);
    if (!queued.ok) {
      return json({ error: queued.error }, queued.status);
    }
    return json(
      { status: "building", requestKey: queued.requestKey },
      202,
      {
        "Cache-Control": "no-store",
        "Retry-After": String(DEFAULT_RETRY_AFTER_SECONDS),
      },
    );
  },
};

function bundleRevision(version) {
  const match = /-r(\d+)$/.exec(version);
  return match ? Number(match[1]) : 0;
}

async function findCandidate(bucket, tile, latitude, longitude) {
  const prefix = `index/${INDEX_ZOOM}/${tile.x}/${tile.y}/`;
  let cursor;
  const holding = [];
  let nearest = null;
  let nearestDistance = CITY_MATCH_TOLERANCE_METERS;
  do {
    const page = await bucket.list({ prefix, cursor, limit: 100 });
    for (const entry of page.objects) {
      const object = await bucket.get(entry.key);
      if (!object) continue;
      let candidate;
      try {
        candidate = await object.json();
      } catch {
        continue;
      }
      if (
        typeof candidate?.version !== "string" ||
        typeof candidate?.bundleKey !== "string"
      ) {
        continue;
      }
      if (!contains(candidate.boundary, latitude, longitude)) {
        const distance = boundaryDistance(candidate.boundary, latitude, longitude);
        if (distance <= nearestDistance) {
          nearest = candidate;
          nearestDistance = distance;
        }
        continue;
      }
      holding.push({ candidate, area: outlineArea(candidate.boundary) });
    }
    cursor = page.truncated ? page.cursor : undefined;
  } while (cursor);
  return mostSpecific(holding) || nearest;
}

// A county or region is built when a request falls outside every town, and
// its outline then holds the towns inside it. Serving the newest outline
// gave Zadar the whole of Zadar County whenever the county was built after
// the town, so the smallest outline holding the point is served. Only
// between outlines of practically one size, which are one place stored under
// an old and a new id, does the newer one win.
function mostSpecific(holding) {
  if (holding.length === 0) return null;
  const smallest = Math.min(...holding.map((entry) => entry.area));
  let chosen = null;
  for (const entry of holding) {
    if (entry.area > smallest * SAME_PLACE_AREA_RATIO) continue;
    if (
      !chosen ||
      Number(entry.candidate.updatedAt || 0) >
        Number(chosen.updatedAt || 0)
    ) {
      chosen = entry.candidate;
    }
  }
  return chosen;
}

// Area of an outline in square degrees, scaled for latitude. It only ranks
// outlines around one point against each other, so no projection is needed.
function outlineArea(boundary) {
  const ringArea = (ring) => {
    if (!Array.isArray(ring) || ring.length < 4) return 0;
    let twice = 0;
    for (let i = 0, j = ring.length - 1; i < ring.length; j = i++) {
      const [latitudeI, longitudeI] = ring[i];
      const [latitudeJ, longitudeJ] = ring[j];
      const scale = Math.cos((((latitudeI + latitudeJ) / 2) * Math.PI) / 180);
      twice += (longitudeJ - longitudeI) * scale * (latitudeJ + latitudeI);
    }
    return Math.abs(twice) / 2;
  };
  const sum = (rings) =>
    (Array.isArray(rings) ? rings : []).reduce(
      (total, ring) => total + ringArea(ring),
      0,
    );
  return sum(boundary?.outer) - sum(boundary?.inner);
}

async function enqueueBuild(env, tile, latitude, longitude) {
  const requestKey = `${INDEX_ZOOM}/${tile.x}/${tile.y}`;
  const markerKey = `requests/${requestKey}.json`;
  const existing = await env.BUCKET.get(markerKey);
  if (existing) {
    try {
      const marker = await existing.json();
      const age = Date.now() - Date.parse(marker.requestedAt);
      if (Number.isFinite(age) && age < REQUEST_COOLDOWN_MS) {
        return { ok: true, requestKey };
      }
    } catch {
      // A malformed marker is replaced by a fresh request below.
    }
  }

  const date = new Date().toISOString().slice(0, 10);
  const dailyPrefix = `requests-by-day/${date}/`;
  const limit = positiveInteger(env.MAX_DAILY_BUILDS, DEFAULT_DAILY_BUILD_LIMIT);
  const daily = await env.BUCKET.list({ prefix: dailyPrefix, limit: limit + 1 });
  if (daily.objects.length >= limit) {
    return { ok: false, status: 429, error: "daily_build_limit_reached" };
  }

  if (!env.GITHUB_TOKEN || !env.GITHUB_REPOSITORY) {
    return { ok: false, status: 503, error: "builder_not_configured" };
  }

  const requestedAt = new Date().toISOString();
  const request = JSON.stringify({
    requestKey,
    latitude,
    longitude,
    requestedAt,
  });
  // Only the build request carries the coordinate, and the builder deletes
  // it when the build ends. The cooldown and daily-count records need only
  // the time, so they hold nothing about where anyone was, and the bucket's
  // lifecycle rules delete all request records after a day.
  const marker = JSON.stringify({ requestedAt });
  const metadata = { httpMetadata: { contentType: "application/json" } };
  // The builder's run logs are public, so the coordinate stays in the
  // bucket and the build is told only an opaque id to read it by.
  const requestId = crypto.randomUUID();
  await env.BUCKET.put(`requests/by-id/${requestId}.json`, request, metadata);
  const dispatched = await dispatchBuild(env, requestId);
  if (!dispatched) {
    await env.BUCKET.delete(`requests/by-id/${requestId}.json`);
    return { ok: false, status: 503, error: "builder_unavailable" };
  }

  await Promise.all([
    env.BUCKET.put(markerKey, marker, metadata),
    env.BUCKET.put(`${dailyPrefix}${requestId}.json`, marker, metadata),
  ]);
  return { ok: true, requestKey };
}

async function dispatchBuild(env, requestId) {
  const workflow = env.GITHUB_WORKFLOW || "territory-bundle.yml";
  const ref = env.GITHUB_REF || "main";
  const response = await fetch(
    `https://api.github.com/repos/${env.GITHUB_REPOSITORY}/actions/workflows/${workflow}/dispatches`,
    {
      method: "POST",
      headers: {
        Accept: "application/vnd.github+json",
        Authorization: `Bearer ${env.GITHUB_TOKEN}`,
        "User-Agent": "waypoints-territory-bundles",
        "X-GitHub-Api-Version": "2022-11-28",
      },
      body: JSON.stringify({
        ref,
        inputs: {
          request_id: requestId,
        },
      }),
    },
  );
  return response.status === 204;
}

function contains(boundary, latitude, longitude) {
  if (!boundary || !Array.isArray(boundary.outer)) return false;
  const inOuter = boundary.outer.some((ring) => pointInRing(ring, latitude, longitude));
  if (!inOuter) return false;
  return !(boundary.inner || []).some((ring) =>
    pointInRing(ring, latitude, longitude),
  );
}

function boundaryDistance(boundary, latitude, longitude) {
  if (!boundary || !Array.isArray(boundary.outer)) return Infinity;
  const scale = Math.cos((latitude * Math.PI) / 180);
  let best = Infinity;
  for (const ring of boundary.outer) {
    if (!Array.isArray(ring)) continue;
    for (let i = 1; i < ring.length; i++) {
      const first = ring[i - 1];
      const second = ring[i];
      if (!Array.isArray(first) || !Array.isArray(second)) continue;
      const ax = (Number(first[1]) - longitude) * scale;
      const ay = Number(first[0]) - latitude;
      const bx = (Number(second[1]) - longitude) * scale;
      const by = Number(second[0]) - latitude;
      const dx = bx - ax;
      const dy = by - ay;
      const length = dx * dx + dy * dy;
      const t = length > 0 ? Math.min(1, Math.max(0, -(ax * dx + ay * dy) / length)) : 0;
      best = Math.min(best, Math.hypot(ax + t * dx, ay + t * dy));
    }
  }
  return best * METERS_PER_DEGREE;
}

function pointInRing(ring, latitude, longitude) {
  if (!Array.isArray(ring) || ring.length < 4) return false;
  let inside = false;
  for (let i = 0, j = ring.length - 1; i < ring.length; j = i++) {
    const first = ring[i];
    const second = ring[j];
    if (!Array.isArray(first) || !Array.isArray(second)) continue;
    const yi = Number(first[0]);
    const xi = Number(first[1]);
    const yj = Number(second[0]);
    const xj = Number(second[1]);
    const crosses =
      yi > latitude !== yj > latitude &&
      longitude < ((xj - xi) * (latitude - yi)) / (yj - yi) + xi;
    if (crosses) inside = !inside;
  }
  return inside;
}

function tileForCoordinate(latitude, longitude, zoom) {
  const count = 2 ** zoom;
  const x = Math.min(
    count - 1,
    Math.max(0, Math.floor(((longitude + 180) / 360) * count)),
  );
  const bounded = Math.min(85.0511287798066, Math.max(-85.0511287798066, latitude));
  const radians = (bounded * Math.PI) / 180;
  const y = Math.min(
    count - 1,
    Math.max(
      0,
      Math.floor(((1 - Math.log(Math.tan(radians) + 1 / Math.cos(radians)) / Math.PI) / 2) * count),
    ),
  );
  return { x, y };
}

// Number() reads a missing or blank parameter as 0, which turned a request
// without a coordinate into a build for 0, 0, where no city exists.
function coordinateParameter(value) {
  return value === null || value.trim() === "" ? Number.NaN : Number(value);
}

function validCoordinate(latitude, longitude) {
  return (
    Number.isFinite(latitude) &&
    Number.isFinite(longitude) &&
    latitude >= -90 &&
    latitude <= 90 &&
    longitude >= -180 &&
    longitude <= 180
  );
}

function positiveInteger(value, fallback) {
  const parsed = Number.parseInt(value || "", 10);
  return Number.isInteger(parsed) && parsed > 0 ? parsed : fallback;
}

function json(body, status = 200, extraHeaders = {}) {
  return new Response(JSON.stringify(body), {
    status,
    headers: corsHeaders({
      "Content-Type": "application/json; charset=utf-8",
      ...extraHeaders,
    }),
  });
}

function corsHeaders(values) {
  const headers = new Headers(values);
  addCors(headers);
  return headers;
}

function addCors(headers) {
  headers.set("Access-Control-Allow-Origin", "*");
}
