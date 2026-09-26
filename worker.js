const INDEX_ZOOM = 12;
const REQUEST_COOLDOWN_MS = 6 * 60 * 60 * 1000;
const BUNDLE_REFRESH_MS = 35 * 24 * 60 * 60 * 1000;
const DEFAULT_DAILY_BUILD_LIMIT = 25;
const DEFAULT_RETRY_AFTER_SECONDS = 20;
const REQUIRED_BUNDLE_REVISION = 6;
// Revision the builder writes now. An older bundle that still meets the
// required revision is served as it is and rebuilt in the background, the way
// an expired bundle is, so a builder change reaches every city without leaving
// anyone without data while it rebuilds. Keep in sync with BUNDLE_REVISION in
// build_bundle.py.
const CURRENT_BUNDLE_REVISION = 7;
// Covers the builder's CITY_MATCH_TOLERANCE_DEGREES, so a coastal request just
// past a city's land boundary finds the bundle built for it.
const CITY_MATCH_TOLERANCE_METERS = 250;
const METERS_PER_DEGREE = 111_320;

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

    const latitude = Number(url.searchParams.get("lat"));
    const longitude = Number(url.searchParams.get("lon"));
    if (!validCoordinate(latitude, longitude)) {
      return json({ error: "invalid_coordinate" }, 400);
    }

    const tile = tileForCoordinate(latitude, longitude, INDEX_ZOOM);
    const candidate = await findCandidate(env.BUCKET, tile, latitude, longitude);
    if (
      candidate &&
      bundleRevision(candidate.version) >= REQUIRED_BUNDLE_REVISION
    ) {
      if (
        Date.now() - Number(candidate.updatedAt || 0) > BUNDLE_REFRESH_MS ||
        bundleRevision(candidate.version) < CURRENT_BUNDLE_REVISION
      ) {
        context.waitUntil(enqueueBuild(env, tile, latitude, longitude));
      }
      const currentVersion = url.searchParams.get("version");
      if (currentVersion && currentVersion === candidate.version) {
        return new Response(null, {
          status: 304,
          headers: corsHeaders({ "Cache-Control": "no-store" }),
        });
      }
      const object = await env.BUCKET.get(candidate.bundleKey);
      if (object) {
        const headers = new Headers();
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
  let newest = null;
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
      const updatedAt = Number(candidate.updatedAt || 0);
      const newestUpdatedAt = Number(newest?.updatedAt || 0);
      if (!newest || updatedAt > newestUpdatedAt) {
        newest = candidate;
      }
    }
    cursor = page.truncated ? page.cursor : undefined;
  } while (cursor);
  return newest || nearest;
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

  const marker = JSON.stringify({
    requestKey,
    latitude,
    longitude,
    requestedAt: new Date().toISOString(),
  });
  const metadata = { httpMetadata: { contentType: "application/json" } };
  // The builder's run logs are public, so the coordinate stays in the
  // bucket and the build is told only an opaque id to read it by.
  const requestId = crypto.randomUUID();
  await env.BUCKET.put(`requests/by-id/${requestId}.json`, marker, metadata);
  const dispatched = await dispatchBuild(env, requestId);
  if (!dispatched) {
    await env.BUCKET.delete(`requests/by-id/${requestId}.json`);
    return { ok: false, status: 503, error: "builder_unavailable" };
  }

  await Promise.all([
    env.BUCKET.put(markerKey, marker, metadata),
    env.BUCKET.put(`${dailyPrefix}${tile.x}-${tile.y}.json`, marker, metadata),
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
