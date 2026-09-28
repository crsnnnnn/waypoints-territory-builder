#!/usr/bin/env python3
"""Build one immutable Waypoints territory bundle from mapped source data."""

from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import math
import os
import re
import subprocess
import sys
import tempfile
import time
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any, Iterable

import boto3
import shapely
from shapely.affinity import scale
from shapely.geometry import LineString, MultiLineString, MultiPoint, Point, box, mapping, shape
from shapely.geometry.base import BaseGeometry
from shapely.ops import unary_union
from shapely.strtree import STRtree

CELL_ZOOM = 20
INDEX_ZOOM = 12
SAMPLE_SPACING_METERS = 25.0
EARTH_RADIUS_METERS = 6_371_008.8
MAX_MERCATOR_LATITUDE = 85.0511287798066
MAX_INDEX_TILES = 2_048
CITY_SEARCH_RADIUS_DEGREES = 0.5
# Coastal requests can land just past a simplified land polygon, such as on a
# harbour quay. Keep in sync with CITY_MATCH_TOLERANCE_METERS in worker.js.
CITY_MATCH_TOLERANCE_DEGREES = 0.002
BOUNDARY_SIMPLIFICATION_DEGREES = 0.00002
# Keep in sync with CURRENT_BUNDLE_REVISION in worker.js. Revision 8 names the
# country and region each place belongs to.
BUNDLE_REVISION = 8
# Division subtypes that stand for the region a place is grouped under, most
# fitting first. Countries without Overture regions, such as Slovenia, group
# their places by the next level down instead of leaving them unnamed.
REGION_SUBTYPES = ("region", "macroregion", "macrocounty", "county")
OVERPASS_QUERY_TIMEOUT_SECONDS = 180
OVERPASS_ATTEMPT_TIMEOUT_SECONDS = 210
OVERPASS_MAX_RESPONSE_BYTES = 128 * 1024 * 1024
OVERPASS_ENDPOINTS = (
    "https://overpass.openstreetmap.fr/api/interpreter",
    "https://overpass.private.coffee/api/interpreter",
    "https://overpass-api.de/api/interpreter",
)
NEIGHBORHOOD_SUBTYPES = ("macrohood", "neighborhood", "microhood")
# Settlements that split a place with no mapped subdivisions, such as the
# villages of a Romanian commune. Hamlets are left out, because a hamlet's
# share would cut a village's fields in two.
SETTLEMENT_CLASSES = ("city", "town", "village")
# Overture classes of a locality that is a settlement in its own right. A
# locality with no class is a part of a city, such as Berlin's Mitte, a
# Bucharest sector, or Manhattan.
TOWN_CLASSES = ("city", "town")
VILLAGE_CLASSES = ("village", "hamlet")
# Area types a place can be at the council level.
MUNICIPAL_SUBTYPES = ("localadmin", "county")
# Area types a place's parts can be. Overture types the same level
# differently from country to country and even within one city: Paris's
# arrondissements are neighborhoods but its 13e a macrohood, Madrid's
# distritos macrohoods but Centro a neighborhood, Berlin's Bezirke and New
# York's boroughs classless localities. So the types are pooled.
SUBDIVISION_TIERS = ("borough", "locality", "macrohood", "neighborhood", "microhood")
# Share of a candidate that may already be covered by chosen subdivisions.
SUBDIVISION_MAX_OVERLAP = 0.15
# Share of a part inside another chosen part that makes it a piece of that
# part rather than a part of its own.
SUBDIVISION_NESTED_SHARE = 0.8
# Share of the place the parts must cover to stand for it.
SUBDIVISION_MIN_COVERAGE = 0.6
# How many parts a list can hold. A set of hundreds is street blocks or
# land registry plots, not districts.
SUBDIVISION_MIN_PARTS = 2
SUBDIVISION_MAX_PARTS = 60
# Smallest subdivision kept, as a share of the place, so a gap is never
# filled with a single square or a street corner.
SUBDIVISION_MIN_SHARE = 0.002
# Neighbouring places listed beside a place with no parts of its own.
NEARBY_PLACES_MAXIMUM = 12
# Set for builds started by the worker. Their logs are public, so they print
# nothing that could place the explorer who asked: no coordinates, no search
# box, no place names, and an error only by its type.
PUBLIC_LOG = False


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--request-id",
        help="Build the request the worker stored under this id in the bucket",
    )
    parser.add_argument("--latitude", type=float)
    parser.add_argument("--longitude", type=float)
    parser.add_argument("--request-key", help="Request marker to clear after publishing")
    parser.add_argument(
        "--output",
        type=Path,
        help="Write the bundle to this file instead of publishing it",
    )
    args = parser.parse_args()
    if args.request_id is None and (args.latitude is None or args.longitude is None):
        parser.error("either --request-id or --latitude and --longitude is required")
    return args


def load_request(request_id: str) -> tuple[float, float, str]:
    """The coordinate and marker of a request the worker stored privately.

    The worker passes the build only an opaque id, because workflow inputs
    show in public run logs. The coordinate stays in the private bucket.
    """
    if not re.fullmatch(r"[0-9a-f-]{36}", request_id):
        raise ValueError("Request id is malformed")
    response = r2_client().get_object(
        Bucket=required_environment("R2_BUCKET"),
        Key=f"requests/by-id/{request_id}.json",
    )
    record = json.loads(response["Body"].read())
    latitude = float(record["latitude"])
    longitude = float(record["longitude"])
    request_key = str(record["requestKey"])
    return latitude, longitude, request_key


def main() -> None:
    global PUBLIC_LOG
    args = parse_args()
    if args.request_id is not None:
        PUBLIC_LOG = True
        args.latitude, args.longitude, args.request_key = load_request(args.request_id)
    validate_coordinate(args.latitude, args.longitude)
    release = latest_release()
    with tempfile.TemporaryDirectory(prefix="waypoints-territory-") as directory:
        work = Path(directory)
        city_feature, search_areas, hierarchy = find_city(
            args.latitude,
            args.longitude,
            work,
        )
        city_geometry = valid_geometry(city_feature)
        city_bounds = tuple(city_geometry.bounds)

        divisions = download_features("division_area", city_bounds, work / "divisions.geojson")
        divisions = deduplicate_divisions(divisions)
        districts = place_parts(divisions, hierarchy, city_feature, city_geometry)
        nearby = nearby_places(search_areas, hierarchy, city_feature, city_geometry)
        neighborhoods = select_areas(
            divisions,
            city_geometry,
            NEIGHBORHOOD_SUBTYPES,
            maximum=250,
        )
        if not districts:
            settlements = settlement_areas(city_feature, city_geometry, hierarchy)
            districts = settlements or [synthetic_district(city_feature)]
            # A settlement is also the explorer's local area. The app measures
            # the smallest outline holding the fix, so a mapped neighbourhood
            # inside a village still wins over the village.
            neighborhoods = [*neighborhoods, *settlements]

        coverage_geometry = unary_union(
            [
                city_geometry,
                *(valid_geometry(area) for area in districts),
                *(valid_geometry(area) for area in neighborhoods),
            ]
        )
        roads = download_named_roads(coverage_geometry)
        water = download_water(coverage_geometry.bounds, work / "water.geojson")
        bundle = make_bundle(
            release=release,
            city=city_feature,
            districts=districts,
            neighborhoods=neighborhoods,
            roads=roads,
            water=water,
            nearby=nearby,
            place=place_hierarchy(city_feature, hierarchy),
        )
        if args.output is not None:
            args.output.write_text(
                json.dumps(bundle, ensure_ascii=False, separators=(",", ":")),
                encoding="utf-8",
            )
            return
        publish_bundle(
            bundle=bundle,
            city_feature=city_feature,
            request_key=args.request_key,
            request_id=args.request_id,
            representative=(args.latitude, args.longitude),
        )
        if PUBLIC_LOG:
            print("Bundle published")


def validate_coordinate(latitude: float, longitude: float) -> None:
    if not (-90 <= latitude <= 90 and -180 <= longitude <= 180):
        raise ValueError("Coordinate is outside the valid latitude or longitude range")


def latest_release() -> str:
    with urllib.request.urlopen(
        "https://stac.overturemaps.org/catalog.json", timeout=30
    ) as response:
        catalog = json.load(response)
    release = catalog.get("latest")
    if not isinstance(release, str) or not release:
        raise RuntimeError("Overture STAC catalog did not provide a latest release")
    return release.rstrip("/")


def find_city(
    latitude: float,
    longitude: float,
    work: Path,
) -> tuple[dict[str, Any], list[dict[str, Any]], list[dict[str, Any]]]:
    """The place the explorer stands in, with the areas and divisions read.

    Returns the place's area, every area read around it, which the nearby
    places are picked from, and the division records, which carry the class
    of each settlement.
    """
    radius = CITY_SEARCH_RADIUS_DEGREES
    bounds = (
        max(-180.0, longitude - radius),
        max(-90.0, latitude - radius),
        min(180.0, longitude + radius),
        min(90.0, latitude + radius),
    )
    features = deduplicate_divisions(
        download_features("division_area", bounds, work / "city-search.geojson")
    )
    divisions = download_features(
        "division",
        bounds,
        work / "city-hierarchy.geojson",
    )
    point = Point(longitude, latitude)
    for tolerance in (0.0, CITY_MATCH_TOLERANCE_DEGREES):
        match = choose_place(features, divisions, point, tolerance)
        if match is not None:
            return match, features, divisions
    raise RuntimeError("No Overture city division contains the requested coordinate")


def division_classes(divisions: Iterable[dict[str, Any]]) -> dict[str, str]:
    """The settlement class Overture gives each division, by division id."""
    classes: dict[str, str] = {}
    for division in divisions:
        properties = division.get("properties") or {}
        division_id = division.get("id") or properties.get("id")
        value = properties.get("class")
        if isinstance(division_id, str) and isinstance(value, str):
            classes[division_id] = value
    return classes


def place_rank(feature: dict[str, Any], classes: dict[str, str]) -> int | None:
    """How readily an area stands for the place around it, or None when never.

    Cities and towns come first, so Madrid is chosen over the Comunidad de
    Madrid and New York over New York County. Villages, local councils, and
    counties such as a Romanian commune come next. Regions, such as Berlin
    or Vienna, only when nothing smaller holds the explorer. A locality with
    no class is a part of a city, such as Mitte or Manhattan, never the place.
    """
    properties = feature.get("properties") or {}
    subtype = properties.get("subtype")
    if subtype == "locality":
        value = classes.get(properties.get("division_id") or "")
        if value in TOWN_CLASSES:
            return 0
        if value in VILLAGE_CLASSES:
            return 1
        return None
    if subtype in MUNICIPAL_SUBTYPES:
        return 1
    if subtype == "region":
        return 2
    return None


def choose_place(
    features: list[dict[str, Any]],
    divisions: list[dict[str, Any]],
    point: Point,
    tolerance: float,
) -> dict[str, Any] | None:
    """The area that stands for the place at [point], or None when none does.

    The best rank wins, then the area nearest the point, then the smallest.
    A locality wins a tie in area over a county or council drawn with the
    same outline, as Paris is all three.
    """
    classes = division_classes(divisions)
    candidates: list[tuple[int, float, float, int, dict[str, Any]]] = []
    diagnostics: list[dict[str, Any]] = []
    for feature in features:
        properties = feature.get("properties") or {}
        rank = place_rank(feature, classes)
        if rank is None:
            continue
        geometry = valid_geometry(feature, required=False)
        if geometry is None:
            continue
        distance = geometry.distance(point)
        if distance > tolerance:
            continue
        land_penalty = 0 if properties.get("is_land") is True else 1
        subtype_order = 0 if properties.get("subtype") == "locality" else 1
        diagnostics.append(
            {
                "name": primary_name(properties),
                "subtype": properties.get("subtype"),
                "class": classes.get(properties.get("division_id") or ""),
                "rank": rank,
            }
        )
        candidates.append(
            (rank, distance, geometry.area, land_penalty * 2 + subtype_order, feature)
        )
    if not PUBLIC_LOG:
        print(
            f"Overture places within {tolerance} degrees: "
            + json.dumps(diagnostics, ensure_ascii=False, separators=(",", ":"))
        )
    if not candidates:
        return None
    candidates.sort(key=lambda value: (value[0], value[1], value[2], value[3]))
    return candidates[0][4]


def download_features(
    feature_type: str,
    bounds: tuple[float, float, float, float],
    output: Path,
    *,
    allow_empty: bool = False,
) -> list[dict[str, Any]]:
    bbox = ",".join(f"{value:.8f}" for value in bounds)
    subprocess.run(
        [
            "overturemaps",
            "download",
            f"--bbox={bbox}",
            "-f",
            "geojson",
            f"--type={feature_type}",
            # The STAC file index in overturemaps 1.0.2 matches no files for
            # current releases, so the CLI writes nothing and still exits 0.
            "--no-stac",
            "-o",
            str(output),
        ],
        check=True,
        # The CLI can echo its arguments, the search box among them.
        stdout=subprocess.DEVNULL if PUBLIC_LOG else None,
        stderr=subprocess.DEVNULL if PUBLIC_LOG else None,
    )
    if not output.exists():
        if allow_empty:
            return []
        raise RuntimeError(
            f"Overture {feature_type} download wrote no data for bbox {bbox}"
        )
    with output.open("r", encoding="utf-8") as source:
        payload = json.load(source)
    if payload.get("type") != "FeatureCollection":
        raise RuntimeError(f"Overture {feature_type} download was not GeoJSON")
    return [feature for feature in payload.get("features", []) if isinstance(feature, dict)]


def valid_geometry(
    feature: dict[str, Any], *, required: bool = True
) -> BaseGeometry | None:
    raw = feature.get("geometry")
    if not raw:
        if required:
            raise RuntimeError("Overture feature has no geometry")
        return None
    geometry = shape(raw)
    if not geometry.is_valid:
        geometry = geometry.buffer(0)
    if geometry.is_empty:
        if required:
            raise RuntimeError("Overture feature has empty geometry")
        return None
    return geometry


def deduplicate_divisions(features: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    selected: dict[str, dict[str, Any]] = {}
    for feature in features:
        feature_id = str(feature.get("id") or (feature.get("properties") or {}).get("id") or "")
        if not feature_id:
            continue
        current = selected.get(feature_id)
        properties = feature.get("properties") or {}
        if current is None or (
            properties.get("is_land") is True
            and (current.get("properties") or {}).get("is_land") is not True
        ):
            selected[feature_id] = feature
    return list(selected.values())


def select_areas(
    features: Iterable[dict[str, Any]],
    city_geometry: BaseGeometry,
    subtypes: tuple[str, ...],
    *,
    maximum: int | None = None,
) -> list[dict[str, Any]]:
    selected: list[tuple[float, dict[str, Any]]] = []
    for feature in features:
        properties = feature.get("properties") or {}
        if properties.get("subtype") not in subtypes or not primary_name(properties):
            continue
        geometry = valid_geometry(feature, required=False)
        if geometry is None or not city_geometry.intersects(geometry):
            continue
        overlap = city_geometry.intersection(geometry).area
        if overlap <= 0 or overlap / max(geometry.area, 1e-15) < 0.5:
            continue
        selected.append((overlap, feature))
    selected.sort(key=lambda item: (-item[0], primary_name(item[1].get("properties") or {})))
    values = [feature for _, feature in selected]
    return values if maximum is None else values[:maximum]


def synthetic_district(city: dict[str, Any]) -> dict[str, Any]:
    city_id = feature_id(city)
    return {
        "id": f"{city_id}-citywide",
        "type": "Feature",
        "geometry": city["geometry"],
        "properties": {
            "subtype": "borough",
            "names": {"primary": primary_name(city.get("properties") or {})},
            "synthetic": True,
        },
    }


def division_populations(divisions: Iterable[dict[str, Any]]) -> dict[str, int]:
    """The population Overture records for each division, by division id."""
    populations: dict[str, int] = {}
    for division in divisions:
        properties = division.get("properties") or {}
        division_id = division.get("id") or properties.get("id")
        value = properties.get("population")
        if isinstance(division_id, str) and isinstance(value, (int, float)) and value > 0:
            populations[division_id] = int(value)
    return populations


def place_parts(
    features: Iterable[dict[str, Any]],
    divisions: Iterable[dict[str, Any]],
    city: dict[str, Any],
    city_geometry: BaseGeometry,
) -> list[dict[str, Any]]:
    """The areas that split a place into its districts.

    Areas of every type inside the place are pooled and taken largest first,
    skipping any that mostly overlaps one already taken. Areas that record a
    population go first: official districts carry one and informal areas
    rarely do, so Paris keeps its arrondissements over Le Marais, which is
    larger than the 3e it covers, and Vienna keeps its Bezirke over land
    registry plots. An area left sitting inside another chosen area is a
    piece of it and is dropped. When that set does not stand for the place,
    the areas are taken by size alone. Returns nothing when neither set
    covers enough of the place in a sensible number of parts.
    """
    city_properties = city.get("properties") or {}
    city_division_id = city_properties.get("division_id")
    city_area = city_geometry.area
    if city_area <= 0:
        return []
    populations = division_populations(divisions)
    candidates: list[tuple[bool, float, BaseGeometry, dict[str, Any]]] = []
    for feature in features:
        properties = feature.get("properties") or {}
        if (
            properties.get("subtype") not in SUBDIVISION_TIERS
            or properties.get("division_id") == city_division_id
            or not primary_name(properties)
        ):
            continue
        geometry = valid_geometry(feature, required=False)
        if (
            geometry is None
            or geometry.area <= 0
            or not city_geometry.intersects(geometry)
        ):
            continue
        inside = city_geometry.intersection(geometry).area
        # The place itself under another type, as Paris is also a county.
        if inside >= 0.95 * city_area:
            continue
        if inside < SUBDIVISION_MIN_SHARE * city_area or inside / geometry.area < 0.9:
            continue
        counted = properties.get("division_id") in populations
        candidates.append((counted, geometry.area, geometry, feature))

    def tile(order: list[tuple[bool, float, BaseGeometry, dict[str, Any]]]):
        chosen: list[tuple[float, BaseGeometry, dict[str, Any]]] = []
        covered: BaseGeometry | None = None
        for _, area, geometry, feature in order:
            if covered is not None:
                if covered.intersection(geometry).area > SUBDIVISION_MAX_OVERLAP * area:
                    continue
            chosen.append((area, geometry, feature))
            covered = geometry if covered is None else covered.union(geometry)
        kept = [
            part
            for part in chosen
            if not any(
                other is not part
                and other[0] > part[0]
                and other[1].intersection(part[1]).area
                >= SUBDIVISION_NESTED_SHARE * part[0]
                for other in chosen
            )
        ]
        if not kept:
            return None
        union = unary_union([geometry for _, geometry, _ in kept])
        coverage = union.intersection(city_geometry).area / city_area
        if (
            coverage < SUBDIVISION_MIN_COVERAGE
            or not SUBDIVISION_MIN_PARTS <= len(kept) <= SUBDIVISION_MAX_PARTS
        ):
            return None
        return [feature for _, _, feature in kept]

    orders = (
        sorted(candidates, key=lambda item: (not item[0], -item[1])),
        sorted(candidates, key=lambda item: -item[1]),
    )
    for order in orders:
        parts = tile(order)
        if parts:
            return sorted(
                parts,
                key=lambda feature: primary_name(feature.get("properties") or {}),
            )
    return []


def nearby_places(
    features: Iterable[dict[str, Any]],
    divisions: Iterable[dict[str, Any]],
    city: dict[str, Any],
    city_geometry: BaseGeometry,
) -> list[dict[str, Any]]:
    """Places of a like size that border the explorer's place, nearest first.

    A village with no districts has nothing of its own to list, so the app
    lists the towns and villages around it instead. A region such as Berlin
    lists none, and a village never lists a whole county.
    """
    classes = division_classes(divisions)
    city_properties = city.get("properties") or {}
    city_rank = place_rank(city, classes)
    if city_rank is None or city_rank >= 2:
        return []
    city_area = city_geometry.area
    center = city_geometry.representative_point()
    touching = city_geometry.buffer(CITY_MATCH_TOLERANCE_DEGREES)
    found: dict[str, tuple[float, dict[str, Any]]] = {}
    for feature in features:
        properties = feature.get("properties") or {}
        name = primary_name(properties)
        if (
            not name
            or name == primary_name(city_properties)
            or properties.get("division_id") == city_properties.get("division_id")
            or properties.get("is_land") is not True
        ):
            continue
        rank = place_rank(feature, classes)
        if rank is None or rank >= 2:
            continue
        # Neighbours are the same kind of area, so Paris lists Boulogne and
        # Saint-Denis, never the Hauts-de-Seine department around them.
        if properties.get("subtype") != city_properties.get("subtype"):
            continue
        geometry = valid_geometry(feature, required=False)
        if geometry is None or not touching.intersects(geometry):
            continue
        if not (city_area / 20 <= geometry.area <= city_area * 20):
            continue
        # A place that overlaps this one is a layer of it, not a neighbour.
        overlap = city_geometry.intersection(geometry).area
        if overlap > 0.1 * min(city_area, geometry.area):
            continue
        distance = geometry.distance(center)
        if name not in found or distance < found[name][0]:
            found[name] = (distance, feature)
    nearest = sorted(found.values(), key=lambda item: item[0])
    return [feature for _, feature in nearest[:NEARBY_PLACES_MAXIMUM]]


def settlement_areas(
    city: dict[str, Any],
    city_geometry: BaseGeometry,
    divisions: Iterable[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Splits a place with no mapped subdivisions between its settlements.

    Overture maps the villages of a commune as points, with no outline of
    their own. Each point takes the part of the place that lies closer to it
    than to any other settlement, so the villages cover the whole place with
    no gaps or overlaps. Returns nothing when fewer than two settlements lie
    inside the place, since one settlement would only repeat the place.
    """
    city_division_id = (city.get("properties") or {}).get("division_id")
    settlements: dict[str, tuple[Point, dict[str, Any]]] = {}
    for division in divisions:
        properties = division.get("properties") or {}
        name = primary_name(properties)
        if (
            properties.get("subtype") != "locality"
            or properties.get("class") not in SETTLEMENT_CLASSES
            or not name
        ):
            continue
        geometry = valid_geometry(division, required=False)
        if not isinstance(geometry, Point) or not city_geometry.contains(geometry):
            continue
        # Two points can carry one name, such as a village and its station.
        # The one the place lists as its own child is kept.
        is_child = properties.get("parent_division_id") == city_division_id
        if name in settlements and not is_child:
            continue
        settlements[name] = (geometry, division)
    if len(settlements) < 2:
        return []
    # A city point inside the area, as London inside Westminster, marks an
    # urban council rather than a rural commune of villages.
    if any(
        (settlement[1].get("properties") or {}).get("class") == "city"
        for settlement in settlements.values()
    ):
        return []

    # Degrees of longitude shrink toward the poles. Scaling them by the cosine
    # of the latitude keeps "closer" meaning closer on the ground.
    latitude = city_geometry.centroid.y
    squeeze = max(math.cos(math.radians(latitude)), 0.01)
    names = list(settlements)
    projected_points = MultiPoint(
        [(settlements[name][0].x * squeeze, settlements[name][0].y) for name in names]
    )
    projected_city = scale(city_geometry, xfact=squeeze, yfact=1, origin=(0, 0))
    cells = shapely.voronoi_polygons(
        projected_points,
        extend_to=projected_city.envelope.buffer(1.0),
        ordered=True,
    )
    areas: list[dict[str, Any]] = []
    for name, cell in zip(names, cells.geoms):
        ground = scale(cell, xfact=1 / squeeze, yfact=1, origin=(0, 0))
        polygons = [
            part
            for part in polygons_of(ground.intersection(city_geometry))
            if not part.is_empty
        ]
        if not polygons:
            continue
        division = settlements[name][1]
        properties = division.get("properties") or {}
        areas.append(
            {
                "id": f"{feature_id(division)}-settlement",
                "type": "Feature",
                "geometry": mapping(unary_union(polygons)),
                "properties": {
                    "subtype": "locality",
                    "class": properties.get("class"),
                    "names": {"primary": name},
                    "synthetic": True,
                },
            }
        )
    areas.sort(key=lambda area: primary_name(area["properties"]))
    return areas if len(areas) >= 2 else []


def download_water(
    bounds: tuple[float, float, float, float],
    output: Path,
) -> BaseGeometry | None:
    """Every mapped body of water around the place, as one shape.

    Lakes and rivers inside a boundary are ground nobody walks, so the bundle
    reports how much of each area they cover and the app leaves it out of the
    ground still to explore.
    """
    features = download_features("water", bounds, output, allow_empty=True)
    shapes = []
    for feature in features:
        geometry = valid_geometry(feature, required=False)
        if geometry is not None and geometry.geom_type in ("Polygon", "MultiPolygon"):
            shapes.append(geometry)
    if not shapes:
        return None
    return unary_union(shapes)


def water_square_meters(area: BaseGeometry, water: BaseGeometry | None) -> float:
    if water is None or not area.intersects(water):
        return 0.0
    return sum(
        polygon_square_meters(polygon)
        for polygon in polygons_of(area.intersection(water))
    )


def polygons_of(geometry: BaseGeometry) -> Iterable[BaseGeometry]:
    if geometry.geom_type == "Polygon":
        yield geometry
    elif hasattr(geometry, "geoms"):
        for child in geometry.geoms:
            yield from polygons_of(child)


def polygon_square_meters(polygon: BaseGeometry) -> float:
    outer = ring_square_meters(polygon.exterior.coords)
    inner = sum(ring_square_meters(ring.coords) for ring in polygon.interiors)
    return max(outer - inner, 0.0)


def ring_square_meters(coordinates: Iterable[tuple[float, ...]]) -> float:
    """Area of one closed ring on the same sphere the app measures with.

    This mirrors AdministrativeBoundary._ringSquareMeters in the app, so the
    water figure subtracts cleanly from the boundary area the app computes.
    """
    points = list(coordinates)
    if len(points) < 4:
        return 0.0
    area = 0.0
    for start, end in zip(points, points[1:]):
        longitude_delta = math.radians(end[0] - start[0])
        if longitude_delta > math.pi:
            longitude_delta -= 2 * math.pi
        elif longitude_delta < -math.pi:
            longitude_delta += 2 * math.pi
        area += longitude_delta * (
            2 + math.sin(math.radians(start[1])) + math.sin(math.radians(end[1]))
        )
    return abs(area * EARTH_RADIUS_METERS * EARTH_RADIUS_METERS / 2)


def download_named_roads(
    city_geometry: BaseGeometry,
) -> list[tuple[str, BaseGeometry]]:
    west, south, east, north = city_geometry.bounds
    query = (
        f"[out:json][timeout:{OVERPASS_QUERY_TIMEOUT_SECONDS}];"
        f'way({south:.8f},{west:.8f},{north:.8f},{east:.8f})'
        '["highway"]["name"];out tags geom;'
    )
    encoded = urllib.parse.urlencode({"data": query}).encode("utf-8")
    failure: Exception | None = None
    for endpoint in OVERPASS_ENDPOINTS:
        request = urllib.request.Request(
            endpoint,
            data=encoded,
            headers={
                "Accept": "application/json",
                "Content-Type": "application/x-www-form-urlencoded",
                "User-Agent": "WaypointsTerritoryBuilder/1.0",
            },
            method="POST",
        )
        try:
            with urllib.request.urlopen(
                request,
                timeout=OVERPASS_ATTEMPT_TIMEOUT_SECONDS,
            ) as response:
                payload = response.read(OVERPASS_MAX_RESPONSE_BYTES + 1)
            if len(payload) > OVERPASS_MAX_RESPONSE_BYTES:
                raise RuntimeError("Overpass street response exceeded its byte budget")
            decoded = json.loads(payload)
            elements = decoded.get("elements")
            if not isinstance(elements, list):
                raise RuntimeError("Overpass street response was malformed")
            roads = named_roads(elements, city_geometry)
            if not roads:
                raise RuntimeError("Overpass returned no named roads for the city")
            return roads
        except (OSError, ValueError, RuntimeError) as error:
            failure = error
    raise RuntimeError("Every Overpass street endpoint failed") from failure


def named_roads(
    elements: Iterable[dict[str, Any]], city_geometry: BaseGeometry
) -> list[tuple[str, BaseGeometry]]:
    roads: list[tuple[str, BaseGeometry]] = []
    for element in elements:
        if not isinstance(element, dict) or element.get("type") != "way":
            continue
        tags = element.get("tags") or {}
        if not isinstance(tags, dict):
            continue
        name = normalize_name(str(tags.get("name") or ""))
        if not name:
            continue
        raw_geometry = element.get("geometry")
        if not isinstance(raw_geometry, list):
            continue
        points = [
            (point.get("lon"), point.get("lat"))
            for point in raw_geometry
            if isinstance(point, dict)
            and isinstance(point.get("lat"), (int, float))
            and isinstance(point.get("lon"), (int, float))
        ]
        if len(points) < 2:
            continue
        geometry = LineString(points)
        if not geometry.intersects(city_geometry):
            continue
        clipped = geometry.intersection(city_geometry)
        if not clipped.is_empty:
            roads.append((name, clipped))
    return roads


def make_bundle(
    *,
    release: str,
    city: dict[str, Any],
    districts: list[dict[str, Any]],
    neighborhoods: list[dict[str, Any]],
    roads: list[tuple[str, BaseGeometry]],
    water: BaseGeometry | None = None,
    nearby: list[dict[str, Any]] | None = None,
    place: dict[str, str] | None = None,
) -> dict[str, Any]:
    road_geometries = [geometry for _, geometry in roads]
    tree = STRtree(road_geometries)
    district_indices: dict[str, dict[str, list[str]]] = {}
    for district in districts:
        area_id = f"relation/{relation_id(district)}"
        district_indices[area_id] = street_index(valid_geometry(district), roads, tree)
    neighborhood_indices: dict[str, dict[str, list[str]]] = {}
    for neighborhood in neighborhoods:
        area_id = f"overture/{feature_id(neighborhood)}"
        neighborhood_indices[area_id] = street_index(
            valid_geometry(neighborhood), roads, tree
        )

    return {
        "schemaVersion": 1,
        "datasetVersion": f"overture-{release}-r{BUNDLE_REVISION}",
        "updatedAt": int(time.time() * 1000),
        "city": {
            "area": administrative_area(city, 8, water),
            "districts": [
                administrative_area(value, 9, water) for value in districts
            ],
            "nearby": [administrative_area(value, 8) for value in nearby or []],
        },
        "neighborhoods": [
            neighborhood_area(value, water) for value in neighborhoods
        ],
        "streetCells": {
            "districts": district_indices,
            "neighborhoods": neighborhood_indices,
        },
        # The country and region the app groups this place under, so it never
        # has to ask a reverse geocoder for them.
        "hierarchy": place or {},
    }


def place_hierarchy(
    city: dict[str, Any],
    divisions: Iterable[dict[str, Any]],
) -> dict[str, str]:
    """The country and region the place belongs to, from Overture.

    Every Overture division carries the ISO 3166-1 code of its country, the
    ISO 3166-2 code of its principal subdivision, and the chain of divisions
    above it with their names. The app used to ask a reverse geocoder for
    these, one request per place, and places in countries whose regions that
    geocoder does not report stayed unnamed. Reading them here names every
    place the moment its bundle arrives.
    """
    properties = city.get("properties") or {}
    division_id = properties.get("division_id")
    record: dict[str, Any] = {}
    for division in divisions:
        division_properties = division.get("properties") or {}
        if (division.get("id") or division_properties.get("id")) == division_id:
            record = division_properties
            break
    hierarchies = record.get("hierarchies")
    chain: list[dict[str, Any]] = []
    if isinstance(hierarchies, list) and hierarchies and isinstance(hierarchies[0], list):
        chain = [entry for entry in hierarchies[0] if isinstance(entry, dict)]

    def named(subtype: str) -> str | None:
        for entry in chain:
            name = entry.get("name")
            if entry.get("subtype") == subtype and isinstance(name, str) and name.strip():
                return name.strip()
        return None

    region_name = next(
        (name for subtype in REGION_SUBTYPES if (name := named(subtype))), None
    )
    if region_name is None and properties.get("subtype") in REGION_SUBTYPES:
        # A place that is itself a region, such as Berlin, names itself.
        region_name = primary_name(properties) or None

    place: dict[str, str] = {}
    country_code = record.get("country") or properties.get("country")
    if isinstance(country_code, str) and re.fullmatch(r"[A-Z]{2}", country_code):
        place["countryCode"] = country_code
    if country_name := named("country"):
        place["countryName"] = country_name
    region_code = record.get("region") or properties.get("region")
    if isinstance(region_code, str) and re.fullmatch(r"[A-Z]{2}-[A-Z0-9]{1,3}", region_code):
        place["regionCode"] = region_code
        if region_name is None:
            # Without the place's own record, the region's record read nearby
            # names it by its code.
            for division in divisions:
                division_properties = division.get("properties") or {}
                if (
                    division_properties.get("subtype") in REGION_SUBTYPES
                    and division_properties.get("region") == region_code
                ):
                    region_name = primary_name(division_properties) or None
                    if region_name:
                        break
    if region_name:
        place["regionName"] = region_name
    return place


def street_index(
    area: BaseGeometry,
    roads: list[tuple[str, BaseGeometry]],
    tree: STRtree,
) -> dict[str, list[str]]:
    cells: dict[str, set[str]] = {}
    for raw_index in tree.query(area, predicate="intersects"):
        index = int(raw_index)
        name, geometry = roads[index]
        clipped = geometry.intersection(area)
        street_cells = cells.setdefault(name, set())
        for line in line_strings(clipped):
            coordinates = list(line.coords)
            for start, end in zip(coordinates, coordinates[1:]):
                distance = haversine(start[1], start[0], end[1], end[0])
                steps = max(1, math.ceil(distance / SAMPLE_SPACING_METERS))
                for step in range(steps + 1):
                    progress = step / steps
                    longitude = start[0] + (end[0] - start[0]) * progress
                    latitude = start[1] + (end[1] - start[1]) * progress
                    street_cells.add(cell_id(latitude, longitude))
    return {name: sorted(values) for name, values in sorted(cells.items())}


def line_strings(geometry: BaseGeometry) -> Iterable[LineString]:
    if isinstance(geometry, LineString):
        yield geometry
    elif isinstance(geometry, MultiLineString) or hasattr(geometry, "geoms"):
        for child in geometry.geoms:
            yield from line_strings(child)


def administrative_area(
    feature: dict[str, Any],
    admin_level: int,
    water: BaseGeometry | None = None,
) -> dict[str, Any]:
    area = {
        "relationId": relation_id(feature),
        "name": primary_name(feature.get("properties") or {}) or "Unnamed area",
        "adminLevel": admin_level,
        "boundary": boundary_json(valid_geometry(feature), water),
    }
    if place_class := settlement_class(feature):
        area["placeClass"] = place_class
    return area


def neighborhood_area(
    feature: dict[str, Any],
    water: BaseGeometry | None = None,
) -> dict[str, Any]:
    geometry = valid_geometry(feature)
    center = geometry.representative_point()
    area = {
        "id": f"overture/{feature_id(feature)}",
        "name": primary_name(feature.get("properties") or {}) or "Unnamed area",
        "lat": center.y,
        "lon": center.x,
        "adminLevel": 10,
        "boundary": boundary_json(geometry, water),
    }
    if place_class := settlement_class(feature):
        area["place"] = place_class
    return area


def settlement_class(feature: dict[str, Any]) -> str | None:
    """The settlement class of an area settlement_areas drew, such as village.

    The app words a place split this way by what its parts are, so Snagov's
    parts read as villages rather than districts.
    """
    properties = feature.get("properties") or {}
    value = properties.get("class")
    if properties.get("synthetic") is True and value in SETTLEMENT_CLASSES:
        return value
    return None


def boundary_json(
    geometry: BaseGeometry,
    water: BaseGeometry | None = None,
) -> dict[str, Any]:
    simplified = geometry.simplify(BOUNDARY_SIMPLIFICATION_DEGREES, preserve_topology=True)
    polygons = [simplified] if simplified.geom_type == "Polygon" else list(simplified.geoms)
    outer: list[list[list[float]]] = []
    inner: list[list[list[float]]] = []
    for polygon in polygons:
        if polygon.geom_type != "Polygon":
            continue
        outer.append([[latitude, longitude] for longitude, latitude in polygon.exterior.coords])
        for ring in polygon.interiors:
            inner.append([[latitude, longitude] for longitude, latitude in ring.coords])
    if not outer:
        raise RuntimeError("Division geometry has no polygon boundary")
    result: dict[str, Any] = {"outer": outer}
    if inner:
        result["inner"] = inner
    # The outline keeps its lakes, so a fix on the water still falls inside
    # the place. The water is reported beside it instead.
    water_area = water_square_meters(geometry, water)
    if water_area >= 1:
        result["waterSquareMeters"] = round(water_area)
    return result


def publish_bundle(
    *,
    bundle: dict[str, Any],
    city_feature: dict[str, Any],
    request_key: str | None,
    request_id: str | None,
    representative: tuple[float, float],
) -> None:
    bucket_name = required_environment("R2_BUCKET")
    client = r2_client()
    city_key = safe_key(feature_id(city_feature))
    version = bundle["datasetVersion"]
    bundle_key = f"bundles/{city_key}/{version}.json.gz"
    encoded = json.dumps(bundle, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    client.put_object(
        Bucket=bucket_name,
        Key=bundle_key,
        Body=gzip.compress(encoded, compresslevel=9),
        ContentType="application/json",
        ContentEncoding="gzip",
        CacheControl="public, max-age=31536000, immutable",
    )

    city_boundary = bundle["city"]["area"]["boundary"]
    candidate = json.dumps(
        {
            "cityId": city_key,
            "version": version,
            "bundleKey": bundle_key,
            "boundary": city_boundary,
            "updatedAt": bundle["updatedAt"],
        },
        ensure_ascii=False,
        separators=(",", ":"),
    ).encode("utf-8")
    city_geometry = valid_geometry(city_feature)
    tiles = sorted(
        set(index_tiles(city_geometry))
        | {
            (
                tile_x(representative[1], INDEX_ZOOM),
                tile_y(representative[0], INDEX_ZOOM),
            )
        }
    )
    if len(tiles) > MAX_INDEX_TILES:
        raise RuntimeError(
            f"City requires {len(tiles)} lookup tiles, above the safety cap of {MAX_INDEX_TILES}"
        )
    for x, y in tiles:
        client.put_object(
            Bucket=bucket_name,
            Key=f"index/{INDEX_ZOOM}/{x}/{y}/{city_key}.json",
            Body=candidate,
            ContentType="application/json",
            CacheControl="public, max-age=3600",
        )

    manifest = {
        "cityId": city_key,
        "version": version,
        "bundleKey": bundle_key,
        "latitude": representative[0],
        "longitude": representative[1],
        "updatedAt": int(time.time() * 1000),
    }
    client.put_object(
        Bucket=bucket_name,
        Key=f"manifests/{city_key}.json",
        Body=json.dumps(manifest, separators=(",", ":")).encode("utf-8"),
        ContentType="application/json",
        CacheControl="no-store",
    )
    if request_key:
        client.delete_object(Bucket=bucket_name, Key=f"requests/{request_key}.json")
    if request_id:
        client.delete_object(Bucket=bucket_name, Key=f"requests/by-id/{request_id}.json")


def r2_client() -> Any:
    account_id = required_environment("R2_ACCOUNT_ID")
    return boto3.client(
        "s3",
        endpoint_url=f"https://{account_id}.r2.cloudflarestorage.com",
        aws_access_key_id=required_environment("R2_ACCESS_KEY_ID"),
        aws_secret_access_key=required_environment("R2_SECRET_ACCESS_KEY"),
        region_name="auto",
    )


def index_tiles(geometry: BaseGeometry) -> Iterable[tuple[int, int]]:
    west, south, east, north = geometry.bounds
    first_x = tile_x(west, INDEX_ZOOM)
    last_x = tile_x(east, INDEX_ZOOM)
    first_y = tile_y(north, INDEX_ZOOM)
    last_y = tile_y(south, INDEX_ZOOM)
    for x in range(min(first_x, last_x), max(first_x, last_x) + 1):
        for y in range(min(first_y, last_y), max(first_y, last_y) + 1):
            if geometry.intersects(tile_polygon(x, y, INDEX_ZOOM)):
                yield x, y


def tile_polygon(x: int, y: int, zoom: int) -> BaseGeometry:
    west = longitude_for_x(x, zoom)
    east = longitude_for_x(x + 1, zoom)
    north = latitude_for_y(y, zoom)
    south = latitude_for_y(y + 1, zoom)
    return box(west, south, east, north)


def cell_id(latitude: float, longitude: float) -> str:
    return f"{CELL_ZOOM}/{tile_x(longitude, CELL_ZOOM)}/{tile_y(latitude, CELL_ZOOM)}"


def tile_x(longitude: float, zoom: int) -> int:
    count = 1 << zoom
    if longitude >= 180:
        return count - 1
    normalized = ((longitude + 180) % 360 + 360) % 360
    return min(count - 1, max(0, math.floor(normalized / 360 * count)))


def tile_y(latitude: float, zoom: int) -> int:
    count = 1 << zoom
    bounded = min(MAX_MERCATOR_LATITUDE, max(-MAX_MERCATOR_LATITUDE, latitude))
    radians = math.radians(bounded)
    projected = (1 - math.log(math.tan(radians) + 1 / math.cos(radians)) / math.pi) / 2
    return min(count - 1, max(0, math.floor(projected * count)))


def longitude_for_x(x: int, zoom: int) -> float:
    return x / (1 << zoom) * 360 - 180


def latitude_for_y(y: int, zoom: int) -> float:
    value = math.pi - 2 * math.pi * y / (1 << zoom)
    return math.degrees(math.atan(math.sinh(value)))


def haversine(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    first = math.radians(lat1)
    second = math.radians(lat2)
    delta_latitude = second - first
    delta_longitude = math.radians(((lon2 - lon1 + 540) % 360) - 180)
    value = (
        math.sin(delta_latitude / 2) ** 2
        + math.cos(first) * math.cos(second) * math.sin(delta_longitude / 2) ** 2
    )
    return 2 * EARTH_RADIUS_METERS * math.asin(math.sqrt(min(1.0, value)))


def primary_name(properties: dict[str, Any]) -> str:
    names = properties.get("names") or {}
    primary = names.get("primary") if isinstance(names, dict) else None
    if isinstance(primary, str):
        return primary.strip()
    if isinstance(primary, dict):
        value = primary.get("value")
        return value.strip() if isinstance(value, str) else ""
    return ""


def normalize_name(value: str) -> str:
    return " ".join(value.strip().lower().split())


def feature_id(feature: dict[str, Any]) -> str:
    value = feature.get("id") or (feature.get("properties") or {}).get("id")
    if not value:
        raise RuntimeError("Overture feature has no stable ID")
    return str(value)


def relation_id(feature: dict[str, Any]) -> int:
    properties = feature.get("properties") or {}
    for source in properties.get("sources") or []:
        record_id = str(source.get("record_id") or "")
        match = re.search(r"(?:relation/|\br)(\d+)", record_id)
        if match:
            return int(match.group(1))
    digest = hashlib.sha256(feature_id(feature).encode("utf-8")).hexdigest()
    return int(digest[:13], 16)


def safe_key(value: str) -> str:
    return re.sub(r"[^a-zA-Z0-9._-]", "_", value)


def required_environment(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"Missing required environment variable {name}")
    return value


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        if not PUBLIC_LOG:
            raise
        # A traceback can carry the search box or a place name.
        print(f"Build failed: {type(error).__name__}")
        sys.exit(1)
