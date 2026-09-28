#!/usr/bin/env python3
"""Build one immutable Waypoints territory bundle from mapped source data."""

from __future__ import annotations

import argparse
import functools
import gzip
import hashlib
import io
import json
import math
import os
import re
import sys
import tempfile
import time
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any, Iterable

import boto3
import pyarrow
import pyarrow.compute
import pyarrow.dataset
import pyarrow.fs
import pyarrow.parquet
import shapely
from overturemaps.core import geoarrow_schema_adapter, type_theme_map as OVERTURE_THEMES
from overturemaps.writers import get_writer
from shapely.affinity import scale
from shapely.geometry import LineString, MultiLineString, MultiPoint, Point, box, mapping, shape
from shapely.geometry.base import BaseGeometry
from shapely.ops import polygonize, unary_union
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
# country and region each place belongs to. Revision 9 draws neighbourhoods
# from their named points in a place that maps no neighbourhood outlines.
# Revision 10 ignores outlines too small to be a neighbourhood and fills a
# place its outlines barely cover from its named points. Revision 11 fills all
# the ground outlines leave open: named points, then named land such as parks
# and campuses, then road-bounded areas named after their main road.
# Revision 12 names road-bounded areas after the land filling them or their
# crossroads, and folds areas with few streets into their neighbours.
# Revision 13 also drops street types that lead a name, as in Romanian.
BUNDLE_REVISION = 13
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
# A place that maps its neighbourhoods only as named points, such as Brandon,
# is split between those points along its main roads, rail lines and large
# water, so the explorer's local area is a real, named neighbourhood rather
# than a square of the exploration grid. Fewer points than this cannot cover a
# town, so the place keeps the grid instead of a few oversized areas.
POINT_NEIGHBORHOOD_MINIMUM = 4
# An outline smaller than this is a plaza, courtyard or campus quad that
# OpenStreetMap tags as a neighbourhood, such as Cupertino's 160 m2 Olive
# Court, not an area to explore, so it is left out.
NEIGHBORHOOD_MINIMUM_SQUARE_METERS = 50_000.0
# Local areas a bundle carries at most. The app measures every area of the
# place on each newly explored cell, so filling open ground never adds areas
# past this, and a place its outlines already fill to it keeps its gaps.
MAXIMUM_AREAS = 250
# Ground no neighbourhood reaches is cut along main roads into areas about
# this large, or larger when the budget of areas runs short.
ROAD_AREA_TARGET_SQUARE_METERS = 1_000_000.0
# A leftover strip smaller than this, such as a sliver between two outlines,
# is not an area of its own.
ROAD_AREA_MINIMUM_SQUARE_METERS = 20_000.0
# Streets this close to a road-bounded area can name it.
ROAD_AREA_NAMING_METERS = 40.0
# A road-bounded area with fewer named streets than this joins its neighbour,
# since a card counting two streets is not worth its own place.
ROAD_AREA_MINIMUM_STREETS = 8
# Only an area smaller than the target joins a neighbour for having few
# streets, and never into an area larger than this many times the target, so
# farmland with few streets is not swallowed into one vast area.
ROAD_AREA_MERGE_LIMIT_FACTOR = 3.0
# A sliver this small with few streets and nothing to join is dropped.
ROAD_AREA_SLIVER_SQUARE_METERS = 100_000.0
# Direction words a crossroads name leaves off the end: "Victoria Avenue East"
# reads "Victoria", the same road as "Victoria Avenue". A leading direction
# is often part of the name, as in West Valley Freeway, so it stays.
DIRECTION_WORDS = frozenset(
    {"north", "south", "east", "west", "n", "s", "e", "w", "ne", "nw", "se", "sw",
     "northeast", "northwest", "southeast", "southwest"}
)
# Named land covering at least this share of a road-bounded area names it,
# as long as the land is not far larger than the area itself.
LANDMARK_NAMING_SHARE = 0.25
LANDMARK_NAMING_MAXIMUM_RATIO = 4.0
# Street type words a crossroads name leaves out, so it reads the way people
# give directions: "Courtney & Dewdney".
STREET_TYPE_WORDS = frozenset(
    {
        "street", "st", "avenue", "ave", "road", "rd", "drive", "dr",
        "boulevard", "blvd", "way", "lane", "ln", "place", "pl", "court",
        "ct", "crescent", "cres", "terrace", "parkway", "pkwy", "highway",
        "hwy", "expressway", "freeway", "trail", "gate", "circle", "row",
    }
)
# Street type words that lead a street's name in Romanian, French, Spanish,
# Italian and Portuguese, which a crossroads name leaves out the same way:
# "Șoseaua Giurgiului & Strada Alexandru Anghel" reads "Giurgiului &
# Alexandru Anghel". German and Dutch join the type onto the name, as in
# Hauptstraße, so they keep it.
LEADING_STREET_TYPE_WORDS = frozenset(
    {
        # Romanian
        "strada", "str", "șoseaua", "şoseaua", "soseaua", "bulevardul", "bd",
        "bdul", "b-dul", "calea", "splaiul", "aleea", "drumul", "intrarea",
        "piața", "piaţa", "piata", "pasajul", "pasaj", "podul", "pod",
        "prelungirea", "autostrada",
        # French
        "rue", "avenue", "av", "boulevard", "chemin", "allée", "allee",
        "impasse", "route", "quai", "cours",
        # Spanish
        "calle", "avenida", "avda", "paseo", "camino", "carretera", "ronda",
        "travesía", "travesia",
        # Italian
        "via", "viale", "corso", "piazza", "vicolo", "strada",
        # Portuguese
        "rua", "travessa", "estrada", "alameda", "largo",
    }
)
# Small words a name can start with once its type is gone, such as "de" in
# "Șoseaua de Centură". A name left starting with one keeps its type.
NAME_PARTICLES = frozenset(
    {"de", "del", "della", "delle", "di", "da", "do", "dos", "das", "du",
     "des", "la", "le", "les", "el", "los", "las", "a", "al", "lui"}
)
# A block this many times the target area, such as a hillside few main roads
# cross, is split into pieces of about the target area.
ROAD_AREA_SPLIT_FACTOR = 3.0
# Named land of these classes is part of the neighbourhood around it rather
# than a place to explore on its own.
LAND_USE_EXCLUDED_CLASSES = frozenset(
    {"school", "kindergarten", "childcare", "hospital", "clinic"}
)
# How strongly a road names the ground beside it, by its highway class.
HIGHWAY_RANKS = {
    "motorway": 7,
    "trunk": 6,
    "primary": 5,
    "secondary": 4,
    "tertiary": 3,
    "unclassified": 2,
    "residential": 1,
}
# A block with no neighbourhood point of its own joins the nearest one only
# this close, so farmland and industry at a town's edge are left to the grid.
POINT_NEIGHBORHOOD_REACH_METERS = 1_500.0
# Water at least this large cuts neighbourhoods apart, the way a river does.
POINT_NEIGHBORHOOD_WATER_CUT_SQUARE_METERS = 50_000.0
# Roads and railways that bound neighbourhoods.
BOUNDARY_HIGHWAYS = frozenset({"motorway", "trunk", "primary", "secondary", "tertiary"})
BOUNDARY_RAILWAYS = frozenset({"rail", "light_rail"})
METERS_PER_DEGREE = 111_320.0
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
# Id of the stored request a worker build is for, so a failed build can
# delete the request, and the coordinate in it, as a published one does.
REQUEST_ID: str | None = None


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
    global PUBLIC_LOG, REQUEST_ID
    args = parse_args()
    if args.request_id is not None:
        PUBLIC_LOG = True
        REQUEST_ID = args.request_id
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

        if contains_bounds(city_search_bounds(args.latitude, args.longitude), city_bounds):
            # The search already read every area around the place, so the
            # place's own areas are picked from it instead of read again.
            divisions = features_within(search_areas, city_bounds)
        else:
            divisions = deduplicate_divisions(
                download_features("division_area", city_bounds, work / "divisions.geojson")
            )
        districts = place_parts(divisions, hierarchy, city_feature, city_geometry)
        nearby = nearby_places(search_areas, hierarchy, city_feature, city_geometry)
        neighborhoods = [
            area
            for area in select_areas(
                divisions,
                city_geometry,
                NEIGHBORHOOD_SUBTYPES,
                maximum=MAXIMUM_AREAS,
            )
            if square_meters_within(valid_geometry(area), city_geometry)
            >= NEIGHBORHOOD_MINIMUM_SQUARE_METERS
        ]
        # Ground no outline covers, which the explorer should still find a
        # named local area on.
        open_ground = open_neighborhood_ground(city_geometry, neighborhoods)
        settlements: list[dict[str, Any]] = []
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
        fill_budget = MAXIMUM_AREAS - len(neighborhoods)
        fills = not settlements and open_ground is not None and fill_budget > 0
        # Streets come from Overpass, water and land use from Overture, so all
        # are read at once.
        with ThreadPoolExecutor(max_workers=3) as pool:
            streets = pool.submit(download_named_roads, coverage_geometry)
            lakes = pool.submit(
                download_water, coverage_geometry.bounds, work / "water.geojson"
            )
            uses = (
                pool.submit(
                    download_features,
                    "land_use",
                    open_ground.bounds,
                    work / "land-use.geojson",
                    allow_empty=True,
                )
                if fills
                else None
            )
            roads, boundaries, road_ranks, road_names = streets.result()
            water = lakes.result()
            land_use = uses.result() if uses is not None else []
        if fills:
            # No settlement splits the place, so the ground its outlines leave
            # open is filled with named local areas.
            drawn = fill_open_ground(
                open_ground=open_ground,
                named={primary_name(area.get("properties") or {}) for area in neighborhoods},
                divisions=hierarchy,
                land_use=land_use,
                boundaries=boundaries,
                roads=roads,
                road_ranks=road_ranks,
                road_names=road_names,
                water=water,
                place_name=primary_name(city_feature.get("properties") or {}),
                budget=fill_budget,
            )
            neighborhoods = [*neighborhoods, *drawn]
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


@functools.cache
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
    bounds = city_search_bounds(latitude, longitude)
    # The areas and the division records are separate reads, so they run
    # side by side.
    with ThreadPoolExecutor(max_workers=2) as pool:
        areas = pool.submit(
            download_features, "division_area", bounds, work / "city-search.geojson"
        )
        records = pool.submit(
            download_features, "division", bounds, work / "city-hierarchy.geojson"
        )
        features = deduplicate_divisions(areas.result())
        divisions = records.result()
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


def city_search_bounds(
    latitude: float, longitude: float
) -> tuple[float, float, float, float]:
    """The box the place and its neighbours are searched for in."""
    radius = CITY_SEARCH_RADIUS_DEGREES
    return (
        max(-180.0, longitude - radius),
        max(-90.0, latitude - radius),
        min(180.0, longitude + radius),
        min(90.0, latitude + radius),
    )


def contains_bounds(
    outer: tuple[float, float, float, float],
    inner: tuple[float, float, float, float],
) -> bool:
    return (
        outer[0] <= inner[0]
        and outer[1] <= inner[1]
        and outer[2] >= inner[2]
        and outer[3] >= inner[3]
    )


def download_features(
    feature_type: str,
    bounds: tuple[float, float, float, float],
    output: Path,
    *,
    allow_empty: bool = False,
) -> list[dict[str, Any]]:
    """Overture features of one type whose bounding box meets [bounds].

    Only the release files the Overture file index places around [bounds] are
    read. Scanning the whole type instead opens the footer of every file of
    the release and took most of a build's time.
    """
    release = latest_release()
    files = overture_files(release, feature_type, bounds)
    source = (
        files
        if files is not None
        else f"overturemaps-us-west-2/release/{release}/theme={OVERTURE_THEMES[feature_type]}/type={feature_type}/"
    )
    if files == []:
        return []
    xmin, ymin, xmax, ymax = bounds
    # The same row filter the Overture CLI applies.
    row_filter = (
        (pyarrow.compute.field("bbox", "xmin") < xmax)
        & (pyarrow.compute.field("bbox", "xmax") > xmin)
        & (pyarrow.compute.field("bbox", "ymin") < ymax)
        & (pyarrow.compute.field("bbox", "ymax") > ymin)
    )
    try:
        dataset = pyarrow.dataset.dataset(
            source,
            filesystem=pyarrow.fs.S3FileSystem(anonymous=True, region="us-west-2"),
        )
        batches = dataset.to_batches(
            filter=row_filter,
            use_threads=True,
            batch_readahead=16,
            fragment_readahead=4,
        )
        with get_writer(
            "geojson",
            str(output),
            schema=geoarrow_schema_adapter(dataset.schema),
        ) as writer:
            for batch in batches:
                if batch.num_rows > 0:
                    writer.write_batch(batch)
    except (OSError, pyarrow.ArrowException) as error:
        # A type that may be missing, such as water, reads as none.
        if allow_empty:
            return []
        raise RuntimeError(f"Overture {feature_type} download failed") from error
    with output.open("r", encoding="utf-8") as source_file:
        payload = json.load(source_file)
    if payload.get("type") != "FeatureCollection":
        raise RuntimeError(f"Overture {feature_type} download was not GeoJSON")
    return [
        feature for feature in payload.get("features", []) if isinstance(feature, dict)
    ]


@functools.cache
def overture_file_index(release: str) -> list[tuple[str, tuple[float, float, float, float]]]:
    """Every file of [release] with its bounding box, from the Overture index.

    The index no longer names each file's type in its collection field, which
    is why the Overture CLI's own index lookup matches nothing, but every file
    path still carries its type.
    """
    with urllib.request.urlopen(
        f"https://stac.overturemaps.org/{release}/collections.parquet", timeout=30
    ) as response:
        table = pyarrow.parquet.read_table(io.BytesIO(response.read()))
    files = []
    for asset, bbox in zip(
        table.column("assets").to_pylist(), table.column("bbox").to_pylist()
    ):
        href = asset["aws"]["alternate"]["s3"]["href"]
        files.append(
            (
                href.removeprefix("s3://"),
                (bbox["xmin"], bbox["ymin"], bbox["xmax"], bbox["ymax"]),
            )
        )
    return files


def overture_files(
    release: str,
    feature_type: str,
    bounds: tuple[float, float, float, float],
) -> list[str] | None:
    """Files of [feature_type] whose bounding box meets [bounds], in path order,
    or None when the index cannot be read and the whole type must be scanned."""
    try:
        index = overture_file_index(release)
    except (OSError, ValueError, KeyError, TypeError, pyarrow.ArrowException):
        return None
    marker = f"/type={feature_type}/"
    typed = [(path, box_) for path, box_ in index if marker in path]
    if not typed:
        return None
    xmin, ymin, xmax, ymax = bounds
    return sorted(
        path
        for path, (west, south, east, north) in typed
        if west < xmax and east > xmin and south < ymax and north > ymin
    )


def features_within(
    features: Iterable[dict[str, Any]],
    bounds: tuple[float, float, float, float],
) -> list[dict[str, Any]]:
    """Features whose geometry's bounding box meets [bounds].

    A small margin keeps every feature a fresh download of [bounds] would
    return, since Overture rounds its stored boxes outward. Callers measure
    each feature against the place's outline anyway.
    """
    margin = 1e-6
    xmin, ymin, xmax, ymax = bounds
    selected = []
    for feature in features:
        raw = feature.get("geometry")
        if not raw:
            continue
        west, south, east, north = shape(raw).bounds
        if (
            west < xmax + margin
            and east > xmin - margin
            and south < ymax + margin
            and north > ymin - margin
        ):
            selected.append(feature)
    return selected


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
) -> tuple[
    list[tuple[str, BaseGeometry]], list[BaseGeometry], dict[str, int], dict[str, str]
]:
    """The place's named streets, the main roads and railways that bound its
    neighbourhoods, each street's highway rank and the name it is written
    with, from one Overpass read."""
    west, south, east, north = city_geometry.bounds
    box_filter = f"({south:.8f},{west:.8f},{north:.8f},{east:.8f})"
    railways = "|".join(sorted(BOUNDARY_RAILWAYS))
    query = (
        f"[out:json][timeout:{OVERPASS_QUERY_TIMEOUT_SECONDS}];"
        f'(way{box_filter}["highway"]["name"];'
        f'way{box_filter}["railway"~"^({railways})$"];);'
        "out tags geom;"
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
            return (
                roads,
                boundary_lines(elements, city_geometry),
                highway_ranks(elements),
                display_names(elements),
            )
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
        if not isinstance(tags, dict) or not tags.get("highway"):
            # Railways share the read only to bound neighbourhoods.
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


def way_line(element: dict[str, Any]) -> LineString | None:
    raw_geometry = element.get("geometry")
    if not isinstance(raw_geometry, list):
        return None
    points = [
        (point.get("lon"), point.get("lat"))
        for point in raw_geometry
        if isinstance(point, dict)
        and isinstance(point.get("lat"), (int, float))
        and isinstance(point.get("lon"), (int, float))
    ]
    return LineString(points) if len(points) >= 2 else None


def boundary_lines(
    elements: Iterable[dict[str, Any]], city_geometry: BaseGeometry
) -> list[BaseGeometry]:
    """Main roads and railways across the place, which bound neighbourhoods.

    Sidings, yards and spurs carry a service tag and are left out, as are
    ramps, which only join the roads they belong to.
    """
    lines: list[BaseGeometry] = []
    for element in elements:
        if not isinstance(element, dict) or element.get("type") != "way":
            continue
        tags = element.get("tags") or {}
        if not isinstance(tags, dict):
            continue
        bounding = tags.get("highway") in BOUNDARY_HIGHWAYS or (
            tags.get("railway") in BOUNDARY_RAILWAYS and not tags.get("service")
        )
        if not bounding:
            continue
        line = way_line(element)
        if line is not None and line.intersects(city_geometry):
            lines.append(line)
    return lines


def square_meters_within(geometry: BaseGeometry, place: BaseGeometry) -> float:
    return sum(
        polygon_square_meters(polygon)
        for polygon in polygons_of(geometry.intersection(place))
    )


def open_neighborhood_ground(
    city_geometry: BaseGeometry, outlines: list[dict[str, Any]]
) -> BaseGeometry | None:
    """The part of the place its outlines leave open, or None when they leave
    no ground large enough to be an area."""
    if not outlines:
        return city_geometry
    covered = unary_union([valid_geometry(area) for area in outlines])
    open_ground = polygonal(city_geometry.difference(covered))
    if square_meters_within(open_ground, city_geometry) < ROAD_AREA_MINIMUM_SQUARE_METERS:
        return None
    return open_ground


def polygonal(geometry: BaseGeometry) -> BaseGeometry:
    """The valid polygon part of [geometry].

    Subtracting one area from another can leave invalid rings and stray
    lines, which the cutting and measuring below cannot take.
    """
    if geometry.is_valid and geometry.geom_type in ("Polygon", "MultiPolygon"):
        # Already clean, and left exactly as it is, so the areas cut from it
        # match earlier builds to the last coordinate.
        return geometry
    if not geometry.is_valid:
        geometry = shapely.make_valid(geometry)
    polygons = [polygon for polygon in polygons_of(geometry) if not polygon.is_empty]
    if not polygons:
        return shapely.Polygon()
    return unary_union(polygons)


def metric_frame(latitude: float):
    """Converters to and from metres on a local plane around [latitude]."""
    x_meters = math.cos(math.radians(latitude)) * METERS_PER_DEGREE

    def to_meters(geometry: BaseGeometry) -> BaseGeometry:
        return scale(geometry, xfact=x_meters, yfact=METERS_PER_DEGREE, origin=(0, 0))

    def to_degrees(geometry: BaseGeometry) -> BaseGeometry:
        return scale(
            geometry, xfact=1 / x_meters, yfact=1 / METERS_PER_DEGREE, origin=(0, 0)
        )

    return to_meters, to_degrees


def cut_blocks(
    ground: BaseGeometry,
    boundaries: Iterable[BaseGeometry],
    water: BaseGeometry | None,
    to_meters,
) -> list[BaseGeometry]:
    """[ground], in metres, cut into blocks along main roads, railways, large
    water and its own edges."""
    ground_meters = polygonal(to_meters(polygonal(ground)))
    if ground_meters.is_empty:
        return []
    cuts: list[BaseGeometry] = [ground_meters.boundary]
    cuts.extend(to_meters(line).intersection(ground_meters) for line in boundaries)
    if water is not None:
        for polygon in polygons_of(water):
            if (
                polygon.intersects(ground)
                and polygon_square_meters(polygon)
                >= POINT_NEIGHBORHOOD_WATER_CUT_SQUARE_METERS
            ):
                cuts.append(to_meters(polygon).boundary.intersection(ground_meters))
    return [
        block
        for block in polygonize(unary_union([cut for cut in cuts if not cut.is_empty]))
        if ground_meters.contains(block.representative_point())
    ]


def highway_ranks(elements: Iterable[dict[str, Any]]) -> dict[str, int]:
    """The highest highway rank each street name carries."""
    ranks: dict[str, int] = {}
    for element in elements:
        if not isinstance(element, dict):
            continue
        tags = element.get("tags") or {}
        if not isinstance(tags, dict):
            continue
        name = normalize_name(str(tags.get("name") or ""))
        rank = HIGHWAY_RANKS.get(str(tags.get("highway") or "").removesuffix("_link"), 0)
        if name and rank > ranks.get(name, 0):
            ranks[name] = rank
    return ranks


def display_names(elements: Iterable[dict[str, Any]]) -> dict[str, str]:
    """The name each street is written with, keyed by its normalised name."""
    counts: dict[str, dict[str, int]] = {}
    for element in elements:
        if not isinstance(element, dict):
            continue
        tags = element.get("tags") or {}
        if not isinstance(tags, dict) or not tags.get("highway"):
            continue
        written = " ".join(str(tags.get("name") or "").split())
        name = normalize_name(written)
        if name:
            spellings = counts.setdefault(name, {})
            spellings[written] = spellings.get(written, 0) + 1
    return {
        name: max(spellings, key=lambda written: (spellings[written], written))
        for name, spellings in counts.items()
    }


def compass_word(origin: BaseGeometry, point: BaseGeometry) -> str:
    """The direction of [point] from [origin], in metres on a local plane."""
    angle = math.degrees(math.atan2(point.y - origin.y, point.x - origin.x))
    words = ("east", "northeast", "north", "northwest", "west", "southwest", "south", "southeast")
    return words[round(angle / 45) % 8]


def fill_open_ground(
    *,
    open_ground: BaseGeometry,
    named: set[str],
    divisions: Iterable[dict[str, Any]],
    land_use: Iterable[dict[str, Any]],
    boundaries: list[BaseGeometry],
    roads: list[tuple[str, BaseGeometry]],
    road_ranks: dict[str, int],
    road_names: dict[str, str],
    water: BaseGeometry | None,
    place_name: str,
    budget: int,
) -> list[dict[str, Any]]:
    """Named local areas for the ground no outline covers, within [budget].

    Named neighbourhood points claim their road-bounded blocks first. Named
    land such as parks, golf courses, cemeteries, campuses and industrial
    estates claims what they leave. What is still open is cut along main roads
    into areas named after their main road, so every part of the place large
    enough to stand in belongs to a named area.
    """
    # A point or a piece of land named like an area the place already has,
    # such as a point beside its own outline, would list the name twice.
    taken = {name.casefold() for name in named}
    points = [
        area
        for area in point_neighborhoods(divisions, open_ground, boundaries, water)
        if primary_name(area["properties"]).casefold() not in taken
    ][:budget]
    taken |= {primary_name(area["properties"]).casefold() for area in points}
    remaining = open_ground
    if points:
        remaining = polygonal(
            remaining.difference(unary_union([valid_geometry(area) for area in points]))
        )
    land_use = list(land_use)
    distinct_land_use = [
        feature
        for feature in land_use
        if primary_name(feature.get("properties") or {}).casefold() not in taken
    ]
    named_land, remaining = land_use_areas(
        distinct_land_use, remaining, budget - len(points)
    )
    chunks = road_areas(
        remaining,
        landmarks=land_use,
        boundaries=boundaries,
        roads=roads,
        road_ranks=road_ranks,
        road_names=road_names,
        water=water,
        place_name=place_name,
        budget=budget - len(points) - len(named_land),
        taken={
            *named,
            *(primary_name(area["properties"]) for area in [*points, *named_land]),
        },
    )
    if not PUBLIC_LOG:
        print(
            f"Open ground filled: {len(points)} from named points, "
            f"{len(named_land)} from named land, {len(chunks)} along roads"
        )
    return [*points, *named_land, *chunks]


def land_use_areas(
    features: Iterable[dict[str, Any]],
    ground: BaseGeometry,
    budget: int,
) -> tuple[list[dict[str, Any]], BaseGeometry]:
    """Named land in [ground] as local areas, and the ground left after them.

    The smallest land claims first, so a golf course inside a large park is
    its own area and the park keeps the rest. A piece under 5 ha is too small
    to explore and is left to the areas around it.
    """
    if budget <= 0 or ground.is_empty:
        return [], ground
    candidates: list[tuple[float, str, str, dict[str, Any], BaseGeometry]] = []
    for feature in features:
        properties = feature.get("properties") or {}
        name = primary_name(properties)
        feature_key = str(feature.get("id") or "")
        if (
            not name
            or not feature_key
            or properties.get("class") in LAND_USE_EXCLUDED_CLASSES
        ):
            continue
        geometry = valid_geometry(feature, required=False)
        if geometry is None or geometry.geom_type not in ("Polygon", "MultiPolygon"):
            continue
        if not geometry.intersects(ground):
            continue
        size = sum(polygon_square_meters(polygon) for polygon in polygons_of(geometry))
        candidates.append((size, name, feature_key, feature, geometry))
    candidates.sort(key=lambda candidate: candidate[:3])
    areas: list[dict[str, Any]] = []
    remaining = ground
    for _, name, feature_key, feature, geometry in candidates:
        if len(areas) >= budget:
            break
        piece = polygonal(polygonal(geometry).intersection(remaining))
        if piece.is_empty:
            continue
        if square_meters_within(piece, piece) < NEIGHBORHOOD_MINIMUM_SQUARE_METERS:
            continue
        remaining = polygonal(remaining.difference(piece))
        areas.append(
            {
                "type": "Feature",
                "id": feature_key,
                "properties": {
                    "names": {"primary": name},
                    "subtype": "neighborhood",
                    "drawn_from_land_use": True,
                },
                "geometry": mapping(piece),
            }
        )
    return areas, remaining


def road_areas(
    ground: BaseGeometry,
    *,
    boundaries: list[BaseGeometry],
    roads: list[tuple[str, BaseGeometry]],
    road_ranks: dict[str, int],
    road_names: dict[str, str],
    water: BaseGeometry | None,
    place_name: str,
    budget: int,
    taken: set[str],
    landmarks: Iterable[dict[str, Any]] = (),
) -> list[dict[str, Any]]:
    """[ground] cut along main roads into compact areas, at most [budget] of
    them, each named after the land that fills much of it, or else after the
    crossroads of the two main roads bounding it."""
    if budget <= 0 or ground.is_empty:
        return []
    if water is not None:
        # A lake is not ground to explore, so no area is drawn on it.
        large_water = [
            polygon
            for polygon in polygons_of(water)
            if polygon_square_meters(polygon) >= POINT_NEIGHBORHOOD_WATER_CUT_SQUARE_METERS
        ]
        if large_water:
            ground = polygonal(ground.difference(unary_union(large_water)))
    if square_meters_within(ground, ground) < ROAD_AREA_MINIMUM_SQUARE_METERS:
        return []
    to_meters, to_degrees = metric_frame(ground.centroid.y)
    cut = [block for block in cut_blocks(ground, boundaries, None, to_meters) if block.area >= 1]
    if not cut:
        return []
    total = sum(block.area for block in cut)

    def split(target: float) -> list[BaseGeometry]:
        """The blocks, with any far larger than [target] split into pieces
        of about [target] around a square lattice of seeds."""
        pieces: list[BaseGeometry] = []
        spacing = math.sqrt(target)
        for block in cut:
            if block.area <= target * ROAD_AREA_SPLIT_FACTOR:
                pieces.append(block)
                continue
            west, south, east, north = block.bounds
            seeds = [
                Point(west + spacing * (column + 0.5), south + spacing * (row + 0.5))
                for row in range(max(1, math.ceil((north - south) / spacing)))
                for column in range(max(1, math.ceil((east - west) / spacing)))
            ]
            seeds = [seed for seed in seeds if block.contains(seed)]
            if len(seeds) < 2:
                pieces.append(block)
                continue
            cells = shapely.voronoi_polygons(MultiPoint(seeds), extend_to=block)
            for cell in cells.geoms:
                pieces.extend(
                    piece for piece in polygons_of(cell.intersection(block)) if piece.area >= 1
                )
        return pieces

    def grow(blocks: list[BaseGeometry], target: float) -> list[list[int]]:
        tree = STRtree(blocks)
        neighbours = [
            [
                int(other)
                for other in tree.query(block, predicate="intersects")
                if int(other) != index
                and block.boundary.intersection(blocks[int(other)].boundary).length > 1
            ]
            for index, block in enumerate(blocks)
        ]
        owner: dict[int, int] = {}
        groups: list[list[int]] = []
        for seed in sorted(range(len(blocks)), key=lambda index: -blocks[index].area):
            if seed in owner:
                continue
            group = [seed]
            owner[seed] = len(groups)
            size = blocks[seed].area
            center = blocks[seed].representative_point()
            frontier = {index for index in neighbours[seed] if index not in owner}
            while size < target and frontier:
                # The closest block keeps the area compact.
                chosen = min(
                    frontier,
                    key=lambda index: (blocks[index].distance(center), index),
                )
                frontier.discard(chosen)
                if chosen in owner:
                    continue
                owner[chosen] = len(groups)
                group.append(chosen)
                size += blocks[chosen].area
                frontier.update(index for index in neighbours[chosen] if index not in owner)
            groups.append(group)
        # A small leftover joins the neighbouring area it shares most with.
        merged = [list(group) for group in groups]
        for index, group in enumerate(groups):
            if sum(blocks[member].area for member in group) >= target / 4:
                continue
            around = [
                owner[other]
                for member in group
                for other in neighbours[member]
                if owner[other] != index and merged[owner[other]]
            ]
            if not around:
                continue
            into = max(set(around), key=lambda candidate: (around.count(candidate), -candidate))
            merged[into].extend(group)
            merged[index] = []
            for member in group:
                owner[member] = into
        return [group for group in merged if group]

    target = max(ROAD_AREA_TARGET_SQUARE_METERS, total / budget)
    blocks = split(target)
    groups = grow(blocks, target)
    while len(groups) > budget:
        target *= 1.5
        blocks = split(target)
        groups = grow(blocks, target)

    road_geometries = [to_meters(geometry) for _, geometry in roads]
    road_tree = STRtree(road_geometries) if road_geometries else None

    def street_names(shape: BaseGeometry) -> set[str]:
        if road_tree is None:
            return set()
        return {roads[int(raw)][0] for raw in road_tree.query(shape, predicate="intersects")}

    shapes = [
        polygonal(unary_union([blocks[member] for member in group])) for group in groups
    ]
    # An area with too few streets joins the neighbour it shares the longest
    # edge with, smallest first, until every area counts enough streets or
    # has no neighbour left to join.
    counts = [len(street_names(shape)) for shape in shapes]
    while True:
        small = [
            index
            for index, shape in enumerate(shapes)
            if not shape.is_empty and counts[index] < ROAD_AREA_MINIMUM_STREETS
        ]
        merged_any = False
        for index in sorted(small, key=lambda index: (counts[index], shapes[index].area)):
            shape = shapes[index]
            if (
                shape.is_empty
                or counts[index] >= ROAD_AREA_MINIMUM_STREETS
                or shape.area >= target
            ):
                continue
            limit = target * ROAD_AREA_MERGE_LIMIT_FACTOR
            shared = [
                (shape.boundary.intersection(other.boundary).length, other_index)
                for other_index, other in enumerate(shapes)
                if other_index != index
                and not other.is_empty
                and other.area + shape.area <= limit
                and shape.touches(other)
            ]
            shared = [item for item in shared if item[0] > 1]
            if not shared:
                continue
            _, into = max(shared)
            shapes[into] = polygonal(unary_union([shapes[into], shape]))
            counts[into] = len(street_names(shapes[into]))
            shapes[index] = shapely.Polygon()
            merged_any = True
        if not merged_any:
            break

    landmark_shapes: list[tuple[str, BaseGeometry, float]] = []
    for feature in landmarks:
        name = primary_name(feature.get("properties") or {})
        geometry = valid_geometry(feature, required=False)
        if not name or geometry is None or geometry.geom_type not in ("Polygon", "MultiPolygon"):
            continue
        in_meters = polygonal(to_meters(geometry))
        if not in_meters.is_empty:
            landmark_shapes.append((name, in_meters, in_meters.area))
    landmark_tree = STRtree([shape for _, shape, _ in landmark_shapes]) if landmark_shapes else None

    used = set(taken)
    whole_center = unary_union(blocks).centroid
    areas: list[dict[str, Any]] = []
    for index, shape_meters in enumerate(shapes):
        if shape_meters.area < ROAD_AREA_MINIMUM_SQUARE_METERS:
            continue
        if (
            shape_meters.area < ROAD_AREA_SLIVER_SQUARE_METERS
            and counts[index] < ROAD_AREA_MINIMUM_STREETS
        ):
            continue
        name = landmark_name(shape_meters, landmark_shapes, landmark_tree, used)
        if name is None:
            name = crossroads_name(
                shape_meters,
                roads=roads,
                road_geometries=road_geometries,
                road_tree=road_tree,
                road_ranks=road_ranks,
                road_names=road_names,
                used=used,
            )
        base = name or f"{place_name} outskirts"
        name = base
        if name in used:
            # A name already taken reads with the side of the place it is on.
            name = f"{base} ({compass_word(whole_center, shape_meters.representative_point())})"
        number = 2
        while name in used:
            name = f"{base} {number}"
            number += 1
        used.add(name)
        geometry = to_degrees(shape_meters)
        center = geometry.representative_point()
        digest = hashlib.sha1(
            f"{name}|{center.x:.3f}|{center.y:.3f}".encode("utf-8")
        ).hexdigest()[:16]
        areas.append(
            {
                "type": "Feature",
                "id": f"road-{digest}",
                "properties": {
                    "names": {"primary": name},
                    "subtype": "neighborhood",
                    "drawn_from_roads": True,
                },
                "geometry": mapping(geometry),
            }
        )
    return areas


def landmark_name(
    shape: BaseGeometry,
    landmarks: list[tuple[str, BaseGeometry, float]],
    tree: STRtree | None,
    used: set[str],
) -> str | None:
    """The name of named land filling much of [shape], such as a park or a
    school, or None. Land far larger than the area would misname it."""
    if tree is None or shape.area <= 0:
        return None
    best: tuple[float, str] | None = None
    for raw in tree.query(shape, predicate="intersects"):
        name, land, size = landmarks[int(raw)]
        if name in used or size > shape.area * LANDMARK_NAMING_MAXIMUM_RATIO:
            continue
        share = land.intersection(shape).area / shape.area
        if share >= LANDMARK_NAMING_SHARE and (best is None or share > best[0]):
            best = (share, name)
    return best[1] if best else None


def short_street_name(written: str) -> str:
    """[written] without its street type word, as people give directions:
    "Courtney Street" reads "Courtney" and "Strada Alexandru Anghel" reads
    "Alexandru Anghel". A name the type word is needed for, such as "Ring
    Road" or "Șoseaua de Centură", keeps it."""
    def keeps(rest: list[str]) -> bool:
        text = " ".join(rest)
        return bool(rest) and (
            len(rest) > 1 or any(character.isdigit() for character in text) or len(text) >= 5
        )

    def word(value: str) -> str:
        return value.lower().rstrip(".")

    words = written.split()
    if (
        len(words) > 1
        and word(words[0]) in LEADING_STREET_TYPE_WORDS
        and word(words[1]) not in NAME_PARTICLES
        and keeps(words[1:])
    ):
        return " ".join(words[1:])
    if len(words) > 1 and word(words[-1]) in DIRECTION_WORDS and keeps(words[:-1]):
        words = words[:-1]
    if len(words) > 1 and word(words[-1]) in STREET_TYPE_WORDS and keeps(words[:-1]):
        words = words[:-1]
    return " ".join(words)


def crossroads_name(
    shape: BaseGeometry,
    *,
    roads: list[tuple[str, BaseGeometry]],
    road_geometries: list[BaseGeometry],
    road_tree: STRtree | None,
    road_ranks: dict[str, int],
    road_names: dict[str, str],
    used: set[str],
) -> str | None:
    """"Courtney & Dewdney": the two main roads along [shape]'s edge, or the
    next pair when that name is taken. One road alone reads "Near Courtney
    Street". None when no named road reaches the area."""
    if road_tree is None:
        return None
    edge = shape.boundary.buffer(ROAD_AREA_NAMING_METERS)
    lengths: dict[str, float] = {}
    for raw in road_tree.query(edge, predicate="intersects"):
        name = roads[int(raw)][0]
        lengths[name] = lengths.get(name, 0.0) + road_geometries[int(raw)].intersection(edge).length
    ranked = sorted(lengths, key=lambda name: (-road_ranks.get(name, 0), -lengths[name], name))
    short = []
    for name in ranked:
        word = short_street_name(road_names.get(name, name))
        if word not in short:
            short.append(word)
    candidates = [
        f"{short[first]} & {short[second]}"
        for total in range(1, len(short) * 2)
        for first in range(len(short))
        for second in range(first + 1, len(short))
        if first + second == total
    ]
    for candidate in candidates:
        if candidate not in used:
            return candidate
    if short:
        single = f"Near {road_names.get(ranked[0], ranked[0])}"
        return single if single not in used else candidates[0] if candidates else None
    return None


def point_neighborhoods(
    divisions: Iterable[dict[str, Any]],
    city_geometry: BaseGeometry,
    boundaries: Iterable[BaseGeometry],
    water: BaseGeometry | None,
) -> list[dict[str, Any]]:
    """Neighbourhood areas drawn from the named neighbourhood points in
    [city_geometry], the ground of a place that no outline covers.

    Main roads, railways, large water and the place's own outline cut it into
    blocks. A block holding one point belongs to that neighbourhood, a block
    holding several is shared between them by distance, and a block holding
    none joins the nearest point within reach. Each area keeps its point's
    Overture ID, so a rebuild names the same ground the same way.
    """
    points: list[tuple[dict[str, Any], BaseGeometry]] = []
    seen: set[str] = set()
    for feature in divisions:
        properties = feature.get("properties") or {}
        feature_key = str(feature.get("id") or "")
        if (
            properties.get("subtype") != "neighborhood"
            or not primary_name(properties)
            or not feature_key
            or feature_key in seen
        ):
            continue
        geometry = valid_geometry(feature, required=False)
        if (
            geometry is None
            or geometry.geom_type != "Point"
            or not city_geometry.contains(geometry)
        ):
            continue
        seen.add(feature_key)
        points.append((feature, geometry))
    if len(points) < POINT_NEIGHBORHOOD_MINIMUM:
        return []

    # Distances are measured in metres on a local plane around the place.
    to_meters, to_degrees = metric_frame(city_geometry.centroid.y)
    blocks = cut_blocks(city_geometry, boundaries, water, to_meters)

    anchors = [to_meters(point) for _, point in points]
    tree = STRtree(anchors)
    parts: list[list[BaseGeometry]] = [[] for _ in points]
    for block in blocks:
        inside = sorted(int(index) for index in tree.query(block, predicate="contains"))
        if len(inside) == 1:
            parts[inside[0]].append(block)
        elif inside:
            cells = shapely.voronoi_polygons(
                MultiPoint([anchors[index] for index in inside]), extend_to=block
            )
            for cell in cells.geoms:
                owner = next(
                    (index for index in inside if cell.contains(anchors[index])), None
                )
                piece = cell.intersection(block)
                if owner is not None and not piece.is_empty:
                    parts[owner].append(piece)
        else:
            center = block.representative_point()
            nearest = int(tree.nearest(center))
            if anchors[nearest].distance(center) <= POINT_NEIGHBORHOOD_REACH_METERS:
                parts[nearest].append(block)

    areas: list[dict[str, Any]] = []
    for (feature, _), pieces in zip(points, parts):
        if not pieces:
            continue
        shape_ = to_degrees(unary_union(pieces)).intersection(city_geometry)
        polygons = list(polygons_of(shape_))
        if not polygons:
            continue
        properties = feature.get("properties") or {}
        areas.append(
            {
                "type": "Feature",
                "id": feature["id"],
                "properties": {
                    "names": properties.get("names"),
                    "subtype": "neighborhood",
                    # Drawn from a point, so its edges follow roads rather
                    # than a surveyed boundary.
                    "drawn_from_point": True,
                },
                "geometry": mapping(unary_union(polygons)),
            }
        )
    areas.sort(key=lambda area: (primary_name(area["properties"]), area["id"]))
    return areas[:250]


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
    # Read before the manifest is replaced, so the lookup tiles the city no
    # longer covers can be removed once the new bundle is published.
    previous_tiles = published_index_tiles(client, bucket_name, city_key)
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
        "tiles": [[x, y] for x, y in tiles],
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
    remove_superseded(
        client,
        bucket_name,
        city_key=city_key,
        bundle_key=bundle_key,
        tiles=set(tiles),
        previous_tiles=previous_tiles,
    )


def published_index_tiles(
    client: Any, bucket_name: str, city_key: str
) -> set[tuple[int, int]]:
    """Lookup tiles the city's published bundle is indexed under.

    The manifest lists them. A manifest written before it did is read from
    the index instead, by listing it once for the city's entries.
    """
    manifest = read_json_object(client, bucket_name, f"manifests/{city_key}.json")
    if manifest is None:
        return set()
    listed = manifest.get("tiles")
    if isinstance(listed, list):
        return {(int(x), int(y)) for x, y in listed}
    suffix = f"/{city_key}.json"
    tiles: set[tuple[int, int]] = set()
    for key, _ in list_objects(client, bucket_name, f"index/{INDEX_ZOOM}/"):
        if key.endswith(suffix):
            _, _, x, y, _ = key.split("/")
            tiles.add((int(x), int(y)))
    return tiles


def remove_superseded(
    client: Any,
    bucket_name: str,
    *,
    city_key: str,
    bundle_key: str,
    tiles: set[tuple[int, int]],
    previous_tiles: set[tuple[int, int]],
) -> None:
    """Deletes the city's older bundles and the lookup tiles it left.

    Every rebuild writes a new bundle beside the old ones, so without this the
    bucket grew by the whole set of bundles every 35 days and on every
    revision. Only the build whose bundle the manifest names cleans up, and
    only bundles written before its own, so two builds of one city finishing
    together never delete the bundle the other one indexed.
    """
    manifest = read_json_object(client, bucket_name, f"manifests/{city_key}.json")
    if manifest is None or manifest.get("bundleKey") != bundle_key:
        return
    bundles = dict(list_objects(client, bucket_name, f"bundles/{city_key}/"))
    published_at = bundles.get(bundle_key)
    if published_at is None:
        return
    keys = [
        key
        for key, modified in bundles.items()
        if key != bundle_key and modified < published_at
    ]
    keys.extend(
        f"index/{INDEX_ZOOM}/{x}/{y}/{city_key}.json"
        for x, y in sorted(previous_tiles - tiles)
    )
    # A build leaves a few keys at most, and single deletes are the call the
    # builder already makes against R2.
    for key in keys:
        client.delete_object(Bucket=bucket_name, Key=key)


def read_json_object(client: Any, bucket_name: str, key: str) -> dict[str, Any] | None:
    try:
        response = client.get_object(Bucket=bucket_name, Key=key)
    except client.exceptions.NoSuchKey:
        return None
    value = json.loads(response["Body"].read())
    return value if isinstance(value, dict) else None


def list_objects(client: Any, bucket_name: str, prefix: str) -> Iterable[tuple[str, Any]]:
    """Key and last modified time of every object under [prefix]."""
    for page in client.get_paginator("list_objects_v2").paginate(
        Bucket=bucket_name, Prefix=prefix
    ):
        for entry in page.get("Contents", []):
            yield entry["Key"], entry["LastModified"]


def forget_failed_request(request_id: str | None) -> None:
    """Deletes the stored request of a build that failed.

    A published build deletes it, but a failed one left it, and with it the
    explorer's coordinate, in the bucket for good. The request's marker is
    kept, so the worker still waits out its cooldown instead of starting a
    build on every retry of the app.
    """
    if request_id is None or not re.fullmatch(r"[0-9a-f-]{36}", request_id):
        return
    try:
        r2_client().delete_object(
            Bucket=required_environment("R2_BUCKET"),
            Key=f"requests/by-id/{request_id}.json",
        )
    except Exception as error:
        print(f"Request cleanup failed: {type(error).__name__}")


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
        forget_failed_request(REQUEST_ID)
        sys.exit(1)
