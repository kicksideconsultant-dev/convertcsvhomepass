from fastapi import FastAPI, UploadFile, File, HTTPException, Form
from fastapi.responses import HTMLResponse, Response
from fastapi.templating import Jinja2Templates
from fastapi import Request

from lxml import etree
from shapely.geometry import Point, Polygon
from shapely.prepared import prep

import pandas as pd
import zipfile, io
import requests
import sqlite3
import time
import os

app = FastAPI()
templates = Jinja2Templates(directory="templates")

KML_NS = {"kml": "http://www.opengis.net/kml/2.2"}

# --- Simple SQLite cache for reverse geocoding ---
CACHE_DB = os.environ.get("GEOCACHE_DB", "geocache.sqlite")
NOMINATIM_THROTTLE_SEC = float(os.environ.get("NOMINATIM_THROTTLE_SEC", "1.1"))
USER_AGENT = os.environ.get("NOMINATIM_USER_AGENT", "kmz2csv/1.0 (contact: your-email@example.com)")

def cache_init():
    with sqlite3.connect(CACHE_DB) as con:
        con.execute("""
          CREATE TABLE IF NOT EXISTS geocache (
            lat_round TEXT NOT NULL,
            lon_round TEXT NOT NULL,
            road TEXT,
            raw_json TEXT,
            PRIMARY KEY (lat_round, lon_round)
          )
        """)
cache_init()

def extract_kml_bytes(upload_bytes: bytes, filename: str) -> bytes:
    name = (filename or "").lower()
    if name.endswith(".kml"):
        return upload_bytes
    if name.endswith(".kmz"):
        z = zipfile.ZipFile(io.BytesIO(upload_bytes))
        kml_files = [n for n in z.namelist() if n.lower().endswith(".kml")]
        if not kml_files:
            raise HTTPException(400, "KMZ tidak berisi file .kml")
        return z.read(kml_files[0])
    raise HTTPException(400, "File harus .kmz atau .kml")

def parse_extended_data(pm) -> dict:
    out = {}
    ext = pm.find(".//kml:ExtendedData", namespaces=KML_NS)
    if ext is None:
        return out

    # ExtendedData/Data
    for d in ext.findall(".//kml:Data", namespaces=KML_NS):
        key = d.get("name")
        val_el = d.find("kml:value", namespaces=KML_NS)
        out[key] = (val_el.text if val_el is not None else None)

    # ExtendedData/SchemaData/SimpleData
    for sd in ext.findall(".//kml:SchemaData", namespaces=KML_NS):
        for s in sd.findall(".//kml:SimpleData", namespaces=KML_NS):
            out[s.get("name")] = s.text

    return out

def parse_polygons(root):
    polygons = []
    for pm in root.findall(".//kml:Placemark", namespaces=KML_NS):
        poly_el = pm.find(".//kml:Polygon", namespaces=KML_NS)
        if poly_el is None:
            continue

        name_el = pm.find("kml:name", namespaces=KML_NS)
        bname = name_el.text.strip() if name_el is not None and name_el.text else "UNKNOWN_BOUNDARY"

        coords_el = pm.find(".//kml:outerBoundaryIs//kml:LinearRing//kml:coordinates", namespaces=KML_NS)
        if coords_el is None or not (coords_el.text or "").strip():
            continue

        coords = []
        for token in coords_el.text.strip().split():
            lon, lat, *_ = token.split(",")
            coords.append((float(lon), float(lat)))

        poly = Polygon(coords)
        polygons.append((bname, prep(poly), poly))

    return polygons

def parse_points(root):
    rows = []
    for pm in root.findall(".//kml:Placemark", namespaces=KML_NS):
        pt_el = pm.find(".//kml:Point", namespaces=KML_NS)
        if pt_el is None:
            continue

        name_el = pm.find("kml:name", namespaces=KML_NS)
        hp_name = name_el.text.strip() if name_el is not None and name_el.text else None

        coords_el = pm.find(".//kml:Point/kml:coordinates", namespaces=KML_NS)
        if coords_el is None or not (coords_el.text or "").strip():
            continue

        lon, lat, *_ = coords_el.text.strip().split(",")
        lon, lat = float(lon), float(lat)

        data = parse_extended_data(pm)
        rows.append({"homepass": hp_name, "lat": lat, "lon": lon, **data})
    return rows

def assign_boundary(lat: float, lon: float, polygons) -> str | None:
    p = Point(lon, lat)
    for bname, ppoly, poly in polygons:
        # contains untuk di dalam; touches untuk titik tepat di tepi
        if ppoly.contains(p) or poly.touches(p):
            return bname
    # sesuai requirement: di luar boundary => NULL
    return None

def reverse_geocode_nominatim_cached(lat: float, lon: float) -> str | None:
    # round koordinat untuk cache (biar titik berdekatan tidak hit API berulang)
    lat_r = f"{lat:.5f}"
    lon_r = f"{lon:.5f}"

    with sqlite3.connect(CACHE_DB) as con:
        cur = con.execute("SELECT road FROM geocache WHERE lat_round=? AND lon_round=?", (lat_r, lon_r))
        row = cur.fetchone()
        if row is not None:
            return row[0]

    # Hit Nominatim
    url = "https://nominatim.openstreetmap.org/reverse"
    params = {"format": "jsonv2", "lat": lat, "lon": lon, "zoom": 18, "addressdetails": 1}
    headers = {"User-Agent": USER_AGENT}

    r = requests.get(url, params=params, headers=headers, timeout=20)
    r.raise_for_status()
    j = r.json()
    addr = j.get("address", {})
    road = addr.get("road") or addr.get("residential") or addr.get("pedestrian")

    with sqlite3.connect(CACHE_DB) as con:
        con.execute(
            "INSERT OR REPLACE INTO geocache(lat_round, lon_round, road, raw_json) VALUES (?,?,?,?)",
            (lat_r, lon_r, road, str(j)[:2000])
        )

    time.sleep(NOMINATIM_THROTTLE_SEC)  # throttle sederhana
    return road

@app.get("/", response_class=HTMLResponse)
def home(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})

@app.post("/convert")
async def convert(
    file: UploadFile = File(...),
    with_geocode: str | None = Form(default=None)
):
    raw = await file.read()
    kml_bytes = extract_kml_bytes(raw, file.filename)

    try:
        root = etree.fromstring(kml_bytes)
    except Exception:
        raise HTTPException(400, "KML tidak valid / gagal diparse.")

    polygons = parse_polygons(root)
    points = parse_points(root)

    if not points:
        raise HTTPException(400, "Tidak ada titik (Point/homepass) ditemukan di file.")

    df = pd.DataFrame(points)
    df["boundary"] = df.apply(lambda r: assign_boundary(r["lat"], r["lon"], polygons), axis=1)

    do_geocode = (with_geocode is not None)
    if do_geocode:
        streets = []
        for _, r in df.iterrows():
            try:
                streets.append(reverse_geocode_nominatim_cached(float(r["lat"]), float(r["lon"])))
            except Exception:
                streets.append(None)
        df["nama_jalan"] = streets

    # kolom inti di depan
    front = ["homepass", "lat", "lon", "nama_jalan", "boundary"]
    cols = [c for c in front if c in df.columns] + [c for c in df.columns if c not in front]
    df = df[cols]

    csv_bytes = df.to_csv(index=False).encode("utf-8")
    return Response(
        content=csv_bytes,
        media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": 'attachment; filename="output.csv"'},
    )
