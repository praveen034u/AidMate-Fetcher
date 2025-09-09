import os, json, hashlib, logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import requests
from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel

app = FastAPI(title="AidMate Crisis Fetcher")
logging.basicConfig(level=logging.INFO)
UTC = timezone.utc

# --- Config via env ---
INGEST_URL = os.environ.get("INGEST_URL")                   # your existing FastAPI /ingest JSON endpoint
NWS_ALERTS_URL = "https://api.weather.gov/alerts"
USGS_GEOJSON_HOURLY = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_hour.geojson"
NHC_CURRENT = "https://www.nhc.noaa.gov/CurrentStorms.json"
FIRMS_API_KEY = os.environ.get("FIRMS_API_KEY", "")         # NASA FIRMS MAP_KEY
AIRNOW_KEY = os.environ.get("AIRNOW_KEY", "")               # AirNow API key

DEFAULT_STATES = (os.environ.get("STATES", "CT,NJ,NY,MA,PA")).split(",")

def _now_iso() -> str:
    return datetime.now(UTC).isoformat()

def sha16(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()[:16]

def post_chunks(chunks: List[Dict[str, Any]]):
    if not INGEST_URL:
        raise RuntimeError("INGEST_URL is not set.")
    # Expect your ingest endpoint to accept a JSON list of chunks
    r = requests.post(INGEST_URL, json=chunks, timeout=60)
    if r.status_code >= 300:
        raise HTTPException(r.status_code, f"Ingest failed: {r.text}")
    return r.json() if r.text else {"status": "ok"}

# -------------------- Pullers --------------------

def pull_usgs_earthquakes(min_mag: float = 2.5) -> List[Dict[str, Any]]:
    r = requests.get(USGS_GEOJSON_HOURLY, timeout=20)
    r.raise_for_status()
    data = r.json()
    out = []
    for f in data.get("features", []):
        props = f.get("properties", {})
        mag = props.get("mag")
        if mag is None or (min_mag and mag < min_mag):
            continue
        place = props.get("place")
        time_ms = props.get("time")
        url = props.get("url")
        lon, lat, depth = f["geometry"]["coordinates"]
        issued = datetime.fromtimestamp(time_ms/1000, UTC).isoformat()
        text = (f"M{mag:.1f} earthquake near {place} at {issued} (depth {depth} km). "
                f"Guidance: Drop, Cover, Hold On. More: {url}")
        out.append({
            "id": sha16(f"eq-{time_ms}-{lat}-{lon}"),
            "crisis": "earthquake",
            "source": "USGS",
            "issued_at": issued,
            "expires_at": None,
            "region": [place] if place else [],
            "lat": lat, "lon": lon,
            "severity": None,
            "language": "en",
            "url": url,
            "text": text
        })
    return out

def pull_nws_alerts(states: List[str]) -> List[Dict[str, Any]]:
    # https://api.weather.gov/alerts
    # Geolocation nuances: zone vs county (see NWS docs) – we fetch state codes directly. :contentReference[oaicite:5]{index=5}
    base_hdr = {"User-Agent": "AidMate/1.0 (contact: aidmate@example.com)"}
    out = []
    # NWS supports comma-separated "area" state codes
    params = {"status": "actual", "active": "true", "limit": 200, "area": ",".join(states)}
    r = requests.get(NWS_ALERTS_URL, params=params, headers=base_hdr, timeout=25)
    r.raise_for_status()
    data = r.json()
    for feat in data.get("features", []):
        p = feat.get("properties", {})
        headline = p.get("headline") or p.get("event")
        desc = p.get("description") or ""
        sev  = p.get("severity")
        onset= p.get("onset") or p.get("effective")
        expires = p.get("expires")
        area_desc = p.get("areaDesc")
        detail = p.get("@id") or p.get("id")
        event = (p.get("event") or "").lower()
        # map to crisis
        if "hurricane" in event or "tropical" in event:
            crisis = "hurricane"
        elif "flood" in event:
            crisis = "flood"
        else:
            crisis = "severe_weather"
        text = f"{headline}\nSeverity: {sev}\nArea: {area_desc}\nOnset: {onset}\nExpires: {expires}\n\n{desc}\nMore: {detail}"
        out.append({
            "id": sha16(detail or headline),
            "crisis": crisis,
            "source": "NWS",
            "issued_at": onset,
            "expires_at": expires,
            "region": [area_desc] if area_desc else [],
            "lat": None, "lon": None,
            "severity": sev,
            "language": "en",
            "url": detail,
            "text": text
        })
    return out

def pull_nhc_current() -> List[Dict[str, Any]]:
    # https://www.nhc.noaa.gov/CurrentStorms.json (file spec in PDF) :contentReference[oaicite:6]{index=6}
    try:
        r = requests.get(NHC_CURRENT, timeout=20)
        r.raise_for_status()
        data = r.json()
    except Exception as e:
        logging.warning(f"NHC fetch failed: {e}")
        return []
    out = []
    for s in data.get("activeStorms", []):
        name = s.get("name")
        basin = s.get("basin")
        adv = s.get("advisoryNumber")
        prods = s.get("products", {})
        text = f"Active tropical system {name} in {basin}. Latest advisory #{adv}. Products: {', '.join(prods.keys())}."
        out.append({
            "id": sha16(f"{name}-{basin}-{adv}"),
            "crisis": "hurricane",
            "source": "NHC",
            "issued_at": _now_iso(),
            "expires_at": None,
            "region": [basin] if basin else [],
            "lat": None, "lon": None,
            "severity": None,
            "language": "en",
            "url": NHC_CURRENT,
            "text": text
        })
    return out

def pull_firms_us(days: int = 1, limit_rows: int = 200) -> List[Dict[str, Any]]:
    # API ref + Python tutorial :contentReference[oaicite:7]{index=7}
    if not FIRMS_API_KEY:
        return []
    url = f"https://firms.modaps.eosdis.nasa.gov/api/country/csv/{FIRMS_API_KEY}/VIIRS_NOAA20_NRT/{days}/USA"
    r = requests.get(url, timeout=25)
    out = []
    if r.status_code == 200 and "latitude" in r.text[:200].lower():
        lines = r.text.splitlines()
        header = [h.strip() for h in lines[0].split(",")]
        ilat = header.index("latitude"); ilon = header.index("longitude")
        iconf = header.index("confidence") if "confidence" in header else None
        for row in lines[1:1+limit_rows]:
            cols = [c.strip() for c in row.split(",")]
            lat = float(cols[ilat]); lon = float(cols[ilon])
            conf = cols[iconf] if iconf is not None else "NA"
            text = f"Active fire detection (VIIRS) at lat {lat:.3f}, lon {lon:.3f}, confidence {conf}."
            out.append({
                "id": sha16(f"firms-{lat}-{lon}-{conf}-{days}"),
                "crisis": "wildfire",
                "source": "NASA_FIRMS",
                "issued_at": _now_iso(),
                "expires_at": None,
                "region": ["USA"],
                "lat": lat, "lon": lon,
                "severity": None,
                "language": "en",
                "url": url,
                "text": text
            })
    return out

def pull_airnow(lat: float, lon: float, dist_km: int = 50) -> List[Dict[str, Any]]:
    # AirNow current obs by lat/long :contentReference[oaicite:8]{index=8}
    if not AIRNOW_KEY:
        return []
    base = "https://www.airnowapi.org/aq/observation/latLong/current/"
    params = {"format":"application/json","latitude":lat,"longitude":lon,"distance":dist_km,"API_KEY":AIRNOW_KEY}
    r = requests.get(base, params=params, timeout=20)
    out = []
    if r.status_code == 200:
        for item in r.json():
            txt = (f"Air quality {item['ParameterName']} AQI {item['AQI']} at {item['DateObserved']} "
                   f"{item['HourObserved']}:00 ({item['Category']['Name']}).")
            out.append({
                "id": sha16(f"airnow-{item['ParameterName']}-{item['DateObserved']}-{item['HourObserved']}-{lat}-{lon}"),
                "crisis": "wildfire",
                "source": "AirNow",
                "issued_at": f"{item['DateObserved']}T{int(item['HourObserved']):02d}:00:00Z",
                "expires_at": None,
                "region": [f"{lat:.3f},{lon:.3f}"],
                "lat": lat, "lon": lon,
                "severity": item["Category"]["Name"],
                "language": "en",
                "url": base,
                "text": txt
            })
    return out

# -------------------- API Models & endpoint --------------------

class CronParams(BaseModel):
    states: Optional[List[str]] = None        # e.g., ["CT","NJ"]
    min_mag: float = 2.5
    air_lat: Optional[float] = None
    air_lon: Optional[float] = None
    pull_earthquakes: bool = True
    pull_nws: bool = True
    pull_nhc: bool = True
    pull_firms: bool = True
    pull_airnow: bool = True

@app.get("/health")
def health():
    return {"ok": True, "time": _now_iso()}

@app.post("/cron/pull")
def cron_pull(body: CronParams):
    states = body.states or DEFAULT_STATES
    chunks: List[Dict[str, Any]] = []
    if body.pull_earthquakes:
        chunks += pull_usgs_earthquakes(min_mag=body.min_mag)
    if body.pull_nws:
        chunks += pull_nws_alerts(states)
    if body.pull_nhc:
        chunks += pull_nhc_current()
    if body.pull_firms:
        chunks += pull_firms_us()
    if body.pull_airnow and body.air_lat and body.air_lon:
        chunks += pull_airnow(body.air_lat, body.air_lon)

    if not chunks:
        return {"added": 0, "note": "no chunks produced (check keys/flags)"}

    # Send to your existing ingest API
    # res = post_chunks(chunks)
    # return {"added": len(chunks), "ingest_result": res}

    return {"added": len(chunks), "chunks": chunks[:2]}  # preview first 2