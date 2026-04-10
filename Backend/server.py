#!/usr/bin/env python3
"""
Movie Collection Backend Server

Load / save strategy:
  PRIMARY   — Firebase Firestore (via REST API, no service account needed)
  FALLBACK  — Local file  Data/movieCollection.nosql.json

On every successful save the local seed is also updated as a backup,
so the fallback is always current.
"""

import json
import logging
import os
import re
from pathlib import Path
from datetime import datetime

import requests
from flask import Flask, request, jsonify
from flask_cors import CORS

# ---------------------------------------------------------------------------
# Logging & app
# ---------------------------------------------------------------------------
logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(message)s")
logger = logging.getLogger(__name__)

app = Flask(__name__)
CORS(app)

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parent
DATA_DIR     = PROJECT_ROOT / "Data"
SEED_FILE    = DATA_DIR / "movieCollection.nosql.json"

# ---------------------------------------------------------------------------
# Firebase / Firestore config  (REST API – no service account required)
# ---------------------------------------------------------------------------
FIREBASE_PROJECT_ID   = "movieproject-6d41f"
FIREBASE_API_KEY      = "AIzaSyDcXCfUIPNoNB7waG9gIONFyyYqQ7cRGgo"
FIREBASE_CONFIG       = {
    "apiKey":            FIREBASE_API_KEY,
    "authDomain":        f"{FIREBASE_PROJECT_ID}.firebaseapp.com",
    "projectId":         FIREBASE_PROJECT_ID,
    "storageBucket":     f"{FIREBASE_PROJECT_ID}.firebasestorage.app",
    "messagingSenderId": "335848803031",
    "appId":             "1:335848803031:web:ebc73092a5af7e0cfcbe92",
}

FIRESTORE_BASE        = (
    f"https://firestore.googleapis.com/v1/projects/{FIREBASE_PROJECT_ID}"
    f"/databases/(default)/documents"
)
FIRESTORE_ITEMS_COL   = "movieCollectionItems"
FIRESTORE_META_COL    = "movieCollectionMeta"
FIRESTORE_META_DOC_ID = "app"
FIRESTORE_TIMEOUT     = 12    # seconds per HTTP request
FIRESTORE_BATCH_SIZE  = 499   # Firestore max is 500 per batchWrite

# ---------------------------------------------------------------------------
# In-memory seed cache
# ---------------------------------------------------------------------------
_seed_cache = None

# ---------------------------------------------------------------------------
# OMDb config/state (backend-owned)
# ---------------------------------------------------------------------------
RESOURCE_FILE = DATA_DIR / "en.json"
OMDB_KEYS_FILE = DATA_DIR / "omdbkeys"
_omdb_api_keys = []
_omdb_keys_file_mtime = None
_resource_catalog = {}
_resource_file_mtime = None


# ===========================================================================
# Firestore REST API helpers
# ===========================================================================

def _api_params():
    return {"key": FIREBASE_API_KEY}


def _doc_name(collection, doc_id):
    return f"{FIRESTORE_BASE}/{collection}/{doc_id}"


# -- value converters -------------------------------------------------------

def _to_fv(value):
    """Python value -> Firestore REST field-value dict."""
    if value is None:
        return {"nullValue": None}
    if isinstance(value, bool):
        return {"booleanValue": value}
    if isinstance(value, int):
        return {"integerValue": str(value)}
    if isinstance(value, float):
        return {"doubleValue": value}
    if isinstance(value, str):
        return {"stringValue": value}
    if isinstance(value, list):
        return {"arrayValue": {"values": [_to_fv(v) for v in value]}}
    if isinstance(value, dict):
        return {"mapValue": {"fields": {k: _to_fv(v) for k, v in value.items()}}}
    return {"stringValue": str(value)}


def _from_fv(fv):
    """Firestore REST field-value dict -> Python value."""
    if not isinstance(fv, dict):
        return fv
    if "nullValue"    in fv: return None
    if "booleanValue" in fv: return bool(fv["booleanValue"])
    if "integerValue" in fv: return int(fv["integerValue"])
    if "doubleValue"  in fv: return float(fv["doubleValue"])
    if "stringValue"  in fv: return fv["stringValue"]
    if "arrayValue"   in fv:
        return [_from_fv(v) for v in fv["arrayValue"].get("values", [])]
    if "mapValue"     in fv:
        return {k: _from_fv(v) for k, v in fv["mapValue"].get("fields", {}).items()}
    return None


def _doc_to_dict(doc):
    return {k: _from_fv(v) for k, v in doc.get("fields", {}).items()}


def _dict_to_fields(data):
    return {k: _to_fv(v) for k, v in data.items()}


# -- CRUD -------------------------------------------------------------------

def fs_get_doc(collection, doc_id):
    """Fetch one document. Returns dict or None if not found."""
    url  = f"{FIRESTORE_BASE}/{collection}/{doc_id}"
    resp = requests.get(url, params=_api_params(), timeout=FIRESTORE_TIMEOUT)
    if resp.status_code == 404:
        return None
    resp.raise_for_status()
    return _doc_to_dict(resp.json())


def fs_set_doc(collection, doc_id, data):
    """Create or overwrite a document."""
    url  = f"{FIRESTORE_BASE}/{collection}/{doc_id}"
    body = {"fields": _dict_to_fields(data)}
    resp = requests.patch(url, json=body, params=_api_params(), timeout=FIRESTORE_TIMEOUT)
    resp.raise_for_status()


def fs_list_docs(collection):
    """List all documents in a collection (handles pagination).
    Returns list of {"id": str, "data": dict}."""
    url    = f"{FIRESTORE_BASE}/{collection}"
    params = {**_api_params(), "pageSize": 300}
    docs   = []
    while True:
        resp = requests.get(url, params=params, timeout=FIRESTORE_TIMEOUT * 3)
        resp.raise_for_status()
        body = resp.json()
        for doc in body.get("documents", []):
            did = doc["name"].split("/")[-1]
            docs.append({"id": did, "data": _doc_to_dict(doc)})
        page_token = body.get("nextPageToken")
        if not page_token:
            break
        params["pageToken"] = page_token
    return docs


def fs_batch_write(writes):
    """Execute Firestore batchWrite in safe chunks."""
    if not writes:
        return
    url = (
        f"https://firestore.googleapis.com/v1/projects/{FIREBASE_PROJECT_ID}"
        f"/databases/(default)/documents:batchWrite"
    )
    for i in range(0, len(writes), FIRESTORE_BATCH_SIZE):
        chunk = writes[i: i + FIRESTORE_BATCH_SIZE]
        resp  = requests.post(
            url, json={"writes": chunk}, params=_api_params(), timeout=60
        )
        resp.raise_for_status()
        logger.info(f"  batchWrite: {i + len(chunk)}/{len(writes)} docs committed")


def fs_is_available():
    """Quick connectivity check."""
    try:
        url  = f"{FIRESTORE_BASE}/{FIRESTORE_META_COL}/{FIRESTORE_META_DOC_ID}"
        resp = requests.get(url, params=_api_params(), timeout=FIRESTORE_TIMEOUT)
        return resp.status_code in (200, 404)
    except Exception:
        return False


# ===========================================================================
# Local seed helpers
# ===========================================================================

def seed_load():
    global _seed_cache
    if _seed_cache is not None:
        return _seed_cache
    if not SEED_FILE.exists():
        raise FileNotFoundError(f"Seed file not found: {SEED_FILE}")
    with open(SEED_FILE, "r", encoding="utf-8") as f:
        _seed_cache = json.load(f)
    logger.info(f"Seed loaded from disk ({SEED_FILE.stat().st_size // 1024} KB)")
    return _seed_cache


def seed_save(seed_data):
    global _seed_cache
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    with open(SEED_FILE, "w", encoding="utf-8") as f:
        json.dump(seed_data, f, indent=2)
    _seed_cache = seed_data
    logger.info("Seed written to disk")


def seed_parts(seed):
    """Return (meta_entry, item_entries) from seed structure."""
    meta_entries = seed.get("collections", {}).get(FIRESTORE_META_COL, [])
    item_entries = seed.get("collections", {}).get(FIRESTORE_ITEMS_COL, [])
    meta_entry   = next(
        (e for e in meta_entries if e.get("id") == FIRESTORE_META_DOC_ID),
        meta_entries[0] if meta_entries else None,
    )
    return meta_entry, item_entries


def _normalize_text(value):
    return str(value or "").strip()


def _resource_value(resource_key, default=""):
    current = _resource_catalog
    for part in resource_key.split("."):
        if not isinstance(current, dict) or part not in current:
            return default
        current = current[part]
    return current if isinstance(current, str) else default


def _resource_text(resource_key, default="", **params):
    template = _resource_value(resource_key, default)
    if params and isinstance(template, str):
        try:
            return template.format(**params)
        except Exception:
            return template
    return template


def _load_resource_catalog_from_file():
    if not RESOURCE_FILE.exists():
        logger.warning(_resource_text(
            "backend.resources.missingFile",
            "Resource file not found: {path}",
            path=RESOURCE_FILE,
        ))
        return {}

    try:
        with open(RESOURCE_FILE, "r", encoding="utf-8") as handle:
            data = json.load(handle)
        return data if isinstance(data, dict) else {}
    except Exception as ex:
        logger.error(f"Failed to load resource catalog from {RESOURCE_FILE}: {ex}")
        return {}


def _refresh_resource_catalog_if_needed(force=False):
    global _resource_catalog, _resource_file_mtime

    try:
        current_mtime = RESOURCE_FILE.stat().st_mtime if RESOURCE_FILE.exists() else None
    except Exception:
        current_mtime = None

    if not force and current_mtime == _resource_file_mtime and _resource_catalog:
        return

    _resource_catalog = _load_resource_catalog_from_file()
    _resource_file_mtime = current_mtime


def _parse_key_list(raw):
    if not raw:
        return []
    if isinstance(raw, list):
        return [_normalize_text(v) for v in raw if _normalize_text(v)]
    return [
        _normalize_text(v)
        for v in re.split(r"[\n,;\s]+", str(raw))
        if _normalize_text(v)
    ]


def _unique_preserve_order(values):
    out = []
    seen = set()
    for v in values:
        if not v or v in seen:
            continue
        seen.add(v)
        out.append(v)
    return out


def _load_omdb_api_keys_from_file():
    if not OMDB_KEYS_FILE.exists():
        return []

    try:
        raw_text = OMDB_KEYS_FILE.read_text(encoding="utf-8")
        if not raw_text.strip():
            return []
        return _unique_preserve_order(_parse_key_list(raw_text.splitlines()))
    except Exception as ex:
        logger.error(f"Failed to load OMDb keys from {OMDB_KEYS_FILE}: {ex}")
        return []


def _refresh_omdb_api_keys_if_needed(force=False):
    """Reload keys when omdbkeys file changes so server restart isn't required."""
    global _omdb_api_keys, _omdb_keys_file_mtime

    try:
        current_mtime = OMDB_KEYS_FILE.stat().st_mtime if OMDB_KEYS_FILE.exists() else None
    except Exception:
        current_mtime = None

    if not force and current_mtime == _omdb_keys_file_mtime and _omdb_api_keys:
        return

    reloaded = _load_omdb_api_keys_from_file()
    _omdb_api_keys = reloaded
    _omdb_keys_file_mtime = current_mtime


def _promote_omdb_key(api_key):
    """Move a working key to the front so subsequent calls use it by default."""
    global _omdb_api_keys
    if not api_key or api_key not in _omdb_api_keys:
        return
    _omdb_api_keys = [api_key] + [k for k in _omdb_api_keys if k != api_key]


def initialize_omdb_api_keys():
    """Load OMDb keys from Data/omdbkeys (one key per line) at startup."""
    _refresh_omdb_api_keys_if_needed(force=True)
    if _omdb_api_keys:
        logger.info(f"Loaded {len(_omdb_api_keys)} OMDb key(s) from {OMDB_KEYS_FILE}")
        return

    logger.warning(
        f"No OMDb keys available. Create {OMDB_KEYS_FILE} with one API key per line."
    )


def get_omdb_api_keys():
    _refresh_omdb_api_keys_if_needed(force=False)
    return list(_omdb_api_keys)


# ===========================================================================
# Row / item conversion helpers
# ===========================================================================

def make_doc_id(sheet_type, row_index):
    return f"{sheet_type}_{str(row_index).zfill(6)}"


def _header_map(header_row):
    return {str(v).strip().lower(): i for i, v in enumerate(header_row or []) if str(v).strip()}


def _get_by_alias(header_map, aliases):
    for alias in aliases:
        idx = header_map.get(alias)
        if idx is not None:
            return idx
    return None


def _compact_metadata(metadata_json):
    raw = str(metadata_json or "").strip()
    if not raw:
        return ""
    try:
        parsed = json.loads(raw)
        if isinstance(parsed, (dict, list)):
            return json.dumps(parsed, separators=(",", ":"), ensure_ascii=False)
    except Exception:
        pass
    return raw


def item_to_row(item, header_row):
    """Build a sheet row array from normalized item fields."""
    hmap = _header_map(header_row)
    row_len = max(len(header_row or []), 15)
    row = [""] * row_len

    def set_cell(value, aliases, fallback_idx=None):
        idx = _get_by_alias(hmap, aliases)
        if idx is None:
            idx = fallback_idx
        if idx is None:
            return
        while len(row) <= idx:
            row.append("")
        row[idx] = "" if value is None else str(value)

    quality = str(item.get("Quality") or "").strip().lower()
    marker = "y" if quality == "hevc" else ("\u2713" if quality == "x264" else "")

    set_cell(item.get("filename", ""), ["filename", "file name", "file", "filepath", "file path"], 0)
    set_cell(item.get("title", ""), ["title", "movie title", "name"], 1)
    set_cell(item.get("year", ""), ["year", "release year"], 2)
    set_cell(item.get("resolution", ""), ["resolution"], 4)
    set_cell(item.get("Movie Type", ""), ["language"], 5)
    set_cell(item.get("storageCase", ""), ["storage case", "case", "storagecase"], 6)
    set_cell(item.get("storageHdd", ""), ["storage hdd", "hdd", "storagehdd"], 7)
    set_cell(marker, ["marker", "codec", "quality marker", "x264/hevc"], 8)
    set_cell(item.get("myRating", ""), ["my rating", "rating", "imdb rating", "myrating"], 11)
    set_cell(item.get("storageSize", ""), ["storage size", "size", "storagesize"], 13)
    set_cell(_compact_metadata(item.get("metadataJson", "")), ["metadata", "metadatajson", "omdb"], 14)

    return row


def rows_from_items(meta, items):
    """Convert flat item list -> sheet row arrays."""
    movie_header = (meta or {}).get("movieHeaderRow", [])
    series_header = (meta or {}).get("seriesHeaderRow", [])

    movie_rows  = [movie_header] if meta else [[]]
    series_rows = []
    if meta and meta.get("seriesSheetName"):
        series_rows = [series_header]

    for item in sorted(items, key=lambda x: (x.get("sheetType", ""), x.get("rowIndex", 0))):
        target = series_rows if item.get("sheetType") == "series" else movie_rows
        header = series_header if item.get("sheetType") == "series" else movie_header
        row_index = int(item.get("rowIndex", 0) or 0)
        while len(target) <= row_index:
            target.append([])
        target[row_index] = item_to_row(item, header)

    return {
        "movieRows":       movie_rows,
        "seriesRows":      series_rows,
        "moviesSheetName": (meta or {}).get("moviesSheetName", "movies"),
        "seriesSheetName": (meta or {}).get("seriesSheetName", ""),
        "sourceFileName":  (meta or {}).get("sourceFileName", "Local Seed"),
    }


def parse_rows(rows, sheet_type):
    """Parse sheet rows into normalized item dicts."""
    if not rows or len(rows) < 2:
        return []
    headers    = [str(v).strip().lower() for v in rows[0]]
    header_map = {h: i for i, h in enumerate(headers) if h}
    items      = []

    for i in range(1, len(rows)):
        row = rows[i] or []
        if all(not str(c).strip() for c in row):
            continue

        filename = ""
        for key in ["filename", "file name", "file", "filepath", "file path"]:
            idx = header_map.get(key)
            if idx is not None and idx < len(row) and str(row[idx]).strip():
                filename = str(row[idx]).strip()
                break
        if not filename and row:
            filename = str(row[0]).strip()

        title = str(row[1]).strip() if len(row) > 1 else ""
        if not title and not filename:
            continue

        marker  = str(row[8]).strip() if len(row) > 8 else ""
        quality = "hevc" if marker.lower() == "y" else ("x264" if marker == "\u2713" else "")

        poster, metadata_json = "", ""
        if len(row) > 14:
            col_o = str(row[14]).strip()
            if col_o:
                try:
                    obj = json.loads(col_o)
                    if isinstance(obj, dict):
                        poster        = obj.get("poster", "") or obj.get("Poster", "")
                        metadata_json = json.dumps(obj, indent=2)
                except Exception:
                    poster        = col_o if col_o.startswith("http") else ""
                    metadata_json = col_o

        def g(idx, default=""):
            return str(row[idx]).strip() if len(row) > idx else default

        items.append({
            "key":          f"{title or filename}__{i}",
            "title":        title,
            "titleMissing": not title,
            "sheetType":    sheet_type,
            "rowIndex":     i,
            "filename":     filename,
            "year":         g(2),
            "Movie Type":   g(5),
            "resolution":   g(4),
            "Quality":      quality,
            "storageCase":  g(6),
            "storageHdd":   g(7),
            "storageSize":  g(13),
            "myRating":     g(11),
            "imdbId":       "",
            "poster":       poster,
            "metadataJson": metadata_json,
        })
    return items


def build_response(movie_rows, series_rows, movies_sheet, series_sheet,
                   source_file, loaded_from):
    return {
        "sourceFileName":  source_file,
        "moviesSheetName": movies_sheet,
        "seriesSheetName": series_sheet,
        "movieRows":       movie_rows,
        "seriesRows":      series_rows,
        "movies":          parse_rows(movie_rows,  "movies"),
        "series":          parse_rows(series_rows, "series"),
        "loadedFrom":      loaded_from,
    }


def items_to_update_ops(items, collection):
    """Build Firestore batchWrite 'update' ops from item dicts."""
    return [
        {
            "update": {
                "name":   _doc_name(collection, make_doc_id(it["sheetType"], it["rowIndex"])),
                "fields": _dict_to_fields(it),
            }
        }
        for it in items
    ]


def is_metadata_missing(item_data):
    raw_meta = str(item_data.get("metadataJson", "") or "").strip()
    if not raw_meta:
        return True
    try:
        parsed = json.loads(raw_meta)
        if not isinstance(parsed, dict):
            return True
        if str(parsed.get("Response", "True")) == "False":
            return True
        return not str(parsed.get("Title", "") or "").strip()
    except Exception:
        return True


def parse_filename_hint(filename):
    """Title is before first '['; year is inside first '[...]'."""
    raw = _normalize_text(filename)
    if not raw:
        return "", ""

    src = raw.split("/")[-1].split("\\")[-1]
    if not src:
        return "", ""

    bracket_index = src.find("[")
    if bracket_index >= 0:
        title_part = src[:bracket_index]
    else:
        title_part = re.sub(r"\.[^.]+$", "", src)
    title = re.sub(r"\s+", " ", title_part.replace(".", " ")).strip()

    year_match = re.search(r"\[([^\]]*)\]", src)
    year = year_match.group(1).strip() if year_match else ""
    return title, year


def _normalize_omdb_query_title(raw_title):
    """Normalize title inputs that may arrive in filename-like format."""
    title = _normalize_text(raw_title)
    if not title:
        return ""

    # If caller sends a full filename-ish string, only keep the title prefix.
    if "[" in title:
        title = title.split("[", 1)[0]

    title = title.split("/")[-1].split("\\")[-1]
    title = re.sub(r"\s+", " ", title.replace(".", " ")).strip()
    return title


def fetch_metadata_from_omdb(query):
    api_keys = get_omdb_api_keys()
    if not api_keys:
        raise ValueError(_resource_text("backend.omdb.apiKeyRequired", "OMDb API key is required."))

    imdb_id = _normalize_text(query.get("imdbId") if isinstance(query, dict) else "")
    title = _normalize_text(query.get("title") if isinstance(query, dict) else "")
    year = _normalize_text(query.get("year") if isinstance(query, dict) else "")
    title = _normalize_omdb_query_title(title)
    if not imdb_id and not title:
        raise ValueError(_resource_text(
            "backend.omdb.inputRequired",
            "Please enter an IMDb ID or provide a filename that includes title and year.",
        ))

    last_error = _resource_text("backend.omdb.requestFailed", "OMDb request failed.")

    for idx, api_key in enumerate(api_keys):
        try:
            def request_omdb(query_title, query_year=""):
                params = {
                    "apikey": api_key,
                    "plot": "full",
                    "r": "json",
                }
                if imdb_id:
                    params["i"] = imdb_id
                else:
                    params["t"] = query_title
                    if query_year:
                        params["y"] = query_year

                response = requests.get("https://www.omdbapi.com/", params=params, timeout=20)
                if response.status_code != 200:
                    nonlocal_status_code[0] = response.status_code
                    nonlocal_last_error[0] = _resource_text(
                        "backend.omdb.requestFailedWithStatus",
                        "OMDb request failed ({status}).",
                        status=response.status_code,
                    )
                    return None
                return response.json()

            nonlocal_last_error = [last_error]
            nonlocal_status_code = [0]

            data = request_omdb(title, year)
            if not data:
                last_error = nonlocal_last_error[0]
                if nonlocal_status_code[0] == 401:
                    next_key = api_keys[idx + 1] if idx + 1 < len(api_keys) else None
                    if next_key:
                        _promote_omdb_key(next_key)
                        logger.warning(
                            "OMDb key %s returned 401; switched default key to next available key.",
                            idx + 1,
                        )
                continue
            if str(data.get("Response", "False")) == "True":
                _promote_omdb_key(api_key)
                return data

            if not imdb_id and year:
                data = request_omdb(title, "")
                if data and str(data.get("Response", "False")) == "True":
                    _promote_omdb_key(api_key)
                    return data

            err = data.get("Error") or ( # type: ignore
                _resource_text("backend.omdb.notFoundById", "OMDb could not find this IMDb ID.")
                if imdb_id else
                _resource_text("backend.omdb.notFoundByTitle", "OMDb could not find this title.")
            )
            last_error = err
            raise ValueError(err)
        except Exception as error:
            last_error = str(error) or last_error

    raise ValueError(last_error or _resource_text(
        "backend.omdb.allKeysUnavailable",
        "All OMDb keys are exhausted or unavailable.",
    ))


def fetch_omdb_metadata(api_key, title, year=""):
    params = {
        "apikey": api_key,
        "t": title,
        "plot": "full",
        "r": "json",
    }
    if year:
        params["y"] = year

    resp = requests.get("https://www.omdbapi.com/", params=params, timeout=20)
    resp.raise_for_status()
    data = resp.json()
    if str(data.get("Response", "False")) == "True":
        return data

    # Retry without year (sometimes year is noisy in filename)
    if year:
        params.pop("y", None)
        resp2 = requests.get("https://www.omdbapi.com/", params=params, timeout=20)
        resp2.raise_for_status()
        data2 = resp2.json()
        if str(data2.get("Response", "False")) == "True":
            return data2

    return None


def enrich_missing_movie_metadata(omdb_api_key):
    """
    Enrich movies with missing metadata from OMDb.
    - Updates title + metadataJson + poster + imdbId.
    - Saves local seed.
    - Attempts to sync changed entries to Firestore (best effort).
    """
    seed = seed_load()
    meta_entry, item_entries = seed_parts(seed)
    if not meta_entry:
        raise ValueError(_resource_text("backend.seed.invalidMeta", "Invalid seed: no meta entry"))

    scanned = 0
    missing = 0
    enriched = 0
    failed = 0
    changed_entries = []

    for entry in item_entries:
        if not entry.get("id") or not entry.get("data"):
            continue
        data = entry["data"]
        if data.get("sheetType") != "movies":
            continue

        scanned += 1
        if not is_metadata_missing(data):
            continue
        missing += 1

        filename = data.get("filename", "")
        title_hint, year_hint = parse_filename_hint(filename)
        existing_title = str(data.get("title", "") or "").strip()
        query_title = existing_title or title_hint
        query_year = str(data.get("year", "") or "").strip() or year_hint

        if not query_title:
            failed += 1
            continue

        try:
            omdb = fetch_omdb_metadata(omdb_api_key, query_title, query_year)
            if not omdb:
                failed += 1
                continue

            final_title = str(omdb.get("Title") or query_title).strip()
            final_year = str(omdb.get("Year") or query_year).strip()
            if final_year and "–" in final_year:
                final_year = final_year.split("–", 1)[0].strip()
            if final_year and "-" in final_year:
                final_year = final_year.split("-", 1)[0].strip()

            imdb_id = str(omdb.get("imdbID") or data.get("imdbId") or "").strip()
            poster = str(omdb.get("Poster") or "").strip()
            if not poster or poster == "N/A":
                poster = str(data.get("poster") or "").strip()

            rating = str(omdb.get("imdbRating") or "").strip()
            metadata_pretty = json.dumps(omdb, indent=2, ensure_ascii=False)
            metadata_compact = json.dumps(omdb, separators=(",", ":"), ensure_ascii=False)

            data["title"] = final_title
            data["titleMissing"] = not bool(final_title)
            data["year"] = final_year or str(data.get("year") or "")
            data["imdbId"] = imdb_id
            data["poster"] = poster
            if rating and rating != "N/A":
                data["myRating"] = rating
            data["metadataJson"] = metadata_pretty
            data["key"] = f"{final_title or filename}__{data.get('rowIndex', 0)}"

            changed_entries.append({"id": entry["id"], "data": data})
            enriched += 1
        except Exception as ex:
            failed += 1
            logger.warning(f"OMDb lookup failed for '{query_title}' ({query_year}): {ex}")

    if not changed_entries:
        return {
            "scannedMovies": scanned,
            "missingMetadata": missing,
            "enriched": 0,
            "failed": failed,
            "firebaseSynced": False,
            "savedToDisk": False,
            "message": _resource_text(
                "backend.enrich.noneEnriched",
                "No missing metadata entries were enriched.",
            )
        }

    # Persist back into seed
    changed_ids = {x["id"] for x in changed_entries}
    new_items = []
    for entry in item_entries:
        if entry.get("id") in changed_ids:
            replacement = next((x for x in changed_entries if x["id"] == entry["id"]), entry)
            new_items.append(replacement)
        else:
            new_items.append(entry)

    meta = dict(meta_entry.get("data") or {})
    meta["updatedAt"] = datetime.now().isoformat()

    updated_seed = {
        **seed,
        "generatedAt": datetime.now().isoformat(),
        "collections": {
            **seed.get("collections", {}),
            FIRESTORE_META_COL: [{"id": FIRESTORE_META_DOC_ID, "data": meta}],
            FIRESTORE_ITEMS_COL: new_items,
        },
    }
    seed_save(updated_seed)

    firebase_synced = False
    try:
        ops = [
            {
                "update": {
                    "name": _doc_name(FIRESTORE_ITEMS_COL, e["id"]),
                    "fields": _dict_to_fields(e["data"]),
                }
            }
            for e in changed_entries
        ]
        if ops:
            fs_batch_write(ops)
        fs_set_doc(FIRESTORE_META_COL, FIRESTORE_META_DOC_ID, meta)
        firebase_synced = True
    except Exception as ex:
        logger.warning(f"Could not sync enriched items to Firestore (local seed already updated): {ex}")

    return {
        "scannedMovies": scanned,
        "missingMetadata": missing,
        "enriched": enriched,
        "failed": failed,
        "firebaseSynced": firebase_synced,
        "savedToDisk": True,
        "changedIds": sorted(list(changed_ids))
    }


# Load shared resources and OMDb keys once when the server module starts.
_refresh_resource_catalog_if_needed(force=True)
# Load encrypted OMDb keys once when the server module starts.
initialize_omdb_api_keys()


# ===========================================================================
# API endpoints
# ===========================================================================

@app.route("/api/health", methods=["GET"])
def health():
    firebase_ok = fs_is_available()
    return jsonify({
        "status":    _resource_text("backend.health.status", "ok"),
        "timestamp": datetime.now().isoformat(),
        "firebase":  _resource_text("backend.health.firebaseReachable", "reachable")
                     if firebase_ok else
                     _resource_text("backend.health.firebaseUnreachable", "unreachable"),
    })


@app.route("/api/resources/<language_code>", methods=["GET"])
def get_resources(language_code):
    if language_code != "en":
        return jsonify({
            "error": _resource_text(
                "backend.resources.unsupportedLanguage",
                "Unsupported language: {language}",
                language=language_code,
            )
        }), 404

    _refresh_resource_catalog_if_needed(force=False)
    return jsonify(_resource_catalog)


@app.route("/api/loadWorkbook", methods=["POST"])
def load_workbook():
    """
    Load strategy:
      1. Load base seed from disk (always fast – cached after first read).
      2. Try to fetch items from Firestore (PRIMARY source).
      3. Merge: Firestore items override matching seed items.
      4. If Firestore is unreachable -> return plain seed (FALLBACK).
    """
    try:
        seed                     = seed_load()
        meta_entry, item_entries = seed_parts(seed)
        if not meta_entry:
            return jsonify({"error": _resource_text("backend.seed.invalidMeta", "Invalid seed: no meta entry")}), 400

        seed_meta  = meta_entry.get("data", {})
        seed_items = {e["id"]: e["data"] for e in item_entries if e.get("id") and e.get("data")}

        # -- Try Firestore --------------------------------------------------
        try:
            fs_meta  = fs_get_doc(FIRESTORE_META_COL, FIRESTORE_META_DOC_ID)
            fs_items = fs_list_docs(FIRESTORE_ITEMS_COL)
            firebase_available = True
            logger.info(
                f"Firestore: meta={'found' if fs_meta else 'missing'}, {len(fs_items)} items"
            )
        except Exception as e:
            logger.warning(f"Firestore unavailable - falling back to local seed. ({e})")
            firebase_available = False
            fs_meta, fs_items  = None, []

        # -- Merge (Firestore wins per field, but preserve seed defaults) ----
        # NOTE:
        # If Firestore meta exists but doesn't include series sheet settings,
        # using fs_meta directly can hide all seed series rows in the UI.
        # So we merge meta field-by-field with seed as fallback.
        effective_meta = ({**seed_meta, **(fs_meta or {})} if firebase_available else dict(seed_meta))
        merged         = dict(seed_items)                    # seed is the base
        for entry in fs_items:                               # Firestore overrides
            if entry.get("id") and entry.get("data"):
                merged[entry["id"]] = entry["data"]

        # Ensure series sheet metadata stays enabled when any series items exist.
        has_series_items = any((it or {}).get("sheetType") == "series" for it in merged.values())
        if has_series_items and not str(effective_meta.get("seriesSheetName", "")).strip():
            effective_meta["seriesSheetName"] = str(seed_meta.get("seriesSheetName") or "series")
            if not effective_meta.get("seriesHeaderRow") and seed_meta.get("seriesHeaderRow"):
                effective_meta["seriesHeaderRow"] = seed_meta.get("seriesHeaderRow")

        rows        = rows_from_items(effective_meta, list(merged.values()))
        loaded_from = "firebase" if firebase_available else "nosql-seed"

        return jsonify(build_response(
            movie_rows   = rows["movieRows"],
            series_rows  = rows["seriesRows"],
            movies_sheet = rows["moviesSheetName"],
            series_sheet = rows["seriesSheetName"],
            source_file  = rows["sourceFileName"],
            loaded_from  = loaded_from,
        ))
    except Exception as e:
        logger.error(f"loadWorkbook error: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500


@app.route("/api/saveWorkbook", methods=["POST"])
def save_workbook():
    """
    Save strategy:
      1. Diff updated rows against seed -> find changed items.
      2. Try to push changed items (overrides) to Firestore (PRIMARY).
      3. Always write the fully updated seed to disk (backup / fallback).
      4. Returns savedTo: 'firebase' | 'disk'
    """
    try:
        data         = request.get_json() or {}
        movie_rows   = data.get("movieRows",      [])
        series_rows  = data.get("seriesRows",     [])
        movies_sheet = data.get("moviesSheetName","movies")
        series_sheet = data.get("seriesSheetName","")
        source_file  = data.get("sourceFileName", "Local Seed")

        seed                       = seed_load()
        meta_entry, seed_item_ents = seed_parts(seed)
        seed_meta  = meta_entry.get("data", {}) if meta_entry else {}
        seed_items = {e["id"]: e["data"]
                      for e in seed_item_ents if e.get("id") and e.get("data")}

        all_items   = parse_rows(movie_rows, "movies") + parse_rows(series_rows, "series")
        current_map = {make_doc_id(it["sheetType"], it["rowIndex"]): it for it in all_items}

        # Only push items that actually changed
        changed = [
            item for did, item in current_map.items()
            if json.dumps(seed_items.get(did), sort_keys=True)
            != json.dumps(item,               sort_keys=True)
        ]
        logger.info(f"Save: {len(changed)}/{len(current_map)} items changed vs seed")

        updated_meta = {
            **seed_meta,
            "sourceFileName":  source_file,
            "moviesSheetName": movies_sheet,
            "seriesSheetName": series_sheet,
            "movieHeaderRow":  movie_rows[0]  if movie_rows  else [],
            "seriesHeaderRow": series_rows[0] if series_rows else [],
            "updatedAt":       datetime.now().isoformat(),
        }

        # -- Try Firestore --------------------------------------------------
        firebase_saved = False
        try:
            writes = items_to_update_ops(changed, FIRESTORE_ITEMS_COL)
            if writes:
                fs_batch_write(writes)
            fs_set_doc(FIRESTORE_META_COL, FIRESTORE_META_DOC_ID, updated_meta)
            firebase_saved = True
            logger.info("Saved to Firestore successfully")
        except Exception as e:
            logger.warning(f"Firestore save failed - writing to disk instead. ({e})")

        # -- Always update local seed as backup / offline fallback ----------
        updated_seed = {
            **seed,
            "generatedAt": datetime.now().isoformat(),
            "collections": {
                FIRESTORE_META_COL:  [{"id": FIRESTORE_META_DOC_ID, "data": updated_meta}],
                FIRESTORE_ITEMS_COL: [{"id": did, "data": item} for did, item in current_map.items()],
            },
            "summary": {
                **(seed.get("summary", {})),
                "totalItems":  len(current_map),
                "lastUpdated": datetime.now().isoformat(),
            },
        }
        seed_save(updated_seed)

        saved_to    = "firebase" if firebase_saved else "disk"
        loaded_from = "firebase" if firebase_saved else "nosql-seed"
        result      = build_response(movie_rows, series_rows, movies_sheet,
                                     series_sheet, source_file, loaded_from)
        result["savedTo"] = saved_to
        return jsonify(result)

    except Exception as e:
        logger.error(f"saveWorkbook error: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500


@app.route("/api/importNosqlSeed", methods=["POST"])
def import_nosql_seed():
    """
    Full sync: push every item in the local seed to Firestore.
    Deletes all existing Firestore items first, then writes fresh.
    Use this when Firestore is empty or has gone out of sync.
    """
    try:
        seed = seed_load()
        meta_entry, item_entries = seed_parts(seed)
        if not meta_entry:
            return jsonify({"error": _resource_text("backend.seed.invalidMeta", "Invalid seed: no meta entry")}), 400

        try:
            existing = fs_list_docs(FIRESTORE_ITEMS_COL)
        except Exception as e:
            return jsonify({"error": _resource_text(
                "backend.firestore.unreachable",
                "Firestore unreachable: {error}",
                error=e,
            )}), 503

        # Delete existing items
        del_ops = [{"delete": _doc_name(FIRESTORE_ITEMS_COL, e["id"])} for e in existing]
        if del_ops:
            fs_batch_write(del_ops)
            logger.info(f"Deleted {len(del_ops)} existing Firestore items")

        # Write all seed items
        write_ops = [
            {
                "update": {
                    "name":   _doc_name(FIRESTORE_ITEMS_COL, e["id"]),
                    "fields": _dict_to_fields(e["data"]),
                }
            }
            for e in item_entries if e.get("id") and e.get("data")
        ]
        if write_ops:
            fs_batch_write(write_ops)

        # Write meta
        fs_set_doc(FIRESTORE_META_COL, FIRESTORE_META_DOC_ID, meta_entry.get("data", {}))
        logger.info(f"Imported {len(write_ops)} items to Firestore")

        meta  = meta_entry.get("data", {})
        items = [e["data"] for e in item_entries if e.get("data")]
        rows  = rows_from_items(meta, items)
        result = build_response(
            movie_rows   = rows["movieRows"],
            series_rows  = rows["seriesRows"],
            movies_sheet = rows["moviesSheetName"],
            series_sheet = rows["seriesSheetName"],
            source_file  = rows["sourceFileName"],
            loaded_from  = "firebase",
        )
        result["importedCount"] = len(write_ops)
        return jsonify(result)

    except Exception as e:
        logger.error(f"importNosqlSeed error: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500


@app.route("/api/enrichMissingMetadata", methods=["POST"])
def enrich_missing_metadata():
    """
    Body: { "omdbApiKey": "..." }
    """
    try:
        data = request.get_json(silent=True) or {}
        omdb_api_key = str(data.get("omdbApiKey") or "").strip()
        if not omdb_api_key:
            return jsonify({
                "error": _resource_text(
                    "backend.omdb.apiKeyBodyRequired",
                    "OMDb API key is required. Provide 'omdbApiKey' in request body.",
                )
            }), 400

        result = enrich_missing_movie_metadata(omdb_api_key)
        return jsonify(result)
    except Exception as e:
        logger.error(f"enrichMissingMetadata error: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500


@app.route("/api/fetchOmdbMetadata", methods=["POST"])
def fetch_omdb_metadata_endpoint():
    """
    Body: {
      "imdbId": "tt..." (optional),
      "filename": "Movie.Name[2021][1080p]" (optional),
      "title": "..." (optional),
      "year": "..." (optional)
    }
    """
    try:
        data = request.get_json(silent=True) or {}
        imdb_id = _normalize_text(data.get("imdbId"))
        filename = _normalize_text(data.get("filename"))
        title = _normalize_text(data.get("title"))
        year = _normalize_text(data.get("year"))

        parsed_title, parsed_year = parse_filename_hint(filename)
        query_title = title or parsed_title
        query_year = year or parsed_year

        if not imdb_id and not query_title:
            return jsonify({
                "error": _resource_text(
                    "backend.omdb.endpointInputRequired",
                    "Please enter an IMDb ID or use a filename like Movie.Name[2021].",
                )
            }), 400

        omdb = fetch_metadata_from_omdb({
            "imdbId": imdb_id,
            "title": query_title,
            "year": query_year,
        })

        return jsonify({
            "omdb": omdb,
            "query": {
                "imdbId": imdb_id,
                "title": query_title,
                "year": query_year,
            },
            "parsed": {
                "title": parsed_title,
                "year": parsed_year,
            }
        })
    except Exception as e:
        logger.error(f"fetchOmdbMetadata error: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500


@app.route("/api/exportNosqlSeed", methods=["GET"])
def export_nosql_seed():
    try:
        seed = seed_load()
        return jsonify({"seedJson": json.dumps(
            {**seed, "generatedAt": datetime.now().isoformat()}, indent=2
        )})
    except Exception as e:
        logger.error(f"exportNosqlSeed error: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500


@app.route("/api/getFirebaseConfig", methods=["GET"])
def get_firebase_config():
    return jsonify(FIREBASE_CONFIG)


@app.route("/api/addMovie", methods=["POST"])
def add_movie():
    """
    Add a new movie entry to the collection.
    Body: { filename, title, year, "Movie Type", resolution, Quality, storageCase, storageHdd }
    """
    try:
        data = request.get_json(silent=True) or {}

        seed = seed_load()
        meta_entry, item_entries = seed_parts(seed)
        if not meta_entry:
            return jsonify({"error": _resource_text("backend.seed.invalidMeta", "Invalid seed: no meta entry")}), 400
        seed_meta = meta_entry.get("data", {})

        # Find the highest rowIndex among existing movie items
        max_row = max(
            (int(e["data"].get("rowIndex", 0) or 0)
             for e in item_entries
             if e.get("data") and e["data"].get("sheetType") == "movies"),
            default=0,
        )
        new_row_index = max_row + 1

        title    = _normalize_text(data.get("title"))
        filename = _normalize_text(data.get("filename"))
        quality  = _normalize_text(data.get("Quality")).lower()

        new_item = {
            "key":          f"{title or filename}__{new_row_index}",
            "title":        title,
            "titleMissing": not bool(title),
            "sheetType":    "movies",
            "rowIndex":     new_row_index,
            "filename":     filename,
            "year":         _normalize_text(data.get("year")),
            "Movie Type":   _normalize_text(data.get("Movie Type")),
            "resolution":   _normalize_text(data.get("resolution")),
            "Quality":      quality,
            "storageCase":  _normalize_text(data.get("storageCase")),
            "storageHdd":   _normalize_text(data.get("storageHdd")),
            "storageSize":  _normalize_text(data.get("storageSize")),
            "myRating":     "",
            "imdbId":       "",
            "poster":       "",
            "metadataJson": "",
            "language":     _normalize_text(data.get("Movie Type")),
        }

        new_doc_id   = make_doc_id("movies", new_row_index)
        updated_meta = {**seed_meta, "updatedAt": datetime.now().isoformat()}

        # Try Firestore first
        firebase_saved = False
        try:
            write_op = {
                "update": {
                    "name":   _doc_name(FIRESTORE_ITEMS_COL, new_doc_id),
                    "fields": _dict_to_fields(new_item),
                }
            }
            fs_batch_write([write_op])
            fs_set_doc(FIRESTORE_META_COL, FIRESTORE_META_DOC_ID, updated_meta)
            firebase_saved = True
            logger.info(f"Added new movie '{title}' to Firestore as {new_doc_id}")
        except Exception as ex:
            logger.warning(f"Firestore save failed for new movie; using local seed. ({ex})")

        # Always update local seed
        all_seed_entries = list(item_entries) + [{"id": new_doc_id, "data": new_item}]
        updated_seed = {
            **seed,
            "generatedAt": datetime.now().isoformat(),
            "collections": {
                FIRESTORE_META_COL:  [{"id": FIRESTORE_META_DOC_ID, "data": updated_meta}],
                FIRESTORE_ITEMS_COL: all_seed_entries,
            },
        }
        seed_save(updated_seed)

        all_items   = [e["data"] for e in all_seed_entries if e.get("data")]
        rows        = rows_from_items(updated_meta, all_items)
        loaded_from = "firebase" if firebase_saved else "nosql-seed"
        result      = build_response(
            movie_rows   = rows["movieRows"],
            series_rows  = rows["seriesRows"],
            movies_sheet = rows["moviesSheetName"],
            series_sheet = rows["seriesSheetName"],
            source_file  = rows["sourceFileName"],
            loaded_from  = loaded_from,
        )
        result["savedTo"]  = "firebase" if firebase_saved else "disk"
        result["newDocId"] = new_doc_id
        return jsonify(result)

    except Exception as e:
        logger.error(f"addMovie error: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500


if __name__ == "__main__":
    logger.info(f"Starting server - seed: {SEED_FILE}")
    app.run(host="127.0.0.1", port=5000, debug=True)
