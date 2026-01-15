from datetime import datetime
import os

import requests
from fastapi import FastAPI, HTTPException, Query

from fycharts.compute_dates import defaultListOfDates


REGION_CODES = {
    "global",
    "ad",
    "ar",
    "at",
    "au",
    "be",
    "bg",
    "bo",
    "br",
    "ca",
    "ch",
    "cl",
    "co",
    "cr",
    "cy",
    "cz",
    "de",
    "dk",
    "do",
    "ec",
    "ee",
    "es",
    "fi",
    "fr",
    "gb",
    "gr",
    "gt",
    "hk",
    "hn",
    "hu",
    "id",
    "ie",
    "il",
    "is",
    "it",
    "jp",
    "lt",
    "lu",
    "lv",
    "mc",
    "mt",
    "mx",
    "my",
    "ni",
    "nl",
    "no",
    "nz",
    "pa",
    "pe",
    "ph",
    "pl",
    "pt",
    "py",
    "ro",
    "se",
    "sg",
    "sk",
    "sv",
    "th",
    "tr",
    "tw",
    "us",
    "uy",
    "vn",
}

app = FastAPI(title="fycharts API", version="0.2.0")

CHARTS_BASE_URL = os.getenv(
    "SPOTIFY_CHARTS_BASE_URL",
    "https://charts-spotify-com-service.spotify.com/auth/v0/charts",
)
CHARTS_TOKEN = os.getenv("SPOTIFY_CHARTS_TOKEN")

ALIAS_TEMPLATES = {
    ("top200", "daily"): "regional-{region}-daily",
    ("top200", "weekly"): "regional-{region}-weekly",
    ("viral50", "daily"): "viral-{region}-daily",
    ("viral50", "weekly"): "viral-{region}-weekly",
}


def parse_date(value, param_name):
    try:
        return datetime.strptime(value, "%Y-%m-%d")
    except ValueError as exc:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid {param_name} date '{value}'. Expected YYYY-MM-DD.",
        ) from exc


def build_dates(start, end, is_weekly, is_viral, latest_only_if_unset=False):
    valid_dates = defaultListOfDates(is_weekly, is_viral)
    valid_date_map = {
        datetime.strptime(date_str, "%Y-%m-%d"): date_str for date_str in valid_dates
    }
    valid_date_list = sorted(valid_date_map.keys())

    if start is None and end is None and latest_only_if_unset:
        return ["latest"]
    elif start is None:
        start_dt = valid_date_list[0]
    else:
        start_dt = parse_date(start, "start")
        if start_dt < valid_date_list[0]:
            start_dt = valid_date_list[0]
        elif is_weekly and start_dt not in valid_date_map:
            suggestions = [
                valid_date_map[date_dt]
                for date_dt in valid_date_list
                if date_dt >= start_dt
            ][:5]
            raise HTTPException(
                status_code=400,
                detail=(
                    "Invalid start date for weekly charts. "
                    f"Try one of: {', '.join(suggestions)}."
                ),
            )

    if end is None:
        if start is None and latest_only_if_unset:
            end_dt = valid_date_list[-1]
        else:
            end_dt = valid_date_list[-1]
    else:
        end_dt = parse_date(end, "end")
        if end_dt > valid_date_list[-1]:
            end_dt = valid_date_list[-1]

    if end_dt < start_dt:
        raise HTTPException(
            status_code=400,
            detail="End date must be the same as or after start date.",
        )

    return [
        valid_date_map[date_dt]
        for date_dt in valid_date_list
        if start_dt <= date_dt <= end_dt
    ]


def normalize_regions(regions):
    if not regions:
        return ["global"]
    invalid = [region for region in regions if region not in REGION_CODES]
    if invalid:
        raise HTTPException(
            status_code=400,
            detail=f"Unsupported region(s): {', '.join(invalid)}.",
        )
    return regions


def parse_spotify_id(uri):
    if not uri or ":" not in uri:
        return None
    parts = uri.split(":")
    return parts[-1] if len(parts) >= 3 else None


def require_token():
    if CHARTS_TOKEN:
        return CHARTS_TOKEN
    raise HTTPException(
        status_code=502,
        detail=(
            "Spotify Charts now requires an access token. "
            "Set SPOTIFY_CHARTS_TOKEN in Fly secrets."
        ),
    )


def fetch_chart_entries(alias, date):
    token = require_token()
    url = f"{CHARTS_BASE_URL}/{alias}/{date}"
    response = requests.get(
        url,
        headers={"Authorization": f"Bearer {token}"},
        timeout=15,
    )
    if response.status_code == 401:
        raise HTTPException(
            status_code=502,
            detail="Spotify Charts token expired or invalid. Refresh SPOTIFY_CHARTS_TOKEN.",
        )
    if response.status_code >= 400:
        raise HTTPException(
            status_code=502,
            detail=f"Spotify Charts API error ({response.status_code}).",
        )
    payload = response.json()
    return payload


def normalize_alias(chart_key, region):
    template = ALIAS_TEMPLATES[chart_key]
    return template.format(region=region.lower())


def extract_entries(payload, region_override=None):
    display = payload.get("displayChart", {})
    chart_date = display.get("date") or payload.get("date")
    dimensions = display.get("chartMetadata", {}).get("dimensions", {})
    region = (
        region_override
        or (dimensions.get("country") or "").lower()
        or None
    )

    entries = []
    for entry in payload.get("entries", []):
        if entry.get("missingRequiredFields"):
            continue
        chart_data = entry.get("chartEntryData", {})
        track_meta = entry.get("trackMetadata") or {}
        artists = track_meta.get("artists") or []
        artist_names = ", ".join(
            [artist.get("name", "") for artist in artists if artist.get("name")]
        )
        rank_metric = chart_data.get("rankingMetric") or {}
        streams = None
        if rank_metric.get("type") == "STREAMS":
            streams = rank_metric.get("value")

        entries.append(
            {
                "Position": chart_data.get("currentRank"),
                "Track Name": track_meta.get("trackName"),
                "Artist": artist_names,
                "Streams": streams,
                "date": chart_date,
                "region": region,
                "spotify_id": parse_spotify_id(track_meta.get("trackUri")),
            }
        )
    return entries


def fetch_chart(chart_key, dates, regions):
    output = []
    misses = []
    for date in dates:
        for region in regions:
            alias = normalize_alias(chart_key, region)
            payload = fetch_chart_entries(alias, date)
            entries = extract_entries(payload, region_override=region)
            if not entries:
                misses.append(f"{date}/{region}")
                continue
            output.extend(entries)
    if not output:
        detail = "No chart data returned for requested dates/regions."
        if misses:
            detail = (
                "Spotify Charts returned no entries for: "
                + ", ".join(misses)
                + "."
            )
        raise HTTPException(status_code=502, detail=detail)
    return output


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/charts/top200/daily")
def top200_daily(
    start: str | None = None,
    end: str | None = None,
    region: list[str] | None = Query(default=None),
):
    dates = build_dates(start, end, is_weekly=False, is_viral=False, latest_only_if_unset=True)
    regions = normalize_regions(region)
    data = fetch_chart(("top200", "daily"), dates, regions)
    return {"chart": "top_200_daily", "data": data}


@app.get("/charts/top200/weekly")
def top200_weekly(
    start: str | None = None,
    end: str | None = None,
    region: list[str] | None = Query(default=None),
):
    dates = build_dates(start, end, is_weekly=True, is_viral=False, latest_only_if_unset=True)
    regions = normalize_regions(region)
    data = fetch_chart(("top200", "weekly"), dates, regions)
    return {"chart": "top_200_weekly", "data": data}


@app.get("/charts/viral50/daily")
def viral50_daily(
    start: str | None = None,
    end: str | None = None,
    region: list[str] | None = Query(default=None),
):
    dates = build_dates(start, end, is_weekly=False, is_viral=True, latest_only_if_unset=True)
    regions = normalize_regions(region)
    data = fetch_chart(("viral50", "daily"), dates, regions)
    return {"chart": "viral_50_daily", "data": data}


@app.get("/charts/viral50/weekly")
def viral50_weekly(
    start: str | None = None,
    end: str | None = None,
    region: list[str] | None = Query(default=None),
):
    dates = build_dates(start, end, is_weekly=True, is_viral=True, latest_only_if_unset=True)
    regions = normalize_regions(region)
    data = fetch_chart(("viral50", "weekly"), dates, regions)
    return {"chart": "viral_50_weekly", "data": data}
