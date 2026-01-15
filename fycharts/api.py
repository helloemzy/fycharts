from datetime import datetime

from fastapi import FastAPI, HTTPException, Query

from fycharts.crawler_base import SpotifyChartsBase
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

app = FastAPI(title="fycharts API", version="0.1.0")
charts = SpotifyChartsBase()


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
        start_dt = valid_date_list[-1]
        end_dt = valid_date_list[-1]
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


def is_empty_df(df):
    if "track name" not in df.columns:
        return False
    return df["track name"].eq("NA").all()


def fetch_chart(fetch_fn, dates, regions):
    output = []
    misses = []
    for date in dates:
        for region in regions:
            df = fetch_fn(date, region)
            if is_empty_df(df):
                misses.append(f"{date}/{region}")
                continue
            output.extend(df.to_dict(orient="records"))
    if not output:
        detail = "No chart data returned for requested dates/regions."
        if misses:
            detail = (
                "Spotifycharts returned no CSV data for: "
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
    data = fetch_chart(charts.helperTop200Daily, dates, regions)
    return {"chart": "top_200_daily", "data": data}


@app.get("/charts/top200/weekly")
def top200_weekly(
    start: str | None = None,
    end: str | None = None,
    region: list[str] | None = Query(default=None),
):
    dates = build_dates(start, end, is_weekly=True, is_viral=False, latest_only_if_unset=True)
    regions = normalize_regions(region)
    data = fetch_chart(charts.helperTop200Weekly, dates, regions)
    return {"chart": "top_200_weekly", "data": data}


@app.get("/charts/viral50/daily")
def viral50_daily(
    start: str | None = None,
    end: str | None = None,
    region: list[str] | None = Query(default=None),
):
    dates = build_dates(start, end, is_weekly=False, is_viral=True, latest_only_if_unset=True)
    regions = normalize_regions(region)
    data = fetch_chart(charts.helperViral50Daily, dates, regions)
    return {"chart": "viral_50_daily", "data": data}


@app.get("/charts/viral50/weekly")
def viral50_weekly(
    start: str | None = None,
    end: str | None = None,
    region: list[str] | None = Query(default=None),
):
    dates = build_dates(start, end, is_weekly=True, is_viral=True, latest_only_if_unset=True)
    regions = normalize_regions(region)
    data = fetch_chart(charts.helperViral50Weekly, dates, regions)
    return {"chart": "viral_50_weekly", "data": data}
