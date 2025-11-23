#!/usr/bin/env python3
"""
TMDB DIY Project - Historical Script

Goal:
- Fetch popular movies and TV shows from roughly the last 6 months
- Use TMDB Discover endpoint (not Trending)
- Normalize results into a clean CSV for Power BI
- Create a by-genre CSV so genre charts are accurate

Auth: uses TMDB V4 Read Access Token from env var TMDB_V4_TOKEN
"""

import os
import json
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, Any, List, Optional

import requests
import pandas as pd

# How far back to look for releases (in days)
DAYS_BACK = 180
# How many pages per media type to pull (20 results per page)
PAGES = 5

API_BASE = "https://api.themoviedb.org/3"
HEADERS = {
    "Authorization": f"Bearer {os.getenv('TMDB_V4_TOKEN', '')}",
    "Accept": "application/json",
}

SCRIPT_DIR = Path(__file__).resolve().parent
DATA_DIR = SCRIPT_DIR / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)


def fetch_json(path: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """GET a TMDB v3 path with simple retries and backoff."""
    url = f"{API_BASE}{path}"
    last_resp = None
    for attempt in range(3):
        resp = requests.get(url, headers=HEADERS, params=params or {})
        if resp.status_code == 200:
            return resp.json()
        last_resp = resp
        time.sleep(1 + attempt)
    raise RuntimeError(f"HTTP {last_resp.status_code} for {url}: {last_resp.text[:300]}")


def get_genre_map() -> Dict[int, str]:
    """Build a single ID to name mapping across movie and TV genres."""
    movie = fetch_json("/genre/movie/list")
    tv = fetch_json("/genre/tv/list")
    genre_map = {g["id"]: g["name"] for g in movie.get("genres", [])}
    genre_map.update({g["id"]: g["name"] for g in tv.get("genres", [])})
    (DATA_DIR / "genres_historical.json").write_text(
        json.dumps(genre_map, indent=2),
        encoding="utf-8",
    )
    return genre_map


def discover_range(
    media_type: str,
    start_date: str,
    end_date: str,
    pages: int = PAGES,
) -> List[Dict[str, Any]]:
    """
    Use TMDB Discover to approximate top titles for a date range.
    Dates are strings in YYYY-MM-DD.
    Returns a list of result dicts.
    """
    if media_type == "movie":
        path = "/discover/movie"
        base_params = {
            "sort_by": "popularity.desc",
            "primary_release_date.gte": start_date,
            "primary_release_date.lte": end_date,
            "include_adult": "false",
        }
    elif media_type == "tv":
        path = "/discover/tv"
        base_params = {
            "sort_by": "popularity.desc",
            "first_air_date.gte": start_date,
            "first_air_date.lte": end_date,
        }
    else:
        return []

    all_results: List[Dict[str, Any]] = []
    for page in range(1, pages + 1):
        params = dict(base_params)
        params["page"] = page
        data = fetch_json(path, params=params)
        results = data.get("results", [])
        if not results:
            break
        for r in results:
            r["media_type"] = media_type
        all_results.extend(results)
        time.sleep(0.3)
    return all_results


def normalize_results(results: List[Dict[str, Any]], genre_map: Dict[int, str]) -> pd.DataFrame:
    """
    Create a tidy DataFrame with common fields across movie and TV rows.
    Maps genre_ids to a semicolon joined genre_names.
    """
    rows = []
    for r in results:
        media_type = r.get("media_type")
        title = r.get("title") or r.get("name")
        date = r.get("release_date") or r.get("first_air_date")
        genre_ids = r.get("genre_ids") or []
        genres = [genre_map.get(gid, str(gid)) for gid in genre_ids]
        rows.append(
            {
                "id": r.get("id"),
                "media_type": media_type,
                "title": title,
                "date": date,
                "popularity": r.get("popularity"),
                "vote_average": r.get("vote_average"),
                "vote_count": r.get("vote_count"),
                "original_language": r.get("original_language"),
                "genres": "; ".join(genres),
            }
        )
    df = pd.DataFrame(rows)
    if not df.empty:
        df = df.sort_values(by=["popularity"], ascending=False, kind="mergesort").reset_index(drop=True)
    return df


def expand_by_genre(df: pd.DataFrame) -> pd.DataFrame:
    """Split the semicolon list in 'genres' into one row per genre for accurate counts."""
    out_rows = []
    for _, r in df.iterrows():
        g_list = [x.strip() for x in str(r["genres"]).split(";") if x and x.strip()]
        if not g_list:
            rr = r.copy()
            rr["genre"] = ""
            out_rows.append(rr)
        else:
            for g in g_list:
                rr = r.copy()
                rr["genre"] = g
                out_rows.append(rr)
    return pd.DataFrame(out_rows)


def main():
    if not os.getenv("TMDB_V4_TOKEN"):
        raise SystemExit("Set environment variable TMDB_V4_TOKEN to your v4 read access token.")

    now_utc = datetime.now(timezone.utc)
    end_date = now_utc.date()
    start_date = end_date - timedelta(days=DAYS_BACK)

    start_s = start_date.strftime("%Y-%m-%d")
    end_s = end_date.strftime("%Y-%m-%d")
    stamp = now_utc.strftime("%Y-%m-%d")

    print(f"Building historical dataset from {start_s} to {end_s}")

    genre_map = get_genre_map()

    all_results: List[Dict[str, Any]] = []

    # movies
    movie_results = discover_range("movie", start_s, end_s, pages=PAGES)
    print(f"Fetched {len(movie_results)} movie rows")
    all_results.extend(movie_results)

    # tv
    tv_results = discover_range("tv", start_s, end_s, pages=PAGES)
    print(f"Fetched {len(tv_results)} tv rows")
    all_results.extend(tv_results)

    if not all_results:
        print("No results fetched for historical range.")
        return

    df = normalize_results(all_results, genre_map)
    print(f"Total normalized rows: {len(df)}")

    # Save clean historical CSV with a date stamp
    clean_path = DATA_DIR / f"historical_all_clean_{stamp}.csv"
    df.to_csv(clean_path, index=False, encoding="utf-8")
    print(f"Saved historical clean CSV to {clean_path.resolve()}")

    # Also save a stable name for Power BI
    latest_clean_path = DATA_DIR / "historical_all_clean_latest.csv"
    df.to_csv(latest_clean_path, index=False, encoding="utf-8")
    print(f"Saved historical clean CSV to {latest_clean_path.resolve()}")

    # By genre
    df_by_genre = expand_by_genre(df)
    by_genre_path = DATA_DIR / f"historical_all_by_genre_{stamp}.csv"
    df_by_genre.to_csv(by_genre_path, index=False, encoding="utf-8")
    print(f"Saved historical by-genre CSV to {by_genre_path.resolve()}")

    latest_by_genre_path = DATA_DIR / "historical_all_by_genre_latest.csv"
    df_by_genre.to_csv(latest_by_genre_path, index=False, encoding="utf-8")
    print(f"Saved historical by-genre CSV to {latest_by_genre_path.resolve()}")


if __name__ == "__main__":
    main()
