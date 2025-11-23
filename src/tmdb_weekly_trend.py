#!/usr/bin/env python3
"""
TMDB DIY Project - Weekly Trending Script

Tasks:
- Fetch weekly trending titles from TMDB
- Build genre ID to name mapping
- Normalize and save a clean CSV for exploration
- Create a by-genre CSV for better genre charts
- Optionally pull a few casts and show frequent actors

Auth: uses TMDB V4 Read Access Token from env var TMDB_V4_TOKEN
"""

import os
import json
import time
import argparse
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Any, List, Optional

import requests
import pandas as pd

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
        time.sleep(1 + attempt)  # 1s, 2s, 3s
    raise RuntimeError(f"HTTP {last_resp.status_code} for {url}: {last_resp.text[:300]}")


def get_trending(
    media_type: str = "all",
    time_window: str = "week",
    pages: int = 5,
) -> Dict[str, Any]:
    """
    Fetch multiple pages of trending data.

    media_type: "all", "movie", "tv", or "person"
    time_window: "day" or "week"
    pages: how many pages of results to retrieve (20 items per page)
    """
    all_results: List[Dict[str, Any]] = []
    for page in range(1, pages + 1):
        data = fetch_json(
            f"/trending/{media_type}/{time_window}",
            params={"page": page},
        )
        page_results = data.get("results", [])
        if not page_results:
            break
        all_results.extend(page_results)
        time.sleep(0.3)  # be kind to TMDB

    return {"results": all_results}


def get_genre_map() -> Dict[int, str]:
    """Build a single ID to name mapping across movie and TV genres."""
    movie = fetch_json("/genre/movie/list")
    tv = fetch_json("/genre/tv/list")
    genre_map = {g["id"]: g["name"] for g in movie.get("genres", [])}
    # Merge TV genres too, override if duplicates share same id
    genre_map.update({g["id"]: g["name"] for g in tv.get("genres", [])})
    # Save for reference
    (DATA_DIR / "genres.json").write_text(
        json.dumps(genre_map, indent=2),
        encoding="utf-8",
    )
    return genre_map


def normalize_trending(results: List[Dict[str, Any]], genre_map: Dict[int, str]) -> pd.DataFrame:
    """
    Create a tidy DataFrame with common fields across movie and TV rows.
    Maps genre_ids to a semicolon joined genre_names.
    """
    rows = []
    for r in results:
        media_type = r.get("media_type")
        # title vs. name
        title = r.get("title") or r.get("name")
        # release_date vs. first_air_date
        date = r.get("release_date") or r.get("first_air_date")
        genre_ids = r.get("genre_ids") or []
        genres = [genre_map.get(gid, str(gid)) for gid in genre_ids]

        # extra fields for Power BI convenience
        overview = r.get("overview")
        poster_path = r.get("poster_path")
        backdrop_path = r.get("backdrop_path")

        if media_type == "movie":
            tmdb_url = f"https://www.themoviedb.org/movie/{r.get('id')}"
        elif media_type == "tv":
            tmdb_url = f"https://www.themoviedb.org/tv/{r.get('id')}"
        else:
            tmdb_url = ""

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
                "overview": overview,
                "poster_path": poster_path,
                "backdrop_path": backdrop_path,
                "tmdb_url": tmdb_url,
            }
        )

    df = pd.DataFrame(rows)
    # Sort by popularity as a basic default
    if not df.empty:
        df = df.sort_values(
            by=["popularity"],
            ascending=False,
            kind="mergesort",
        ).reset_index(drop=True)
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


def get_credits(media_type: str, tmdb_id: int) -> Dict[str, Any]:
    """
    Fetch credits for a given title.
    Movie: /movie/{id}/credits
    TV: /tv/{id}/credits
    """
    if media_type == "movie":
        return fetch_json(f"/movie/{tmdb_id}/credits")
    elif media_type == "tv":
        return fetch_json(f"/tv/{tmdb_id}/credits")
    else:
        return {"cast": [], "crew": []}


def top_cast_from_sample(df: pd.DataFrame, sample_size: int = 10) -> pd.DataFrame:
    """
    Pull credits for a few top titles and count actor frequency.
    """
    cast_counts: Dict[str, int] = {}
    sample = df.head(sample_size)
    for _, row in sample.iterrows():
        credits = get_credits(row["media_type"], int(row["id"]))
        for c in credits.get("cast", []):
            name = c.get("name")
            if not name:
                continue
            cast_counts[name] = cast_counts.get(name, 0) + 1
        time.sleep(0.2)  # be gentle

    if not cast_counts:
        return pd.DataFrame(columns=["name", "count"])

    out = pd.DataFrame([{"name": k, "count": v} for k, v in cast_counts.items()])
    return out.sort_values("count", ascending=False).reset_index(drop=True)


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--media", choices=["all", "movie", "tv"], default="all")
    p.add_argument("--window", choices=["day", "week"], default="week")
    p.add_argument(
        "--cast-sample",
        type=int,
        default=8,
        help="number of top items to pull credits for",
    )
    return p.parse_args()


def main():
    if not os.getenv("TMDB_V4_TOKEN"):
        raise SystemExit("Set environment variable TMDB_V4_TOKEN to your v4 read access token.")

    args = parse_args()

    # Week stamp for filenames and a slicer field for history
    now_utc = datetime.now(timezone.utc)
    STAMP = now_utc.strftime("%Y-%m-%d")
    week_label = STAMP

    # 1) Fetch and save raw JSON snapshot
    trending = get_trending(media_type=args.media, time_window=args.window, pages=5)
    (DATA_DIR / f"trending_{args.media}_{args.window}_raw_{STAMP}.json").write_text(
        json.dumps(trending, indent=2),
        encoding="utf-8",
    )

    # 2) Genre map
    genre_map = get_genre_map()

    # 3) Normalize and add week label
    results = trending.get("results", [])
    df = normalize_trending(results, genre_map)
    df["week"] = week_label

    # 4) Save stamped clean CSV
    clean_path = DATA_DIR / f"trending_{args.media}_{args.window}_clean_{STAMP}.csv"
    df.to_csv(clean_path, index=False, encoding="utf-8")
    print(f"Saved clean trending CSV to {clean_path.resolve()} with {len(df)} rows")

    # 5) Save by-genre expanded CSV for accurate genre charts
    df_by_genre = expand_by_genre(df)
    by_genre_path = DATA_DIR / f"trending_{args.media}_{args.window}_by_genre_{STAMP}.csv"
    df_by_genre.to_csv(by_genre_path, index=False, encoding="utf-8")
    print(f"Saved by-genre CSV to {by_genre_path.resolve()} with {len(df_by_genre)} rows")

    # 6) Append to rolling history for multi week comparisons
    hist_path = DATA_DIR / f"trending_{args.media}_{args.window}_history.csv"
    if hist_path.exists():
        old = pd.read_csv(hist_path)
        hist = pd.concat([old, df], ignore_index=True)
    else:
        hist = df
    hist.to_csv(hist_path, index=False, encoding="utf-8")
    print(f"Appended to history at {hist_path.resolve()}")

    # 7) Optional cast sampling
    if not df.empty and args.cast_sample > 0:
        cast_df = top_cast_from_sample(df, sample_size=min(args.cast_sample, len(df)))
        cast_path = DATA_DIR / f"sample_cast_counts_{STAMP}.csv"
        cast_df.to_csv(cast_path, index=False, encoding="utf-8")
        print("Sample frequent cast:")
        print(cast_df.head(10))
        print(f"Saved sample cast counts to {cast_path.resolve()}")


if __name__ == "__main__":
    main()
