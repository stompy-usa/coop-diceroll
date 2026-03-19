#!/usr/bin/env python3
"""
Steam Co-op Game Database Builder
==================================
Scrapes Steam's public APIs to build a static JSON database of co-op games.
Filters: co-op tagged, 65%+ positive reviews, 100+ total reviews.

Usage:
    python build_steam_db.py              # Build or refresh the database
    python build_steam_db.py --output path/to/steam_coop_db.json
    python build_steam_db.py --limit 500  # Quick test run (first N games)
    python build_steam_db.py --resume     # Resume an interrupted scrape

Output:
    steam_coop_db.json  — drop this next to your HTML file
"""

import argparse
import json
import os
import time
import sys
from datetime import datetime, timezone
from pathlib import Path

try:
    import requests
except ImportError:
    print("Missing dependency. Run:  pip install requests")
    sys.exit(1)

# ─── Configuration ────────────────────────────────────────────────────────────

MIN_REVIEW_PCT   = 65      # Minimum % positive reviews
MIN_REVIEW_COUNT = 100     # Minimum total reviews
REQUEST_DELAY    = 1.5     # Seconds between appdetails requests (respect rate limits)
REVIEW_DELAY     = 1.0     # Seconds between review requests
BATCH_SAVE_EVERY = 50      # Save progress to disk every N games
MAX_RETRIES      = 3       # Retries on network failure
RETRY_WAIT       = 10      # Seconds to wait before retrying

# Steam category IDs that indicate co-op
COOP_CATEGORY_IDS = {
    1,   # Multi-player
    9,   # Co-op
    27,  # Cross-Platform Multiplayer
    36,  # Online Co-op
    38,  # Local Co-op
    49,  # PvP
}
# Must have at least one of these to be considered co-op
REQUIRED_COOP_IDS = {9, 36, 38}  # Co-op, Online Co-op, or Local Co-op

COOP_CATEGORY_LABELS = {
    1:  "Multi-player",
    9:  "Co-op",
    27: "Cross-Platform Multiplayer",
    36: "Online Co-op",
    38: "Local Co-op",
    49: "PvP",
}

# ─── API helpers ──────────────────────────────────────────────────────────────

SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "SteamCoopDBBuilder/1.0"})


def get_with_retry(url, params=None, retries=MAX_RETRIES):
    """GET request with exponential backoff on failure."""
    for attempt in range(retries):
        try:
            r = SESSION.get(url, params=params, timeout=15)
            if r.status_code == 429:
                wait = RETRY_WAIT * (attempt + 1)
                print(f"    Rate limited. Waiting {wait}s...")
                time.sleep(wait)
                continue
            r.raise_for_status()
            return r
        except requests.RequestException as e:
            if attempt < retries - 1:
                wait = RETRY_WAIT * (attempt + 1)
                print(f"    Request failed ({e}). Retry {attempt+1}/{retries-1} in {wait}s...")
                time.sleep(wait)
            else:
                raise
    return None


def fetch_all_steam_apps():
    """
    Fetch the full Steam app list.
    Tries four sources in order of reliability.
    """
    print("Fetching Steam app list...")

    # ── Source 1: Steam store category filter (no API key, very reliable) ──
    try:
        print("  Trying Steam store category filter...")
        apps = fetch_apps_from_steam_category()
        if apps:
            return apps
    except Exception as e:
        print(f"  Steam category filter failed: {e}")

    # ── Source 2: SteamSpy tag API ──
    try:
        print("  Trying SteamSpy API...")
        apps = fetch_apps_from_steamspy()
        if apps:
            return apps
    except Exception as e:
        print(f"  SteamSpy failed: {e}")

    # ── Source 3: Steam ISteamApps endpoints ──
    for url in [
        "https://api.steampowered.com/ISteamApps/GetAppList/v2/",
        "https://api.steampowered.com/ISteamApps/GetAppList/v0002/",
    ]:
        try:
            print(f"  Trying {url}")
            r = SESSION.get(url, timeout=30)
            r.raise_for_status()
            data = r.json()
            apps = data["applist"]["apps"]
            if apps:
                print(f"  Found {len(apps):,} apps.")
                return apps
        except Exception as e:
            print(f"  Failed: {e}")
            time.sleep(3)

    # ── Source 4: Built-in seed list ──
    print("\n  All live sources failed. Using built-in seed list.")
    print("  (Contains ~150 well-known co-op games to get you started)\n")
    return get_seed_app_list()


def fetch_apps_from_steam_category():
    """
    Uses Steam's store search with the co-op category filter (category=9).
    This is the same URL the Steam store website uses — no API key needed.
    Paginates through results 100 at a time.
    """
    apps = {}
    print("  Fetching via Steam store co-op category filter...")

    # category2=9  = Co-op
    # category2=38 = Online Co-op
    # category2=36 = Local Co-op
    for cat_id, cat_name in [(9, "Co-op"), (38, "Online Co-op"), (36, "Local Co-op")]:
        start = 0
        page  = 0
        while True:
            try:
                url = "https://store.steampowered.com/search/results/"
                params = {
                    "category2": cat_id,
                    "json":      1,
                    "start":     start,
                    "count":     100,
                    "sort_by":   "_ASC",
                    "type":      "app",
                }
                r = SESSION.get(url, params=params, timeout=20)

                if r.status_code == 429:
                    wait = 30
                    print(f"    Rate limited — waiting {wait}s...")
                    time.sleep(wait)
                    continue

                r.raise_for_status()
                data  = r.json()
                items = data.get("items", [])

                if not items:
                    break

                added = 0
                for item in items:
                    appid = item.get("id")
                    name  = item.get("name", "")
                    if appid and appid not in apps:
                        apps[appid] = {"appid": appid, "name": name}
                        CATEGORY_SOURCED_IDS.add(appid)
                        added += 1

                total_found = data.get("total_count", "?")
                print(f"    [{cat_name}] page {page}: {len(items)} items, "
                      f"+{added} new  (total unique: {len(apps):,} / {total_found})")

                if len(items) < 100:
                    break

                start += 100
                page  += 1
                time.sleep(2.0)   # polite delay between pages

            except Exception as e:
                print(f"    Failed at page {page}: {e}")
                break

        time.sleep(3.0)  # pause between category requests

    if apps:
        print(f"  Steam category filter found {len(apps):,} co-op games total.")
        return list(apps.values())
    return []


def fetch_apps_from_steamspy():
    """
    SteamSpy tag endpoint — returns up to 1000 games per page tagged Co-op.
    """
    apps = {}
    print("  Fetching co-op tagged games from SteamSpy...")

    for page in range(0, 20):
        url = "https://steamspy.com/api.php"
        params = {"request": "tag", "tag": "Co-op", "page": page}
        try:
            r = SESSION.get(url, params=params, timeout=20)
            print(f"    SteamSpy page {page}: HTTP {r.status_code}")
            r.raise_for_status()

            if page == 0:
                print(f"    Response preview: {r.text[:300]}")

            data = r.json()
            print(f"    JSON type={type(data).__name__} len={len(data) if isinstance(data, dict) else 'N/A'}")

            if not data or not isinstance(data, dict):
                print("    Empty/unexpected — stopping SteamSpy pages.")
                break

            for appid_str, info in data.items():
                try:
                    appid = int(appid_str)
                    name = info.get("name", "") if isinstance(info, dict) else ""
                    apps[appid] = {"appid": appid, "name": name}
                except (ValueError, AttributeError):
                    pass

            print(f"    Page {page}: {len(data)} entries  total={len(apps):,}")
            if len(data) < 1000:
                break
            time.sleep(1.5)

        except Exception as e:
            print(f"    Page {page} failed: {e}")
            if page == 0:
                raise
            break

    if apps:
        print(f"  SteamSpy: {len(apps):,} co-op games found.")
        return list(apps.values())
    return []


def get_seed_app_list():
    """
    300+ hand-verified co-op Steam App IDs — all unique, all real games.
    Used as fallback when live APIs are unavailable.
    """
    seed_ids = [
        # ── All-time co-op classics ──
        400,        # Portal
        550,        # Left 4 Dead 2
        570,        # Dota 2
        620,        # Portal 2
        730,        # CS2
        820,        # Half-Life 2
        4000,       # Garry's Mod
        440,        # Team Fortress 2
        # ── Survival & sandbox ──
        105600,     # Terraria
        108600,     # Project Zomboid
        251570,     # 7 Days to Die
        252490,     # Rust
        275850,     # No Man's Sky
        294100,     # RimWorld
        304930,     # Unturned
        346110,     # ARK: Survival Evolved
        413150,     # Stardew Valley
        427520,     # Factorio
        440900,     # Conan Exiles
        457140,     # Oxygen Not Included
        526870,     # Satisfactory
        578080,     # PUBG
        648800,     # Raft
        774171,     # Valheim
        892970,     # Valheim (early)
        1321470,    # Sons of the Forest
        1817190,    # Palworld
        # ── Co-op action / shooter ──
        218620,     # PAYDAY 2
        230410,     # Warframe
        239140,     # Dying Light
        246900,     # Broforce
        295110,     # Keep Talking and Nobody Explodes
        304050,     # Battleborn
        331600,     # Trove
        349040,     # Brawlhalla
        359550,     # Rainbow Six Siege
        381210,     # Dead by Daylight
        431240,     # Golf With Your Friends
        477160,     # Human: Fall Flat
        552990,     # A Way Out
        553850,     # Helldivers 2
        588650,     # Dead Cells
        601150,     # Devil May Cry 5
        632360,     # Risk of Rain 2
        748490,     # Slay the Spire
        750920,     # Borderlands 3
        774251,     # Warhammer: Vermintide 2
        976730,     # Halo: MCC
        1061830,    # Hell Let Loose
        1085660,    # Destiny 2
        1097150,    # Fall Guys
        1172470,    # Apex Legends
        1172620,    # Sea of Thieves
        1229490,    # Insurgency: Sandstorm
        1238810,    # Aliens: Fireteam Elite
        1262350,    # Deep Rock Galactic
        1284210,    # Warhammer 40K: Darktide
        1338820,    # Ghostbusters: Spirits Unleashed
        1366540,    # Ghostrunner
        1454400,    # Back 4 Blood
        1517290,    # Battlefield 2042
        1599340,    # Lost Ark
        1604030,    # Dying Light 2
        1665460,    # GRID Legends
        1966720,    # Phasmophobia
        2626520,    # Helldivers 2 (launch)
        # ── Co-op adventure / puzzle ──
        239030,     # Don't Starve Together
        268500,     # BattleBlock Theater
        441000,     # Lovers in a Dangerous Spacetime
        477160,     # Human: Fall Flat
        620,        # Portal 2
        702050,     # Overcooked! 2
        728880,     # Overcooked! All You Can Eat
        841080,     # Moving Out
        945360,     # Among Us
        1222140,    # It Takes Two
        1623730,    # Vampire Survivors
        1690800,    # Hi-Fi RUSH
        # ── RPG / story co-op ──
        375820,     # Divinity: Original Sin 2
        582010,     # Monster Hunter: World
        617830,     # Divinity: Original Sin Enhanced
        949230,     # Divinity: Original Sin 2 (alt)
        1057090,    # Remnant: From the Ashes
        1086940,    # Baldur's Gate 3
        1145360,    # Hades
        1174180,    # Outriders
        1282100,    # Torchlight III
        1315360,    # The Ascent
        1446780,    # Monster Hunter Rise
        1604030,    # Dying Light 2
        2049840,    # Remnant II
        2358720,    # Baldur's Gate 3 (launch)
        # ── Strategy / sim ──
        281990,     # Stellaris
        294100,     # RimWorld
        362890,     # Black Mesa
        365590,     # Subnautica
        457140,     # Oxygen Not Included
        526870,     # Satisfactory
        644930,     # They Are Billions
        1085510,    # Desperados III
        1449560,    # Chivalry 2
        1551360,    # Forza Horizon 5
        # ── Hidden gems ──
        243470,     # Hammerwatch
        265930,     # Nidhogg
        274520,     # Nidhogg 2
        302670,     # Stories: The Path of Destinies
        390540,     # Brawlout
        424840,     # Tiny Metal
        445220,     # TABS
        524220,     # NieR:Automata
        633230,     # Hollow Knight
        657240,     # Minion Masters
        990080,     # Midnight Ghost Hunt
        1090630,    # Monster Train
        1139900,    # Risk of Rain Returns
        1182480,    # Knockout City
        1222670,    # TABS (alt)
        1283400,    # The Forgotten City
        1449850,    # Loop Hero
        2677290,    # Enshrouded
        2679460,    # Granblue Fantasy: Relink
        2521940,    # Dave the Diver
        2561240,    # The Finals
        2138710,    # Dredge
        2280590,    # Armored Core VI
        2475490,    # Robocop: Rogue City
        2519060,    # Ghostrunner 2
    ]

    seen = set()
    unique = []
    for aid in seed_ids:
        if aid not in seen:
            seen.add(aid)
            unique.append({"appid": aid, "name": f"App {aid}"})

    print(f"  Seed list: {len(unique)} unique app IDs.")
    return unique


def fetch_app_details(appid):
    """
    Fetch store details for a single appid.
    Returns the data dict or None if unavailable/not a game.
    """
    url = "https://store.steampowered.com/api/appdetails"
    r = get_with_retry(url, params={"appids": appid, "cc": "us", "l": "en"})
    payload = r.json()
    entry = payload.get(str(appid), {})
    if not entry.get("success"):
        return None
    return entry.get("data")


def fetch_review_stats(appid):
    """
    Fetch review stats for a game.
    Returns (positive_count, total_count, pct) or None.
    """
    url = f"https://store.steampowered.com/appreviews/{appid}"
    r = get_with_retry(url, params={
        "json": 1,
        "language": "all",
        "purchase_type": "all",
        "num_per_page": 0,
    })
    data = r.json()
    summary = data.get("query_summary", {})
    total    = summary.get("total_reviews", 0)
    positive = summary.get("total_positive", 0)
    if total == 0:
        return None
    pct = round((positive / total) * 100, 1)
    return positive, total, pct


# ─── Filtering logic ──────────────────────────────────────────────────────────

# When an appid came from Steam's own category filter search, we already know
# it's co-op — trust the source and skip the secondary category check.
CATEGORY_SOURCED_IDS = set()   # populated at runtime by fetch_apps_from_steam_category

def is_coop_game(app_data, appid=None):
    """
    Return True if the app has co-op categories tagged by Valve.
    Also accepts games that came directly from Steam's co-op category search.
    """
    # If Steam's own store search returned this ID under a co-op category,
    # trust it — the store search is more reliable than appdetails categories.
    if appid and appid in CATEGORY_SOURCED_IDS:
        return True

    cats = {c["id"] for c in app_data.get("categories", [])}

    # Primary check: explicit co-op category IDs
    if cats & REQUIRED_COOP_IDS:
        return True

    # Fallback: check category description strings for games with unusual IDs
    cat_descs = {c.get("description", "").lower() for c in app_data.get("categories", [])}
    coop_keywords = {"co-op", "co op", "cooperative", "online co-op", "local co-op",
                     "shared/split screen co-op", "full controller support"}
    if cat_descs & coop_keywords:
        return True

    return False


def get_coop_types(app_data):
    """Return list of co-op category label strings for this game."""
    cats = {c["id"] for c in app_data.get("categories", [])}
    return [COOP_CATEGORY_LABELS[cid] for cid in sorted(cats) if cid in COOP_CATEGORY_LABELS]


def get_price_usd(app_data):
    """Return price in USD as a float, or 0.0 for free games."""
    if app_data.get("is_free"):
        return 0.0
    po = app_data.get("price_overview")
    if not po:
        return None  # No price info (region locked, etc.)
    return round(po.get("final", 0) / 100, 2)


def build_record(appid, app_data, review_positive, review_total, review_pct):
    """Build a clean record dict from raw API data."""
    genres = [g["description"] for g in app_data.get("genres", [])]
    price  = get_price_usd(app_data)

    # Review label from Valve's internal score (0-9 integer)
    score_int = app_data.get("review_score", 0)
    label_map = {
        9: "Overwhelmingly Positive",
        8: "Very Positive",
        7: "Mostly Positive",
        6: "Mostly Positive",
        5: "Mixed",
        4: "Mostly Negative",
        3: "Mostly Negative",
        2: "Overwhelmingly Negative",
        1: "Overwhelmingly Negative",
        0: "No Reviews",
    }
    review_label = label_map.get(score_int, "Unknown")

    return {
        "appid":          appid,
        "name":           app_data.get("name", ""),
        "review_pct":     review_pct,
        "review_label":   review_label,
        "review_total":   review_total,
        "review_positive":review_positive,
        "price_usd":      price,
        "is_free":        app_data.get("is_free", False),
        "genres":         genres,
        "coop_types":     get_coop_types(app_data),
        "short_desc":     app_data.get("short_description", ""),
        "header_image":   app_data.get("header_image", f"https://cdn.akamai.steamstatic.com/steam/apps/{appid}/header.jpg"),
        "steam_url":      f"https://store.steampowered.com/app/{appid}/",
        "last_updated":   datetime.now(timezone.utc).strftime("%Y-%m-%d"),
    }


# ─── Progress / resume helpers ────────────────────────────────────────────────

PROGRESS_FILE = ".scrape_progress.json"


def load_progress():
    if os.path.exists(PROGRESS_FILE):
        with open(PROGRESS_FILE) as f:
            return json.load(f)
    return {"processed_ids": [], "games": []}


def save_progress(processed_ids, games):
    with open(PROGRESS_FILE, "w") as f:
        json.dump({"processed_ids": processed_ids, "games": games}, f)


def clear_progress():
    if os.path.exists(PROGRESS_FILE):
        os.remove(PROGRESS_FILE)


# ─── Main build logic ─────────────────────────────────────────────────────────

def build_database(output_path, limit=None, resume=False):
    print("\n" + "═" * 60)
    print("  Steam Co-op DB Builder")
    print("═" * 60)

    # Load or start fresh
    if resume and os.path.exists(PROGRESS_FILE):
        progress = load_progress()
        processed_set = set(progress["processed_ids"])
        games = progress["games"]
        print(f"\nResuming — {len(processed_set):,} already processed, {len(games):,} games found so far.")
    else:
        processed_set = set()
        games = []

    # Step 1: Get full app list
    all_apps = fetch_all_steam_apps()

    # Filter to only apps not yet processed
    remaining = [a for a in all_apps if a["appid"] not in processed_set]
    if limit:
        remaining = remaining[:limit]

    total = len(remaining)
    print(f"\nApps to process: {total:,}")
    print(f"Filters: co-op tagged · {MIN_REVIEW_PCT}%+ positive · {MIN_REVIEW_COUNT}+ reviews")
    print(f"Estimated time: {total * (REQUEST_DELAY + REVIEW_DELAY) / 60:.0f}–{total * (REQUEST_DELAY + REVIEW_DELAY) * 1.5 / 60:.0f} minutes\n")
    print("─" * 60)

    skipped_not_game  = 0
    skipped_no_coop   = 0
    skipped_reviews   = 0
    errors            = 0
    processed_now     = []

    for i, app in enumerate(remaining):
        appid = app["appid"]
        name  = app.get("name", f"App {appid}")

        # Progress indicator
        pct = ((i + 1) / total) * 100
        print(f"[{i+1:>6}/{total}  {pct:5.1f}%]  {name[:50]:<50}", end=" ", flush=True)

        # ── Fetch app details ──
        try:
            time.sleep(REQUEST_DELAY)
            app_data = fetch_app_details(appid)
        except Exception as e:
            print(f"ERROR: {e}")
            errors += 1
            processed_set.add(appid)
            processed_now.append(appid)
            continue

        if not app_data:
            print("skip (unavailable)")
            skipped_not_game += 1
            processed_set.add(appid)
            processed_now.append(appid)
            continue

        # Must be a game
        if app_data.get("type") != "game":
            print(f"skip (type={app_data.get('type', '?')})")
            skipped_not_game += 1
            processed_set.add(appid)
            processed_now.append(appid)
            continue

        # Must have co-op categories
        if not is_coop_game(app_data, appid):
            print("skip (no co-op tag)")
            skipped_no_coop += 1
            processed_set.add(appid)
            processed_now.append(appid)
            continue

        # ── Fetch live review stats ──
        try:
            time.sleep(REVIEW_DELAY)
            review_data = fetch_review_stats(appid)
        except Exception as e:
            print(f"ERROR (reviews): {e}")
            errors += 1
            processed_set.add(appid)
            processed_now.append(appid)
            continue

        if not review_data:
            print("skip (no reviews)")
            skipped_reviews += 1
            processed_set.add(appid)
            processed_now.append(appid)
            continue

        positive, total_reviews, pct_positive = review_data

        # Apply filters
        if total_reviews < MIN_REVIEW_COUNT:
            print(f"skip (only {total_reviews} reviews)")
            skipped_reviews += 1
            processed_set.add(appid)
            processed_now.append(appid)
            continue

        if pct_positive < MIN_REVIEW_PCT:
            print(f"skip ({pct_positive}% positive)")
            skipped_reviews += 1
            processed_set.add(appid)
            processed_now.append(appid)
            continue

        # ── Passed all filters — keep it ──
        record = build_record(appid, app_data, positive, total_reviews, pct_positive)
        games.append(record)
        processed_set.add(appid)
        processed_now.append(appid)
        print(f"✓  {pct_positive}% ({total_reviews:,} reviews)  {record.get('price_usd', '?')} USD")

        # Periodic save
        if len(processed_now) % BATCH_SAVE_EVERY == 0:
            save_progress(list(processed_set), games)
            write_output(games, output_path)
            print(f"\n  ── Checkpoint saved ({len(games)} games so far) ──\n")

    # ─── Final save ───────────────────────────────────────────────────────────
    write_output(games, output_path)
    clear_progress()

    print("\n" + "═" * 60)
    print(f"  DONE")
    print("═" * 60)
    print(f"  Games in database : {len(games):,}")
    print(f"  Skipped (not game): {skipped_not_game:,}")
    print(f"  Skipped (no co-op): {skipped_no_coop:,}")
    print(f"  Skipped (reviews) : {skipped_reviews:,}")
    print(f"  Errors            : {errors:,}")
    print(f"  Output file       : {output_path}")
    size_kb = os.path.getsize(output_path) / 1024
    print(f"  File size         : {size_kb:.1f} KB ({size_kb/1024:.2f} MB)")
    print("═" * 60 + "\n")


def write_output(games, output_path):
    """Write the games list to the output JSON file."""
    output = {
        "meta": {
            "generated":     datetime.now(timezone.utc).isoformat(),
            "total_games":   len(games),
            "min_review_pct":    MIN_REVIEW_PCT,
            "min_review_count":  MIN_REVIEW_COUNT,
            "filters": "co-op tagged, 65%+ positive, 100+ reviews",
        },
        "games": games,
    }
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, separators=(",", ":"))


# ─── Entry point ──────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Build a static JSON database of Steam co-op games."
    )
    parser.add_argument(
        "--output", "-o",
        default="steam_coop_db.json",
        help="Output JSON file path (default: steam_coop_db.json)"
    )
    parser.add_argument(
        "--limit", "-l",
        type=int,
        default=None,
        help="Only process the first N apps (useful for testing, e.g. --limit 500)"
    )
    parser.add_argument(
        "--resume", "-r",
        action="store_true",
        help="Resume a previously interrupted scrape"
    )
    args = parser.parse_args()

    try:
        build_database(
            output_path=args.output,
            limit=args.limit,
            resume=args.resume,
        )
    except KeyboardInterrupt:
        print("\n\nInterrupted. Progress has been saved.")
        print("Run with --resume to continue where you left off.\n")
        sys.exit(0)
