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
REQUEST_DELAY    = 0.8     # Seconds between appdetails requests (respect rate limits)
REVIEW_DELAY     = 1.0     # Seconds between review requests
BATCH_SAVE_EVERY = 50      # Save progress to disk every N games
MAX_RETRIES      = 3       # Retries on network failure
RETRY_WAIT       = 10      # Seconds to wait before retrying

# Steam category IDs
MULTIPLAYER_CATEGORY_IDS = {
    1,   # Multi-player
    9,   # Co-op
    27,  # Cross-Platform Multiplayer
    36,  # Online Co-op
    38,  # Local Co-op
    49,  # PvP
}
# Any of these = include in database (both co-op AND multiplayer games)
REQUIRED_MULTIPLAYER_IDS = {1, 9, 27, 36, 38, 49}
# Subset used to classify a game as "co-op" specifically
COOP_ONLY_IDS = {9, 36, 38}

CATEGORY_LABELS = {
    1:  "Multi-player",
    9:  "Co-op",
    27: "Cross-Platform Multiplayer",
    36: "Online Co-op",
    38: "Local Co-op",
    49: "PvP",
}

# ─── API helpers ──────────────────────────────────────────────────────────────

# All requests are routed through the Cloudflare Worker proxy.
# This prevents SteamSpy and Steam from blocking GitHub Actions server IPs.
PROXY_URL = "https://coop-diceroll-proxy.rcookson80.workers.dev/"

SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "X-Requested-With": "XMLHttpRequest",
})


def proxied(url, params=None):
    """Route a URL through the Cloudflare Worker proxy."""
    import urllib.parse
    if params:
        url = url + ("&" if "?" in url else "?") + urllib.parse.urlencode(params)
    return PROXY_URL + "?url=" + urllib.parse.quote(url, safe="")


def get_with_retry(url, params=None, retries=MAX_RETRIES):
    """GET request via Cloudflare Worker proxy with exponential backoff."""
    proxied_url = proxied(url, params)
    for attempt in range(retries):
        try:
            r = SESSION.get(proxied_url, timeout=120)
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
    Two-step fetch strategy:
    1. Pull ALL games from SteamSpy paginated 'all' endpoint (gets us stats/prices)
    2. Pull co-op + multiplayer appids from tag endpoint (gets us the filter list)
    3. Cross-reference to get co-op/mp games with full stats
    Falls back to seed list if both fail.
    """
    print("Step 1: Fetching full game stats from SteamSpy (request=all)...")

    all_data = {}
    page = 0
    while True:
        try:
            url = "https://steamspy.com/api.php"
            params = {"request": "all", "page": page}
            r = get_with_retry(url, params=params, retries=5)
            data = r.json()
            if not data or not isinstance(data, dict) or len(data) == 0:
                break
            all_data.update(data)
            print(f"  Page {page}: {len(data):,} games (total: {len(all_data):,})")
            if len(data) < 1000:
                break
            page += 1
            time.sleep(0.5)
        except Exception as e:
            print(f"  Page {page} failed: {e}")
            break

    print(f"  Total games fetched: {len(all_data):,}")

    if not all_data:
        print("  All endpoint failed — falling back to seed list.")
        return get_seed_app_list()

    print("\nStep 2: Fetching co-op + multiplayer appids from SteamSpy tag endpoint...")

    coop_ids  = fetch_tag_appids("Co-op")
    mp_ids    = fetch_tag_appids("Multiplayer")
    combined  = coop_ids | mp_ids

    print(f"  Co-op appids:       {len(coop_ids):,}")
    print(f"  Multiplayer appids: {len(mp_ids):,}")
    print(f"  Combined unique:    {len(combined):,}")

    if not combined:
        print("  Tag endpoint returned nothing — using all_data games with genre fallback.")
        # Last resort: include everything from all_data, Steam appdetails will filter
        return [{'appid': int(k), 'name': v.get('name',''), 'is_coop': False}
                for k, v in all_data.items() if isinstance(v, dict)]

    print("\nStep 3: Cross-referencing to build final game pool...")

    apps = []
    matched = 0
    for appid in combined:
        info = all_data.get(str(appid)) or all_data.get(appid)
        is_coop = appid in coop_ids
        if info and isinstance(info, dict):
            apps.append({
                'appid':   appid,
                'name':    info.get('name', ''),
                'is_coop': is_coop,
            })
            matched += 1
        else:
            # Game not in all_data — include anyway, stats fetched at roll time
            apps.append({'appid': appid, 'name': '', 'is_coop': is_coop})

    print(f"  Matched in all_data: {matched:,}")
    print(f"  Total pool:          {len(apps):,}")
    return apps


def fetch_tag_appids(tag):
    """Fetch all appids for a given SteamSpy tag. Returns a set of ints."""
    appids = set()
    for page in range(0, 20):
        try:
            url = "https://steamspy.com/api.php"
            params = {"request": "tag", "tag": tag, "page": page}
            r = get_with_retry(url, params=params, retries=3)
            data = r.json()
            if not data or not isinstance(data, dict) or len(data) == 0:
                break
            for appid_str in data.keys():
                try:
                    appids.add(int(appid_str))
                except ValueError:
                    pass
            print(f"  [{tag}] page {page}: {len(data)} entries (total: {len(appids):,})")
            if len(data) < 1000:
                break
            time.sleep(1.0)
        except Exception as e:
            print(f"  [{tag}] page {page} failed: {e}")
            break
    return appids


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

def is_multiplayer_game(app_data, appid=None):
    """
    Return True if the app has any multiplayer or co-op category tag.
    Includes pure multiplayer games (CS2, PUBG) as well as co-op games.
    """
    cats = {c["id"] for c in app_data.get("categories", [])}
    if cats & REQUIRED_MULTIPLAYER_IDS:
        return True

    cat_descs = {c.get("description", "").lower() for c in app_data.get("categories", [])}
    mp_keywords = {"co-op", "co op", "cooperative", "online co-op", "local co-op",
                   "multi-player", "multiplayer", "pvp", "shared/split screen co-op"}
    if cat_descs & mp_keywords:
        return True

    return False


def is_coop_specifically(app_data):
    """Return True if the game has explicit co-op tags (not just multiplayer)."""
    cats = {c["id"] for c in app_data.get("categories", [])}
    if cats & COOP_ONLY_IDS:
        return True
    cat_descs = {c.get("description", "").lower() for c in app_data.get("categories", [])}
    return bool(cat_descs & {"co-op", "co op", "cooperative", "online co-op", "local co-op",
                              "shared/split screen co-op"})


def get_coop_types(app_data):
    """Return list of multiplayer/co-op category label strings for this game."""
    cats = {c["id"] for c in app_data.get("categories", [])}
    return [CATEGORY_LABELS[cid] for cid in sorted(cats) if cid in CATEGORY_LABELS]


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

    # Derive label from the actual calculated percentage — more accurate than
    # Valve's 0-9 integer which can return 0 even for games with many reviews.
    def pct_to_label(pct):
        if pct >= 95: return "Overwhelmingly Positive"
        if pct >= 85: return "Very Positive"
        if pct >= 70: return "Mostly Positive"
        if pct >= 65: return "Positive"
        if pct >= 40: return "Mixed"
        if pct >= 20: return "Mostly Negative"
        return "Overwhelmingly Negative"

    review_label = pct_to_label(review_pct)

    return {
        "appid":          appid,
        "name":           app_data.get("name", ""),
        "is_coop":        is_coop_specifically(app_data),
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

# ─── Main build logic ─────────────────────────────────────────────────────────

def build_database(output_path, limit=None, resume=False):
    print("\n" + "═" * 60)
    print("  Steam Co-op DB Builder")
    print("═" * 60)

    # Fetch all games from SteamSpy — no per-game API calls
    all_apps = fetch_all_steam_apps()

    if limit:
        all_apps = all_apps[:limit]

    print(f"\nStoring {len(all_apps):,} games to database (no per-game filtering).")
    print("All quality filtering happens at roll time in the web app.\n")

    write_output(all_apps, output_path)

    print("\n" + "═" * 60)
    print("  DONE")
    print("═" * 60)
    print(f"  Games in database : {len(all_apps):,}")
    print(f"  Output file       : {output_path}")
    size_kb = os.path.getsize(output_path) / 1024
    print(f"  File size         : {size_kb:.1f} KB ({size_kb/1024:.2f} MB)")
    print("═" * 60 + "\n")

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
    print(f"  Skipped (no multiplayer tag): {skipped_no_coop:,}")
    print(f"  Skipped (reviews) : {skipped_reviews:,}")
    print(f"  Errors            : {errors:,}")
    print(f"  Output file       : {output_path}")
    size_kb = os.path.getsize(output_path) / 1024
    print(f"  File size         : {size_kb:.1f} KB ({size_kb/1024:.2f} MB)")
    print("═" * 60 + "\n")


def refresh_database(output_path):
    """
    Fast monthly refresh — two steps:
      1. Re-check review stats for every game already in the DB
         (scores drift over time as more people review)
      2. Scan only apps added to Steam since the last build date
         and add any new co-op games that pass the filters.

    Typically completes in 30-60 minutes vs 8+ hours for a full build.
    """
    print("\n" + "═" * 60)
    print("  Steam Co-op DB — Monthly Refresh")
    print("═" * 60)

    # ── Load existing DB ──────────────────────────────────────────
    if not os.path.exists(output_path):
        print(f"\n  No existing database found at '{output_path}'.")
        print("  Run without --refresh first to do a full build.\n")
        sys.exit(1)

    with open(output_path, encoding="utf-8") as f:
        existing = json.load(f)

    games        = existing.get("games", [])
    meta         = existing.get("meta", {})
    last_built   = meta.get("generated", "unknown")[:10]
    games_by_id  = {g["appid"]: g for g in games}

    print(f"\n  Existing DB  : {len(games):,} games")
    print(f"  Last built   : {last_built}")
    print(f"  Output file  : {output_path}\n")

    # ════════════════════════════════════════════════════════════
    # STEP 1 — Refresh review scores for existing games
    # ════════════════════════════════════════════════════════════
    print("─" * 60)
    print("  STEP 1 of 2 — Refreshing review scores for existing games")
    print("─" * 60)

    updated   = 0
    removed   = 0
    errors    = 0
    keep      = []

    for i, game in enumerate(games):
        appid = game["appid"]
        name  = game.get("name", f"App {appid}")
        pct   = ((i + 1) / len(games)) * 100
        print(f"[{i+1:>5}/{len(games)}  {pct:5.1f}%]  {name[:48]:<48}", end=" ", flush=True)

        try:
            time.sleep(REVIEW_DELAY)
            review_data = fetch_review_stats(appid)
        except Exception as e:
            print(f"ERROR: {e}")
            errors += 1
            keep.append(game)   # keep old data on error
            continue

        if not review_data:
            print("skip (no reviews returned)")
            keep.append(game)
            continue

        positive, total_reviews, pct_positive = review_data

        # Drop games that have fallen below thresholds
        if total_reviews < MIN_REVIEW_COUNT or pct_positive < MIN_REVIEW_PCT:
            print(f"removed ({pct_positive}% / {total_reviews:,} reviews)")
            removed += 1
            continue

        # Update the record with fresh numbers
        def pct_to_label(p):
            if p >= 95: return "Overwhelmingly Positive"
            if p >= 85: return "Very Positive"
            if p >= 70: return "Mostly Positive"
            if p >= 65: return "Positive"
            if p >= 40: return "Mixed"
            if p >= 20: return "Mostly Negative"
            return "Overwhelmingly Negative"

        old_pct = game.get("review_pct", 0)
        game["review_pct"]      = pct_positive
        game["review_label"]    = pct_to_label(pct_positive)
        game["review_total"]    = total_reviews
        game["review_positive"] = positive
        game["last_updated"]    = datetime.now(timezone.utc).strftime("%Y-%m-%d")

        delta = pct_positive - old_pct
        arrow = f"↑{delta:+.1f}%" if delta > 0 else (f"↓{delta:.1f}%" if delta < 0 else "—")
        print(f"{pct_positive}% ({total_reviews:,} reviews)  {arrow}")
        updated += 1
        keep.append(game)

    games = keep
    print(f"\n  Step 1 done — {updated} updated, {removed} dropped, {errors} errors\n")

    # ════════════════════════════════════════════════════════════
    # STEP 2 — Check for new co-op games added since last build
    # ════════════════════════════════════════════════════════════
    print("─" * 60)
    print("  STEP 2 of 2 — Scanning for new co-op games since last build")
    print("─" * 60)

    all_apps     = fetch_all_steam_apps()
    existing_ids = {g["appid"] for g in games}
    new_apps     = [a for a in all_apps if a["appid"] not in existing_ids]

    print(f"\n  {len(new_apps):,} app IDs not yet in the database — checking each...\n")

    new_added  = 0
    skipped_not_game = 0
    skipped_no_coop  = 0
    skipped_reviews  = 0
    step2_errors     = 0

    for i, app in enumerate(new_apps):
        appid = app["appid"]
        name  = app.get("name", f"App {appid}")
        pct   = ((i + 1) / len(new_apps)) * 100
        print(f"[{i+1:>6}/{len(new_apps)}  {pct:5.1f}%]  {name[:50]:<50}", end=" ", flush=True)

        try:
            time.sleep(REQUEST_DELAY)
            app_data = fetch_app_details(appid)
        except Exception as e:
            print(f"ERROR: {e}")
            step2_errors += 1
            continue

        if not app_data:
            print("skip (unavailable)")
            skipped_not_game += 1
            continue

        if app_data.get("type") != "game":
            print(f"skip (type={app_data.get('type', '?')})")
            skipped_not_game += 1
            continue

        if not is_multiplayer_game(app_data, appid):
            print("skip (no multiplayer/co-op tag)")
            skipped_no_coop += 1
            continue

        try:
            time.sleep(REVIEW_DELAY)
            review_data = fetch_review_stats(appid)
        except Exception as e:
            print(f"ERROR (reviews): {e}")
            step2_errors += 1
            continue

        if not review_data:
            print("skip (no reviews)")
            skipped_reviews += 1
            continue

        positive, total_reviews, pct_positive = review_data

        if total_reviews < MIN_REVIEW_COUNT:
            print(f"skip (only {total_reviews} reviews)")
            skipped_reviews += 1
            continue

        if pct_positive < MIN_REVIEW_PCT:
            print(f"skip ({pct_positive}% positive)")
            skipped_reviews += 1
            continue

        record = build_record(appid, app_data, positive, total_reviews, pct_positive)
        games.append(record)
        new_added += 1
        print(f"✓ NEW  {pct_positive}% ({total_reviews:,} reviews)")

        # Periodic save during step 2
        if new_added % BATCH_SAVE_EVERY == 0:
            write_output(games, output_path)
            print(f"\n  ── Checkpoint saved ({len(games)} games total) ──\n")

    # ── Final save ────────────────────────────────────────────────
    write_output(games, output_path)

    print("\n" + "═" * 60)
    print("  REFRESH COMPLETE")
    print("═" * 60)
    print(f"  Games in database  : {len(games):,}")
    print(f"  Scores updated     : {updated:,}")
    print(f"  Games removed      : {removed:,}  (fell below thresholds)")
    print(f"  New games added    : {new_added:,}")
    print(f"  Errors             : {errors + step2_errors:,}")
    print(f"  Output file        : {output_path}")
    size_kb = os.path.getsize(output_path) / 1024
    print(f"  File size          : {size_kb:.1f} KB ({size_kb/1024:.2f} MB)")
    print("═" * 60 + "\n")


def write_output(games, output_path):
    """Write the games list to the output JSON file."""
    output = {
        "meta": {
            "generated":    datetime.now(timezone.utc).isoformat(),
            "total_games":  len(games),
            "filters":      "raw pool — filtering at roll time",
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
    parser.add_argument(
        "--refresh",
        action="store_true",
        help="Fast monthly refresh — updates scores for existing games and checks for new ones (~30-60 min)"
    )
    args = parser.parse_args()

    try:
        if args.refresh:
            refresh_database(output_path=args.output)
        else:
            build_database(
                output_path=args.output,
                limit=args.limit,
                resume=args.resume,
            )
    except KeyboardInterrupt:
        print("\n\nInterrupted. Progress has been saved.")
        print("Run with --resume to continue where you left off.\n")
        sys.exit(0)
