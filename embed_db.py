#!/usr/bin/env python3
"""
embed_db.py — Bundles steam_coop_db.json into steam-coop-finder.html
=====================================================================
Creates a fully standalone HTML file that works by just double-clicking.
No web server, no Python, no dependencies needed to run the output file.

Usage:
    python embed_db.py

    # Custom paths:
    python embed_db.py --html steam-coop-finder.html --db steam_coop_db.json --out coop-diceroll-standalone.html
"""

import argparse
import json
import os
import re
import sys
from datetime import datetime


def embed(html_path, db_path, out_path):

    # ── Validate inputs ──────────────────────────────────────────────────────
    if not os.path.exists(html_path):
        print(f"Error: HTML file not found: '{html_path}'")
        sys.exit(1)

    if not os.path.exists(db_path):
        print(f"Error: Database file not found: '{db_path}'")
        sys.exit(1)

    print(f"Reading HTML  : {html_path}")
    print(f"Reading DB    : {db_path}")

    with open(html_path, encoding='utf-8') as f:
        html = f.read()

    with open(db_path, encoding='utf-8') as f:
        db_raw = f.read()

    # Validate JSON
    try:
        db_data = json.loads(db_raw)
    except json.JSONDecodeError as e:
        print(f"Error: Database JSON is invalid: {e}")
        sys.exit(1)

    game_count = len(db_data.get('games', []))
    db_date    = db_data.get('meta', {}).get('generated', 'unknown')[:10]
    print(f"  {game_count:,} games found, built {db_date}")

    # ── Replace the entire loadDB() function using regex ─────────────────────
    # Match from "async function loadDB()" through its closing brace
    pattern = re.compile(
        r'async function loadDB\(\)\s*\{.*?\n\}',
        re.DOTALL
    )

    if not pattern.search(html):
        print("Error: Could not find loadDB() function in HTML.")
        print("Make sure you're using the latest steam-coop-finder.html.")
        sys.exit(1)

    new_load_db = """\
async function loadDB() {
  setStatus('<span class="dot-anim">Loading game database</span>');
  try {
    const json = window.__COOP_DB__;
    if (!json) throw new Error('Embedded database not found.');
    DB = json.games || [];
    dbLoaded = true;
    const meta = json.meta || {};
    const date = meta.generated ? meta.generated.slice(0, 10) : 'unknown';
    document.getElementById('dbMeta').textContent =
      `${DB.length.toLocaleString()} games \u00b7 built ${date}`;
    setStatus('');
    document.getElementById('rollBtn').disabled = false;
  } catch(e) {
    setStatus('');
    document.getElementById('resultArea').innerHTML = `
      <div class="error-box">
        <strong>Embedded database error.</strong><br><br>
        ${e.message}<br><br>
        Try re-running embed_db.py to regenerate this file.
      </div>`;
    document.getElementById('dbMeta').textContent = 'error';
  }
}"""

    html = pattern.sub(new_load_db, html, count=1)

    # ── Inject DB as a JS variable just before </body> ────────────────────────
    # Remove any previously embedded DB first (idempotent re-runs)
    html = re.sub(
        r'\n<script>\n// ─+ Embedded Co-op Diceroll Database.*?</script>\n',
        '',
        html,
        flags=re.DOTALL
    )

    db_compact = json.dumps(db_data, ensure_ascii=False, separators=(',', ':'))
    now_str    = datetime.now().strftime('%Y-%m-%d %H:%M')

    db_script = (
        f'<script>\n'
        f'// Embedded Co-op Diceroll Database — {now_str}\n'
        f'// {game_count:,} games, built {db_date}\n'
        f'window.__COOP_DB__ = {db_compact};\n'
        f'</script>\n'
    )

    # Inject BEFORE the main <script> block so __COOP_DB__ exists when loadDB() runs
    # Find the first <script> tag inside the body (the app script)
    body_script_pos = html.find('<script>\n// \u2500\u2500\u2500 Database')
    if body_script_pos == -1:
        # Fallback: find any <script> after <body>
        body_script_pos = html.find('<script>', html.find('<body>'))

    if body_script_pos == -1:
        print("Error: Could not find main script block to inject before.")
        sys.exit(1)

    html = html[:body_script_pos] + db_script + html[body_script_pos:]

    # ── Write output ──────────────────────────────────────────────────────────
    with open(out_path, 'w', encoding='utf-8') as f:
        f.write(html)

    size_kb = os.path.getsize(out_path) / 1024
    print(f"\nOutput file   : {out_path}")
    print(f"File size     : {size_kb:.0f} KB ({size_kb/1024:.2f} MB)")
    print(f"Games embedded: {game_count:,}")
    print(f"\nDone! Share '{out_path}' with anyone \u2014 they just double-click to open it.")
    print("No Python, no server, no setup required on their end.\n")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Embed steam_coop_db.json into steam-coop-finder.html as a standalone file.'
    )
    parser.add_argument('--html', default='steam-coop-finder.html',
                        help='Source HTML file (default: steam-coop-finder.html)')
    parser.add_argument('--db',   default='steam_coop_db.json',
                        help='Database JSON file (default: steam_coop_db.json)')
    parser.add_argument('--out',  default='coop-diceroll-standalone.html',
                        help='Output filename (default: coop-diceroll-standalone.html)')
    args = parser.parse_args()

    embed(args.html, args.db, args.out)
