"""
Geocode extracted location names using Nominatim (via Docker or public API).

Maintains a local cache so each unique location is only geocoded once.

Usage:
    python scripts/geocode.py                    # geocode all unresolved locations
    python scripts/geocode.py --docker           # use local Nominatim Docker instance
    python scripts/geocode.py --limit 100        # only geocode first 100 unique locations

Docker setup (run once):
    docker run -d --name nominatim -p 8080:8080 \
        -e PBF_URL=https://download.geofabrik.de/planet-latest.osm.pbf \
        mediagis/nominatim:4.4
    (or use a regional extract for faster import)
"""

import os
import sys
import json
import time
import sqlite3
import argparse
import requests

DATA_DIR = os.path.join(os.path.dirname(__file__), '..', 'data')
DB_FILE = os.path.join(DATA_DIR, 'gutenberg_map.db')
CACHE_FILE = os.path.join(DATA_DIR, 'geocode_cache.json')

PUBLIC_NOMINATIM = 'https://nominatim.openstreetmap.org'
DOCKER_NOMINATIM = 'http://localhost:8080'

# Rate limit for public API (1 req/sec per Nominatim policy)
PUBLIC_DELAY = 1.1

# No delay needed for local Docker
DOCKER_DELAY = 0.01


def load_cache():
    if os.path.exists(CACHE_FILE):
        with open(CACHE_FILE, 'r') as f:
            return json.load(f)
    return {}


def save_cache(cache):
    with open(CACHE_FILE, 'w') as f:
        json.dump(cache, f, indent=2)


def geocode_location(name, base_url, delay):
    """
    Geocode a location name via Nominatim.
    Returns (lat, lon, display_name, place_type) or None.
    """
    params = {
        'q': name,
        'format': 'json',
        'limit': 1,
        'addressdetails': 1,
    }
    headers = {
        'User-Agent': 'GutenbergMap/1.0 (literary geography project)',
    }

    try:
        resp = requests.get(f'{base_url}/search', params=params, headers=headers, timeout=10)
        time.sleep(delay)

        if resp.status_code != 200:
            return None

        results = resp.json()
        if not results:
            return None

        r = results[0]
        return {
            'lat': float(r['lat']),
            'lon': float(r['lon']),
            'display_name': r.get('display_name', name),
            'type': r.get('type', ''),
            'importance': float(r.get('importance', 0)),
        }
    except (requests.RequestException, ValueError, KeyError):
        return None


def get_unique_locations(conn):
    """Get all unique location names from the mentions table."""
    c = conn.cursor()
    c.execute('''
        SELECT location_name, COUNT(*) as mention_count
        FROM mentions
        GROUP BY location_name
        ORDER BY mention_count DESC
    ''')
    return c.fetchall()


def update_db_with_coords(conn):
    """Add lat/lon columns to mentions and populate from cache."""
    c = conn.cursor()

    # Add columns if they don't exist
    try:
        c.execute('ALTER TABLE mentions ADD COLUMN lat REAL')
    except sqlite3.OperationalError:
        pass
    try:
        c.execute('ALTER TABLE mentions ADD COLUMN lon REAL')
    except sqlite3.OperationalError:
        pass
    try:
        c.execute('ALTER TABLE mentions ADD COLUMN geocoded_name TEXT')
    except sqlite3.OperationalError:
        pass

    conn.commit()

    # Load cache and update
    cache = load_cache()
    updated = 0

    for name, geo in cache.items():
        if geo is None:
            continue
        c.execute(
            'UPDATE mentions SET lat = ?, lon = ?, geocoded_name = ? WHERE location_name = ? AND lat IS NULL',
            (geo['lat'], geo['lon'], geo['display_name'], name)
        )
        updated += c.rowcount

    conn.commit()
    return updated


def main():
    parser = argparse.ArgumentParser(description='Geocode literary locations')
    parser.add_argument('--docker', action='store_true', help='Use local Nominatim Docker')
    parser.add_argument('--limit', type=int, default=0, help='Max locations to geocode')
    args = parser.parse_args()

    if args.docker:
        base_url = DOCKER_NOMINATIM
        delay = DOCKER_DELAY
        print('Using local Nominatim Docker instance')
    else:
        base_url = PUBLIC_NOMINATIM
        delay = PUBLIC_DELAY
        print('Using public Nominatim API (1 req/sec)')

    conn = sqlite3.connect(DB_FILE)
    cache = load_cache()

    # Get unique locations
    locations = get_unique_locations(conn)
    print(f'Found {len(locations)} unique location names\n')

    # Filter to uncached
    to_geocode = [(name, count) for name, count in locations if name not in cache]
    print(f'{len(to_geocode)} locations need geocoding ({len(locations) - len(to_geocode)} cached)\n')

    if args.limit > 0:
        to_geocode = to_geocode[:args.limit]

    # Geocode
    resolved = 0
    failed = 0

    for i, (name, count) in enumerate(to_geocode):
        result = geocode_location(name, base_url, delay)
        cache[name] = result

        if result:
            resolved += 1
            print(f'  [{i+1}/{len(to_geocode)}] {name} -> {result["display_name"][:60]} ({result["lat"]:.4f}, {result["lon"]:.4f})')
        else:
            failed += 1
            print(f'  [{i+1}/{len(to_geocode)}] {name} -> NOT FOUND')

        # Save cache periodically
        if (i + 1) % 20 == 0:
            save_cache(cache)

    save_cache(cache)

    # Update database with coordinates
    updated = update_db_with_coords(conn)
    conn.close()

    total_cached = sum(1 for v in cache.values() if v is not None)
    total_failed = sum(1 for v in cache.values() if v is None)

    print(f'\nResults:')
    print(f'  Geocoded this run: {resolved}')
    print(f'  Failed this run: {failed}')
    print(f'  Total in cache: {total_cached} resolved, {total_failed} failed')
    print(f'  DB rows updated: {updated}')


if __name__ == '__main__':
    main()
