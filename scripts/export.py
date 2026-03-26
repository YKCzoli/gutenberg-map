"""
Export geocoded mentions from SQLite to a slim GeoJSON index + passage chunks.

Produces:
  data/locations.geojson        — ~7k location features (coords, names, book refs, relevance)
  data/passages/chunk_XX.json   — 100 passage chunk files, loaded on demand

Usage:
    python scripts/export.py                    # export all
    python scripts/export.py --min-mentions 2   # only locations mentioned 2+ times
"""

import os
import sys
import json
import hashlib
import sqlite3
import shutil
import argparse

DATA_DIR = os.path.join(os.path.dirname(__file__), '..', 'data')
DB_FILE = os.path.join(DATA_DIR, 'gutenberg_map.db')
OUTPUT_FILE = os.path.join(DATA_DIR, 'locations.geojson')
PASSAGES_DIR = os.path.join(DATA_DIR, 'passages')
NUM_CHUNKS = 100


def location_key(lat, lon):
    """Match the frontend's locationKey: round to 1 decimal."""
    lng = round(lon * 10) / 10
    lat = round(lat * 10) / 10
    return f'{lat},{lng}'


def chunk_id(key):
    """Deterministic chunk assignment from location key."""
    h = hashlib.md5(key.encode()).hexdigest()
    return int(h, 16) % NUM_CHUNKS


def export(conn, min_mentions=1):
    c = conn.cursor()

    c.execute('''
        SELECT
            m.location_name, m.lat, m.lon,
            m.chapter, m.line_number, m.passage, m.char_offset,
            b.id as book_id, b.title as book_title, b.author,
            COALESCE(m.relevance, 0.5) as relevance,
            COALESCE(m.mention_type, 'mention') as mention_type
        FROM mentions m
        JOIN books b ON m.book_id = b.id
        WHERE m.lat IS NOT NULL AND m.lon IS NOT NULL
        ORDER BY b.id, m.char_offset
    ''')

    rows = c.fetchall()

    # Track per-book narrative order
    book_order = {}

    # Collect all books metadata
    books_meta = {}

    # Group data by location key
    locations = {}   # key -> {coords, name, books_at_loc}
    passages = {}    # key -> [passage_dicts]

    for row in rows:
        (loc_name, lat, lon, chapter, line_num, passage,
         char_offset, book_id, book_title, author,
         relevance, mention_type) = row

        if book_id not in book_order:
            book_order[book_id] = 0
        book_order[book_id] += 1
        order = book_order[book_id]

        # Register book
        if book_id not in books_meta:
            books_meta[book_id] = {'title': book_title, 'author': author}

        key = location_key(lat, lon)

        # Build location index entry
        if key not in locations:
            locations[key] = {
                'lat': lat, 'lon': lon,
                'name': loc_name,
                'books': {},  # book_id -> {max_relevance, min_order, chapter}
                'count': 0,
                'max_relevance': 0,
            }

        loc = locations[key]
        loc['count'] += 1
        if relevance > loc['max_relevance']:
            loc['max_relevance'] = relevance

        if book_id not in loc['books']:
            loc['books'][book_id] = {
                'r': relevance,
                'o': order,
                'ch': chapter or '',
            }
        else:
            book_entry = loc['books'][book_id]
            if relevance > book_entry['r']:
                book_entry['r'] = relevance
            if order < book_entry['o']:
                book_entry['o'] = order

        # Build passage entry
        if key not in passages:
            passages[key] = []
        passages[key].append({
            'b': book_id,
            'ch': chapter or '',
            'l': line_num,
            'p': passage or '',
            'r': relevance,
            't': mention_type,
        })

    # Filter by min mentions
    if min_mentions > 1:
        locations = {k: v for k, v in locations.items() if v['count'] >= min_mentions}
        passages = {k: v for k, v in passages.items() if k in locations}

    # Build slim GeoJSON index
    features = []
    for key, loc in locations.items():
        books_list = []
        for bid, bdata in loc['books'].items():
            books_list.append({
                'i': bid,
                'r': bdata['r'],
                'o': bdata['o'],
                'ch': bdata['ch'],
            })

        features.append({
            'type': 'Feature',
            'geometry': {
                'type': 'Point',
                'coordinates': [loc['lon'], loc['lat']],
            },
            'properties': {
                'k': key,
                'n': loc['name'],
                'r': round(loc['max_relevance'], 3),
                'c': loc['count'],
                'ch': chunk_id(key),
                'b': books_list,
            },
        })

    geojson = {
        'type': 'FeatureCollection',
        'books': books_meta,
        'features': features,
    }

    # Build passage chunks
    chunks = {}
    for key, plist in passages.items():
        cid = chunk_id(key)
        if cid not in chunks:
            chunks[cid] = {}
        chunks[cid][key] = plist

    return geojson, chunks, books_meta


def main():
    parser = argparse.ArgumentParser(description='Export to GeoJSON + passage chunks')
    parser.add_argument('--min-mentions', type=int, default=1,
                        help='Minimum mentions for a location to be included')
    args = parser.parse_args()

    if not os.path.exists(DB_FILE):
        print('Database not found. Run extract.py and geocode.py first.')
        sys.exit(1)

    conn = sqlite3.connect(DB_FILE)
    geojson, chunks, books_meta = export(conn, args.min_mentions)
    conn.close()

    # Write index
    with open(OUTPUT_FILE, 'w') as f:
        json.dump(geojson, f)

    # Write passage chunks
    if os.path.exists(PASSAGES_DIR):
        shutil.rmtree(PASSAGES_DIR)
    os.makedirs(PASSAGES_DIR)

    for cid, data in chunks.items():
        path = os.path.join(PASSAGES_DIR, f'chunk_{cid:02d}.json')
        with open(path, 'w') as f:
            json.dump(data, f)

    index_size = os.path.getsize(OUTPUT_FILE) / 1024 / 1024
    total_passages = sum(len(v) for ch in chunks.values() for v in ch.values())

    print(f'Index: {len(geojson["features"])} locations ({index_size:.1f} MB)')
    print(f'  {len(books_meta)} books')
    print(f'  {total_passages:,} passages in {len(chunks)} chunks')
    print(f'  Output: {OUTPUT_FILE}')
    print(f'  Passages: {PASSAGES_DIR}/')


if __name__ == '__main__':
    main()
