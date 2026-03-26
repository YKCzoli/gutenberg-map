"""
Extract geographic locations from downloaded books using spaCy NER.

Supports parallel workers for faster processing.

Usage:
    python scripts/extract.py                    # 2 parallel workers (default)
    python scripts/extract.py --workers 1        # single worker (lowest memory)
    python scripts/extract.py --ids 345 1342     # specific books
    python scripts/extract.py --status           # show progress
"""

import os
import sys
import json
import re
import sqlite3
import gc
import multiprocessing as mp
from functools import partial

DATA_DIR = os.path.join(os.path.dirname(__file__), '..', 'data')
BOOKS_DIR = os.path.join(DATA_DIR, 'books')
CATALOG_FILE = os.path.join(DATA_DIR, 'book_catalog.json')
DB_FILE = os.path.join(DATA_DIR, 'gutenberg_map.db')

CONTEXT_CHARS = 200
MIN_ENTITY_LEN = 2

SKIP_ENTITIES = {
    'god', 'lord', 'sir', 'mr', 'mrs', 'miss', 'dr', 'st',
    'christmas', 'easter', 'sunday', 'monday', 'tuesday',
    'wednesday', 'thursday', 'friday', 'saturday',
    'january', 'february', 'march', 'april', 'may', 'june',
    'july', 'august', 'september', 'october', 'november', 'december',
    'the', 'chapter', 'act', 'scene', 'part', 'book',
}


def init_db():
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS books (
        id TEXT PRIMARY KEY, title TEXT NOT NULL, author TEXT, gutenberg_id TEXT
    )''')
    c.execute('''CREATE TABLE IF NOT EXISTS mentions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        book_id TEXT NOT NULL, location_name TEXT NOT NULL,
        chapter TEXT, line_number INTEGER, passage TEXT, char_offset INTEGER,
        FOREIGN KEY (book_id) REFERENCES books(id)
    )''')
    c.execute('CREATE INDEX IF NOT EXISTS idx_mentions_book ON mentions(book_id)')
    c.execute('CREATE INDEX IF NOT EXISTS idx_mentions_location ON mentions(location_name)')
    conn.commit()
    return conn


def detect_chapters(text):
    patterns = [
        r'^(CHAPTER\s+[IVXLCDM\d]+[.\s]*.*?)$',
        r'^(Chapter\s+\d+[.\s]*.*?)$',
        r'^(BOOK\s+[IVXLCDM\d]+[.\s]*.*?)$',
        r'^(ACT\s+[IVXLCDM]+[.\s]*.*?)$',
        r'^(ADVENTURE\s+[IVXLCDM\d]+[.\s]*.*?)$',
    ]
    chapters = []
    for pattern in patterns:
        for m in re.finditer(pattern, text, re.MULTILINE | re.IGNORECASE):
            label = m.group(1).strip()
            if len(label) > 80:
                label = label[:80] + '...'
            chapters.append((m.start(), label))
        if chapters:
            break
    chapters.sort(key=lambda x: x[0])
    return chapters


def get_chapter_for_offset(chapters, offset):
    if not chapters:
        return None
    chapter = None
    for start, label in chapters:
        if start <= offset:
            chapter = label
        else:
            break
    return chapter


def get_line_number(text, offset):
    return text[:offset].count('\n') + 1


def extract_passage(text, start, end):
    ctx_start = max(0, start - CONTEXT_CHARS)
    for boundary in ['. ', '.\n', '!\n', '?\n', '! ', '? ', '\n\n']:
        idx = text.find(boundary, ctx_start, start)
        if idx != -1:
            ctx_start = idx + len(boundary)
            break
    ctx_end = min(len(text), end + CONTEXT_CHARS)
    for boundary in ['. ', '.\n', '!\n', '?\n', '! ', '? ', '\n\n']:
        idx = text.find(boundary, end, ctx_end)
        if idx != -1:
            ctx_end = idx + 1
            break
    passage = text[ctx_start:ctx_end].strip()
    return re.sub(r'\s+', ' ', passage)


def extract_single_book(book_meta):
    """
    Process a single book in its own process. Loads spaCy model per-worker
    (via the initializer), extracts mentions, returns them as a list.
    """
    import spacy
    book_id = book_meta['id']
    title = book_meta['title']
    author = book_meta['author']
    filepath = os.path.join(BOOKS_DIR, book_meta['file'])

    if not os.path.exists(filepath):
        return book_id, title, author, []

    # Each worker loads its own model (cached after first call via globals)
    global _nlp
    if '_nlp' not in globals() or _nlp is None:
        _nlp = spacy.load('en_core_web_trf')
        _nlp.max_length = 3_000_000

    with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
        text = f.read()

    chapters = detect_chapters(text)
    chunk_size = 100_000
    mentions = []
    seen_dedup = set()

    for chunk_start in range(0, len(text), chunk_size):
        chunk = text[chunk_start:chunk_start + chunk_size]
        doc = _nlp(chunk)

        for ent in doc.ents:
            if ent.label_ not in ('GPE', 'LOC', 'FAC'):
                continue
            name = ent.text.strip()
            if len(name) < MIN_ENTITY_LEN:
                continue
            if name.lower() in SKIP_ENTITIES:
                continue
            if not re.search(r'[a-zA-Z]', name):
                continue

            abs_start = chunk_start + ent.start_char
            abs_end = chunk_start + ent.end_char
            line_num = get_line_number(text, abs_start)
            chapter = get_chapter_for_offset(chapters, abs_start)
            passage = extract_passage(text, abs_start, abs_end)

            dedup_key = (name.lower(), line_num)
            if dedup_key in seen_dedup:
                continue
            seen_dedup.add(dedup_key)

            mentions.append((book_id, name, chapter, line_num, passage, abs_start))

        del doc
        gc.collect()

    del text
    gc.collect()

    return book_id, title, author, mentions


def init_worker():
    """Initialize spaCy model in each worker process."""
    global _nlp
    import spacy
    _nlp = spacy.load('en_core_web_trf')
    _nlp.max_length = 3_000_000


def get_books_to_extract(conn, specific_ids=None):
    c = conn.cursor()

    if specific_ids:
        placeholders = ','.join('?' * len(specific_ids))
        c.execute(f'''
            SELECT bc.gutenberg_id, bc.title, bc.author, bc.file_path
            FROM book_catalog bc
            WHERE bc.status = 'downloaded'
            AND bc.gutenberg_id IN ({placeholders})
        ''', specific_ids)
    else:
        c.execute('''
            SELECT bc.gutenberg_id, bc.title, bc.author, bc.file_path
            FROM book_catalog bc
            WHERE bc.status = 'downloaded'
            AND bc.gutenberg_id NOT IN (
                SELECT DISTINCT book_id FROM mentions
            )
            ORDER BY CAST(bc.gutenberg_id AS INTEGER)
        ''')

    rows = c.fetchall()
    books = [
        {'id': r[0], 'title': r[1], 'author': r[2], 'file': r[3] or f'{r[0]}.txt'}
        for r in rows
    ]

    # Fallback: JSON catalog
    if not books and os.path.exists(CATALOG_FILE):
        with open(CATALOG_FILE, 'r') as f:
            catalog = json.load(f)
        if specific_ids:
            catalog = {k: v for k, v in catalog.items() if k in specific_ids}
        c.execute('SELECT DISTINCT book_id FROM mentions')
        already = {r[0] for r in c.fetchall()}
        books = [
            {'id': k, 'title': v['title'], 'author': v['author'], 'file': v['file']}
            for k, v in catalog.items() if k not in already
        ]

    return books


def main():
    import argparse
    parser = argparse.ArgumentParser(description='Extract locations from books')
    parser.add_argument('--ids', nargs='+', help='Specific Gutenberg IDs')
    parser.add_argument('--limit', type=int, default=0, help='Max books to process')
    parser.add_argument('--workers', type=int, default=2, help='Parallel workers (default 2)')
    parser.add_argument('--status', action='store_true', help='Show extraction status')
    args = parser.parse_args()

    conn = init_db()

    if args.status:
        c = conn.cursor()
        c.execute('SELECT COUNT(DISTINCT book_id) FROM mentions')
        extracted = c.fetchone()[0]
        c.execute('SELECT COUNT(*) FROM mentions')
        total_mentions = c.fetchone()[0]
        c.execute("SELECT COUNT(*) FROM book_catalog WHERE status='downloaded'")
        downloaded = c.fetchone()[0]
        print(f'Extracted: {extracted}/{downloaded} downloaded books')
        print(f'Total mentions: {total_mentions:,}')
        conn.close()
        return

    books = get_books_to_extract(conn, args.ids)

    if args.limit > 0:
        books = books[:args.limit]

    if not books:
        print('No new books to extract. All downloaded books already processed.')
        conn.close()
        return

    num_workers = min(args.workers, len(books))
    print(f'Extracting locations from {len(books)} books with {num_workers} workers...')
    print(f'(Each worker loads its own spaCy model — give it a minute to start)\n')

    total_mentions = 0
    done = 0

    try:
        with mp.Pool(num_workers, initializer=init_worker) as pool:
            for book_id, title, author, mentions in pool.imap_unordered(extract_single_book, books):
                done += 1
                short = (title[:50] + '...') if len(title) > 50 else title

                if mentions:
                    # Write to DB from the main process (SQLite isn't multi-writer safe)
                    c = conn.cursor()
                    c.execute('INSERT OR REPLACE INTO books VALUES (?, ?, ?, ?)',
                              (book_id, title, author, book_id))
                    c.executemany(
                        'INSERT INTO mentions (book_id, location_name, chapter, line_number, passage, char_offset) VALUES (?, ?, ?, ?, ?, ?)',
                        mentions
                    )
                    conn.commit()
                    total_mentions += len(mentions)
                    print(f'  [{done}/{len(books)}] {short} — {len(mentions)} mentions')
                else:
                    print(f'  [{done}/{len(books)}] {short} — no locations found')

    except KeyboardInterrupt:
        print(f'\n\nInterrupted! {done} books saved ({total_mentions:,} mentions).')
        pool.terminate()

    conn.close()
    print(f'\nDone: {done} books, {total_mentions:,} mentions extracted')
    print(f'Database: {DB_FILE}')


if __name__ == '__main__':
    main()
