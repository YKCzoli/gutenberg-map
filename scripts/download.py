"""
Download plain-text books from Project Gutenberg.

Uses the Gutenberg RSS/catalog to discover books. Tracks download state
in a SQLite database so you can cancel and resume at any time.

Usage:
    python scripts/download.py                     # download all English fiction
    python scripts/download.py --limit 100         # download up to 100 new books
    python scripts/download.py --ids 84 1342 76    # download specific IDs
    python scripts/download.py --status             # show download stats
    python scripts/download.py --category fiction   # filter by category (default: all)
"""

import os
import sys
import json
import time
import sqlite3
import argparse
import csv
import io
import requests

DATA_DIR = os.path.join(os.path.dirname(__file__), '..', 'data')
BOOKS_DIR = os.path.join(DATA_DIR, 'books')
DB_FILE = os.path.join(DATA_DIR, 'gutenberg_map.db')

GUTENBERG_CATALOG_URL = 'https://www.gutenberg.org/cache/epub/feeds/pg_catalog.csv'

GUTENBERG_TEXT_URLS = [
    'https://www.gutenberg.org/cache/epub/{id}/pg{id}.txt',
    'https://www.gutenberg.org/files/{id}/{id}-0.txt',
]

# Delay between downloads to be nice to Gutenberg servers
DOWNLOAD_DELAY = 0.5


def init_db():
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()

    c.execute('''CREATE TABLE IF NOT EXISTS book_catalog (
        gutenberg_id TEXT PRIMARY KEY,
        title TEXT,
        author TEXT,
        language TEXT,
        subject TEXT,
        status TEXT DEFAULT 'pending',
        file_path TEXT,
        line_count INTEGER,
        downloaded_at TEXT,
        error TEXT
    )''')

    # Existing tables from extract/geocode
    c.execute('''CREATE TABLE IF NOT EXISTS books (
        id TEXT PRIMARY KEY,
        title TEXT NOT NULL,
        author TEXT,
        gutenberg_id TEXT
    )''')

    c.execute('''CREATE TABLE IF NOT EXISTS mentions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        book_id TEXT NOT NULL,
        location_name TEXT NOT NULL,
        chapter TEXT,
        line_number INTEGER,
        passage TEXT,
        char_offset INTEGER,
        FOREIGN KEY (book_id) REFERENCES books(id)
    )''')

    c.execute('''CREATE INDEX IF NOT EXISTS idx_mentions_book ON mentions(book_id)''')
    c.execute('''CREATE INDEX IF NOT EXISTS idx_mentions_location ON mentions(location_name)''')

    conn.commit()
    return conn


def fetch_gutenberg_catalog(conn, force=False):
    """Download and parse the Gutenberg CSV catalog."""
    c = conn.cursor()

    if not force:
        c.execute("SELECT COUNT(*) FROM book_catalog WHERE status = 'pending'")
        pending = c.fetchone()[0]
        c.execute('SELECT COUNT(*) FROM book_catalog')
        total = c.fetchone()[0]
        if pending > 0 and total > 100:
            print(f'Catalog has {total} entries ({pending} pending). Use --refresh-catalog to re-fetch.')
            return total

    print('Fetching Gutenberg catalog (this may take a moment)...')
    resp = requests.get(GUTENBERG_CATALOG_URL, timeout=120)
    resp.raise_for_status()

    reader = csv.DictReader(io.StringIO(resp.text))
    count = 0
    batch = []

    for row in reader:
        gid = row.get('Text#', '').strip()
        title = row.get('Title', '').strip()
        authors = row.get('Authors', '').strip()
        language = row.get('Language', '').strip()
        subject = row.get('Subjects', '').strip()

        if not gid or not title:
            continue

        # Only English texts
        if 'en' not in language.lower():
            continue

        batch.append((gid, title, authors, language, subject, 'pending'))
        count += 1

        if len(batch) >= 500:
            c.executemany(
                'INSERT OR IGNORE INTO book_catalog (gutenberg_id, title, author, language, subject, status) VALUES (?, ?, ?, ?, ?, ?)',
                batch
            )
            conn.commit()
            batch = []

    if batch:
        c.executemany(
            'INSERT OR IGNORE INTO book_catalog (gutenberg_id, title, author, language, subject, status) VALUES (?, ?, ?, ?, ?, ?)',
            batch
        )
        conn.commit()

    print(f'Loaded {count} English books into catalog')
    return count


def strip_gutenberg_header_footer(text):
    """Remove Project Gutenberg header and footer boilerplate."""
    start_markers = [
        '*** START OF THE PROJECT GUTENBERG EBOOK',
        '*** START OF THIS PROJECT GUTENBERG EBOOK',
        '*END*THE SMALL PRINT',
    ]
    end_markers = [
        '*** END OF THE PROJECT GUTENBERG EBOOK',
        '*** END OF THIS PROJECT GUTENBERG EBOOK',
        'End of the Project Gutenberg EBook',
        'End of Project Gutenberg',
    ]

    start_idx = 0
    for marker in start_markers:
        idx = text.upper().find(marker.upper())
        if idx != -1:
            newline = text.find('\n', idx)
            if newline != -1:
                start_idx = newline + 1
            break

    end_idx = len(text)
    for marker in end_markers:
        idx = text.upper().find(marker.upper())
        if idx != -1:
            end_idx = idx
            break

    return text[start_idx:end_idx].strip()


def download_book(gid):
    """Try multiple Gutenberg URLs to download a book. Returns text or None."""
    for url_template in GUTENBERG_TEXT_URLS:
        url = url_template.format(id=gid)
        try:
            resp = requests.get(url, timeout=30)
            if resp.status_code == 200 and len(resp.text) > 500:
                return resp.text
        except requests.RequestException:
            continue
    return None


def process_download(conn, gid, title, author):
    """Download a single book, save to disk, update catalog."""
    c = conn.cursor()
    filepath = os.path.join(BOOKS_DIR, f'{gid}.txt')

    # Already on disk?
    if os.path.exists(filepath) and os.path.getsize(filepath) > 100:
        line_count = sum(1 for _ in open(filepath, encoding='utf-8', errors='ignore'))
        c.execute(
            'UPDATE book_catalog SET status=?, file_path=?, line_count=?, downloaded_at=datetime("now") WHERE gutenberg_id=?',
            ('downloaded', f'{gid}.txt', line_count, gid)
        )
        conn.commit()
        return 'skip'

    text = download_book(gid)
    if text is None:
        c.execute('UPDATE book_catalog SET status=?, error=? WHERE gutenberg_id=?',
                  ('failed', 'no text found', gid))
        conn.commit()
        return 'fail'

    text = strip_gutenberg_header_footer(text)
    if len(text) < 500:
        c.execute('UPDATE book_catalog SET status=?, error=? WHERE gutenberg_id=?',
                  ('failed', 'text too short after stripping', gid))
        conn.commit()
        return 'fail'

    with open(filepath, 'w', encoding='utf-8') as f:
        f.write(text)

    line_count = len(text.splitlines())
    c.execute(
        'UPDATE book_catalog SET status=?, file_path=?, line_count=?, downloaded_at=datetime("now") WHERE gutenberg_id=?',
        ('downloaded', f'{gid}.txt', line_count, gid)
    )
    conn.commit()
    return 'ok'


def show_status(conn):
    c = conn.cursor()
    c.execute("SELECT status, COUNT(*) FROM book_catalog GROUP BY status ORDER BY COUNT(*) DESC")
    print('\nCatalog status:')
    for status, count in c.fetchall():
        print(f'  {status:15s} {count:,}')

    c.execute("SELECT COUNT(*) FROM book_catalog")
    total = c.fetchone()[0]
    c.execute("SELECT COUNT(*) FROM book_catalog WHERE status='downloaded'")
    downloaded = c.fetchone()[0]
    c.execute("SELECT SUM(line_count) FROM book_catalog WHERE status='downloaded'")
    lines = c.fetchone()[0] or 0

    print(f'\n  Total: {total:,} books in catalog')
    print(f'  Downloaded: {downloaded:,} books ({lines:,} lines)')

    c.execute("SELECT COUNT(*) FROM mentions")
    mentions = c.fetchone()[0]
    c.execute("SELECT COUNT(DISTINCT book_id) FROM mentions")
    extracted = c.fetchone()[0]
    print(f'  Extracted: {extracted:,} books ({mentions:,} mentions)')


def main():
    parser = argparse.ArgumentParser(description='Download Gutenberg books')
    parser.add_argument('--limit', type=int, default=0, help='Max books to download (0=all pending)')
    parser.add_argument('--ids', nargs='+', help='Specific Gutenberg IDs to download')
    parser.add_argument('--status', action='store_true', help='Show download status')
    parser.add_argument('--refresh-catalog', action='store_true', help='Re-fetch the Gutenberg catalog')
    parser.add_argument('--category', type=str, default='', help='Filter by subject keyword (e.g. "fiction")')
    args = parser.parse_args()

    os.makedirs(BOOKS_DIR, exist_ok=True)
    conn = init_db()

    if args.status:
        show_status(conn)
        conn.close()
        return

    if args.refresh_catalog:
        conn.execute("DELETE FROM book_catalog WHERE status = 'pending'")
        conn.commit()

    # Ensure catalog is populated
    fetch_gutenberg_catalog(conn, force=args.refresh_catalog)

    c = conn.cursor()

    if args.ids:
        # Download specific IDs — add to catalog if not there
        for gid in args.ids:
            c.execute('SELECT gutenberg_id FROM book_catalog WHERE gutenberg_id=?', (gid,))
            if not c.fetchone():
                c.execute(
                    'INSERT INTO book_catalog (gutenberg_id, title, author, language, subject, status) VALUES (?, ?, ?, ?, ?, ?)',
                    (gid, f'Gutenberg #{gid}', 'Unknown', 'en', '', 'pending')
                )
        conn.commit()
        c.execute('SELECT gutenberg_id, title, author FROM book_catalog WHERE gutenberg_id IN ({})'.format(
            ','.join('?' * len(args.ids))), args.ids)
    else:
        # Get pending books
        query = "SELECT gutenberg_id, title, author FROM book_catalog WHERE status='pending'"
        params = []
        if args.category:
            query += " AND LOWER(subject) LIKE ?"
            params.append(f'%{args.category.lower()}%')
        query += " ORDER BY CAST(gutenberg_id AS INTEGER)"
        if args.limit > 0:
            query += f" LIMIT {args.limit}"
        c.execute(query, params)

    to_download = c.fetchall()
    total = len(to_download)

    if total == 0:
        print('Nothing to download. All books are already downloaded or no matches.')
        show_status(conn)
        conn.close()
        return

    print(f'\nDownloading {total} books...\n')

    ok = 0
    skipped = 0
    failed = 0

    try:
        for i, (gid, title, author) in enumerate(to_download):
            short_title = (title[:50] + '...') if len(title) > 50 else title
            result = process_download(conn, gid, title, author)

            if result == 'ok':
                ok += 1
                print(f'  [{i+1}/{total}] {short_title} — downloaded')
            elif result == 'skip':
                skipped += 1
                print(f'  [{i+1}/{total}] {short_title} — already on disk')
            else:
                failed += 1
                print(f'  [{i+1}/{total}] {short_title} — FAILED')

            time.sleep(DOWNLOAD_DELAY)

    except KeyboardInterrupt:
        print(f'\n\nInterrupted! Progress saved.')

    print(f'\nResults: {ok} downloaded, {skipped} skipped, {failed} failed')
    show_status(conn)
    conn.close()


if __name__ == '__main__':
    main()
