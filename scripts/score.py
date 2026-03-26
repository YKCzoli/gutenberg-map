"""
Score each mention's relevance: is this location a story setting or just referenced?

Combines:
  1. Frequency within the book (more mentions = more likely a setting)
  2. Chapter spread (appears across chapters = setting)
  3. Context keywords (verbs of presence/movement vs. passing reference)

Writes a relevance score (0.0–1.0) and a label ("setting" / "mention") to the DB.

Usage:
    python scripts/score.py
"""

import os
import re
import sqlite3

DATA_DIR = os.path.join(os.path.dirname(__file__), '..', 'data')
DB_FILE = os.path.join(DATA_DIR, 'gutenberg_map.db')

# ── Context keyword patterns ─────────────────────────

# Strong indicators that the narrative is happening AT this location
SETTING_PATTERNS = [
    r'\barrived?\s+(at|in)\b',
    r'\bcame\s+to\b',
    r'\breached\b',
    r'\btravell?ed\s+to\b',
    r'\bjourney(ed)?\s+to\b',
    r'\bwent\s+to\b',
    r'\bset\s+(out|off)\s+(for|to)\b',
    r'\bleft\s+(for|from)\b',
    r'\breturned?\s+to\b',
    r'\bback\s+(to|in|at)\b',
    r'\b(was|were|stood|sat|lived|stayed|remained)\s+(in|at|on|near)\b',
    r'\bstreets?\s+of\b',
    r'\brooms?\s+(at|in|of)\b',
    r'\bhouse\s+(in|at|on|of)\b',
    r'\bhotel\s+(in|at)\b',
    r'\bstation\s+(at|in|of)\b',
    r'\bport\s+of\b',
    r'\bharbou?r\s+(of|at|in)\b',
    r'\bthrough\s+the\s+streets\b',
    r'\bcrossed\s+(into|to|the)\b',
    r'\bsailed\s+(to|from|into)\b',
    r'\bthe\s+road\s+to\b',
    r'\bhere\s+in\b',
    r'\bthis\s+(city|town|place|country|village)\b',
]

# Indicators of passing reference, not narrative presence
MENTION_PATTERNS = [
    r'\bheard\s+of\b',
    r'\bspoke\s+of\b',
    r'\bknew\s+of\b',
    r'\bthought\s+of\b',
    r'\bdreams?\s+of\b',
    r'\bnews\s+from\b',
    r'\bletter\s+from\b',
    r'\bstory\s+(of|from|about)\b',
    r'\btale\s+of\b',
    r'\blike\s+(a|the|those\s+in)\b',
    r'\bas\s+(in|they\s+do\s+in)\b',
    r'\breminds?\s+(me|one|him|her|us)\s+of\b',
    r'\bspeaking\s+of\b',
    r'\bmentioned?\b',
    r'\bso[-\s]called\b',
    r'\bthe\s+word\b',
    r'\bnamed\s+after\b',
    r'\bfamous\s+(in|for)\b',
]

SETTING_RE = [re.compile(p, re.IGNORECASE) for p in SETTING_PATTERNS]
MENTION_RE = [re.compile(p, re.IGNORECASE) for p in MENTION_PATTERNS]


def score_context(passage):
    """Score a passage based on context keywords. Returns -1.0 to 1.0."""
    if not passage:
        return 0.0

    setting_hits = sum(1 for pat in SETTING_RE if pat.search(passage))
    mention_hits = sum(1 for pat in MENTION_RE if pat.search(passage))

    if setting_hits + mention_hits == 0:
        return 0.0

    # Net score: positive = setting, negative = mention
    return (setting_hits - mention_hits) / (setting_hits + mention_hits)


def compute_scores(conn):
    """Compute relevance scores for all mentions."""
    c = conn.cursor()

    # Add columns if needed
    for col_def in ['relevance REAL', 'mention_type TEXT']:
        try:
            c.execute(f'ALTER TABLE mentions ADD COLUMN {col_def}')
        except sqlite3.OperationalError:
            pass
    conn.commit()

    # ── Step 1: Compute per-book frequency and chapter spread ──

    # Frequency: how many times this location appears in this book
    c.execute('''
        SELECT book_id, location_name, COUNT(*) as freq
        FROM mentions
        GROUP BY book_id, location_name
    ''')
    freq_map = {}
    for book_id, loc, freq in c.fetchall():
        freq_map[(book_id, loc)] = freq

    # Max frequency per book (for normalization)
    c.execute('''
        SELECT book_id, MAX(cnt) FROM (
            SELECT book_id, location_name, COUNT(*) as cnt
            FROM mentions GROUP BY book_id, location_name
        ) GROUP BY book_id
    ''')
    max_freq = {row[0]: row[1] for row in c.fetchall()}

    # Chapter spread: how many distinct chapters this location appears in
    c.execute('''
        SELECT book_id, location_name, COUNT(DISTINCT chapter) as chapters
        FROM mentions
        WHERE chapter IS NOT NULL
        GROUP BY book_id, location_name
    ''')
    chapter_spread = {}
    for book_id, loc, chapters in c.fetchall():
        chapter_spread[(book_id, loc)] = chapters

    # Total chapters per book
    c.execute('''
        SELECT book_id, COUNT(DISTINCT chapter)
        FROM mentions
        WHERE chapter IS NOT NULL
        GROUP BY book_id
    ''')
    total_chapters = {row[0]: row[1] for row in c.fetchall()}

    # ── Step 2: Score each mention ──

    c.execute('SELECT id, book_id, location_name, passage FROM mentions')
    rows = c.fetchall()

    updates = []
    for mention_id, book_id, loc_name, passage in rows:
        # Frequency score (0–1): log-scaled relative to max in this book
        freq = freq_map.get((book_id, loc_name), 1)
        mf = max_freq.get(book_id, 1)
        freq_score = min(1.0, freq / max(mf * 0.3, 1))

        # Chapter spread score (0–1)
        spread = chapter_spread.get((book_id, loc_name), 1)
        total = total_chapters.get(book_id, 1)
        spread_score = min(1.0, spread / max(total * 0.3, 1))

        # Context score (-1 to 1, remapped to 0–1)
        ctx_score = (score_context(passage) + 1) / 2

        # Weighted combination
        relevance = (
            0.40 * freq_score +
            0.30 * spread_score +
            0.30 * ctx_score
        )
        relevance = round(min(1.0, max(0.0, relevance)), 3)

        # Label
        if relevance >= 0.5:
            mention_type = 'setting'
        else:
            mention_type = 'mention'

        updates.append((relevance, mention_type, mention_id))

    c.executemany('UPDATE mentions SET relevance = ?, mention_type = ? WHERE id = ?', updates)
    conn.commit()

    return len(updates)


def print_summary(conn):
    c = conn.cursor()

    c.execute('SELECT mention_type, COUNT(*) FROM mentions GROUP BY mention_type')
    counts = dict(c.fetchall())
    print(f'\nResults:')
    print(f'  Settings: {counts.get("setting", 0)}')
    print(f'  Mentions: {counts.get("mention", 0)}')

    print(f'\nTop settings (highest relevance):')
    c.execute('''
        SELECT b.title, m.location_name, m.relevance, COUNT(*) as freq
        FROM mentions m JOIN books b ON m.book_id = b.id
        WHERE m.mention_type = 'setting'
        GROUP BY m.book_id, m.location_name
        ORDER BY m.relevance DESC, freq DESC
        LIMIT 15
    ''')
    for title, loc, rel, freq in c.fetchall():
        print(f'  {rel:.2f}  {loc:30s}  ({freq}x in {title})')

    print(f'\nTop mentions (lowest relevance):')
    c.execute('''
        SELECT b.title, m.location_name, m.relevance
        FROM mentions m JOIN books b ON m.book_id = b.id
        WHERE m.mention_type = 'mention'
        GROUP BY m.book_id, m.location_name
        ORDER BY m.relevance ASC
        LIMIT 10
    ''')
    for title, loc, rel in c.fetchall():
        print(f'  {rel:.2f}  {loc:30s}  ({title})')


def main():
    if not os.path.exists(DB_FILE):
        print('Database not found. Run extract.py first.')
        return

    conn = sqlite3.connect(DB_FILE)
    count = compute_scores(conn)
    print(f'Scored {count} mentions')
    print_summary(conn)
    conn.close()


if __name__ == '__main__':
    main()
