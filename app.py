import re
import json
import time
import sqlite3
import uuid
import argparse
import os
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import List, Optional, Dict, Any
from urllib.parse import urlparse

import httpx
from dateutil import parser as dateparser
from rapidfuzz import fuzz
from playwright.sync_api import sync_playwright
import trafilatura

from reportlab.lib.pagesizes import A4
from reportlab.pdfgen import canvas
from reportlab.lib.units import cm


# UI deps are imported only when running streamlit mode
# (so CLI scans can run even if streamlit isn't installed in a minimal env)
try:
    import streamlit as st
except Exception:
    st = None

SEARXNG_URL = "http://localhost:8080/search"  # local searxng JSON enabled
DB_PATH = "data/cache.sqlite"
USER_AGENT = "Mozilla/5.0 (compatible; CompanyResearchAgent/0.1; +http://localhost)"
DEFAULT_TIMEOUT = 15

# --------------------------- storage ---------------------------

def ensure_db():
    import os
    os.makedirs("data", exist_ok=True)
    with sqlite3.connect(DB_PATH) as con:
        con.execute("""
        CREATE TABLE IF NOT EXISTS docs (
          url TEXT PRIMARY KEY,
          title TEXT,
          published_at TEXT,
          source TEXT,
          snippet TEXT,
          text TEXT,
          fetched_at TEXT
        )
        """)
        con.execute("""
        CREATE TABLE IF NOT EXISTS runs (
          run_id TEXT PRIMARY KEY,
          query TEXT,
          created_at TEXT
        )
        """)
        # run metadata for comparisons
        con.execute("""
        CREATE TABLE IF NOT EXISTS run_meta (
          run_id TEXT PRIMARY KEY,
          company_key TEXT,
          input_value TEXT,
          days INTEGER,
          created_at TEXT
        )
        """)
        # mapping run -> urls (+ bucket)
        con.execute("""
        CREATE TABLE IF NOT EXISTS run_docs (
          run_id TEXT,
          bucket TEXT,
          url TEXT,
          title TEXT,
          source TEXT,
          published_at TEXT,
          score REAL,
          PRIMARY KEY(run_id, url)
        )
        """)
        # watchlist
        con.execute("""
        CREATE TABLE IF NOT EXISTS watchlist (
          company_key TEXT PRIMARY KEY,
          label TEXT,
          input_value TEXT,
          created_at TEXT,
          last_run_id TEXT
        )
        """)

def upsert_doc(doc: Dict[str, Any]):
    with sqlite3.connect(DB_PATH) as con:
        con.execute("""
        INSERT INTO docs(url, title, published_at, source, snippet, text, fetched_at)
        VALUES(?,?,?,?,?,?,?)
        ON CONFLICT(url) DO UPDATE SET
          title=excluded.title,
          published_at=excluded.published_at,
          source=excluded.source,
          snippet=excluded.snippet,
          text=excluded.text,
          fetched_at=excluded.fetched_at
        """, (
            doc.get("url"),
            doc.get("title"),
            doc.get("published_at"),
            doc.get("source"),
            doc.get("snippet"),
            doc.get("text"),
            doc.get("fetched_at"),
        ))

def get_cached(url: str) -> Optional[Dict[str, Any]]:
    with sqlite3.connect(DB_PATH) as con:
        cur = con.execute("SELECT url,title,published_at,source,snippet,text,fetched_at FROM docs WHERE url=?", (url,))
        row = cur.fetchone()
        if not row:
            return None
        return {
            "url": row[0],
            "title": row[1],
            "published_at": row[2],
            "source": row[3],
            "snippet": row[4],
            "text": row[5],
            "fetched_at": row[6],
        }

def record_run(run_id: str, query: str):
    with sqlite3.connect(DB_PATH) as con:
        con.execute("INSERT INTO runs(run_id, query, created_at) VALUES(?,?,?)",
                    (run_id, query, datetime.now().isoformat()))

def record_run_meta(run_id: str, company_key_: str, input_value: str, days: int):
    with sqlite3.connect(DB_PATH) as con:
        con.execute("""
          INSERT INTO run_meta(run_id, company_key, input_value, days, created_at)
          VALUES(?,?,?,?,?)
        """, (run_id, company_key_, input_value, int(days), datetime.now().isoformat()))

def record_run_doc(run_id: str, bucket: str, doc: Dict[str, Any], score: float):
    with sqlite3.connect(DB_PATH) as con:
        con.execute("""
          INSERT OR REPLACE INTO run_docs(run_id, bucket, url, title, source, published_at, score)
          VALUES(?,?,?,?,?,?,?)
        """, (
            run_id,
            bucket,
            doc.get("url"),
            doc.get("title"),
            doc.get("source"),
            doc.get("published_at"),
            float(score),
        ))

def get_last_run_id(company_key_: str) -> Optional[str]:
    with sqlite3.connect(DB_PATH) as con:
        cur = con.execute("""
          SELECT run_id FROM run_meta
          WHERE company_key=?
          ORDER BY created_at DESC
          LIMIT 1
        """, (company_key_,))
        row = cur.fetchone()
    return row[0] if row else None

def get_run_docs(run_id: str) -> List[Dict[str, Any]]:
    with sqlite3.connect(DB_PATH) as con:
        cur = con.execute("""
          SELECT bucket, url, title, source, published_at, score
          FROM run_docs
          WHERE run_id=?
        """, (run_id,))
        rows = cur.fetchall()
    return [
        {"bucket": r[0], "url": r[1], "title": r[2], "source": r[3], "published_at": r[4], "score": r[5]}
        for r in rows
    ]

def add_to_watchlist(entity: Dict[str, str], original_input: str):
    key = company_key(entity)
    label = entity.get("domain") or entity["company"]
    now = datetime.now().isoformat()
    with sqlite3.connect(DB_PATH) as con:
        con.execute("""
        INSERT INTO watchlist(company_key, label, input_value, created_at, last_run_id)
        VALUES(?,?,?,?,?)
        ON CONFLICT(company_key) DO UPDATE SET
          label=excluded.label,
          input_value=excluded.input_value
        """, (key, label, original_input, now, None))

def remove_from_watchlist(company_key_: str):
    with sqlite3.connect(DB_PATH) as con:
        con.execute("DELETE FROM watchlist WHERE company_key=?", (company_key_,))

def list_watchlist() -> List[Dict[str, Any]]:
    with sqlite3.connect(DB_PATH) as con:
        cur = con.execute("""
          SELECT company_key, label, input_value, created_at, last_run_id
          FROM watchlist
          ORDER BY created_at DESC
        """)
        rows = cur.fetchall()
    return [
        {"company_key": r[0], "label": r[1], "input_value": r[2], "created_at": r[3], "last_run_id": r[4]}
        for r in rows
    ]

def update_watchlist_last_run(company_key_: str, run_id: str):
    with sqlite3.connect(DB_PATH) as con:
        con.execute("UPDATE watchlist SET last_run_id=? WHERE company_key=?", (run_id, company_key_))

# --------------------------- utils ---------------------------

def is_domain_like(s: str) -> bool:
    return bool(re.match(r"^(https?://)?([a-zA-Z0-9-]+\.)+[a-zA-Z]{2,}(/.*)?$", s.strip()))

def normalize_company_input(q: str) -> Dict[str, str]:
    q = q.strip()
    if is_domain_like(q):
        if not q.startswith("http"):
            q = "https://" + q
        u = urlparse(q)
        domain = u.netloc.lower().replace("www.", "")
        return {"company": domain, "domain": domain, "input_type": "domain"}
    return {"company": q, "domain": "", "input_type": "name"}

def company_key(entity: Dict[str, str]) -> str:
    return (entity.get("domain") or entity["company"]).strip().lower()

def parse_date_guess(text: Optional[str]) -> Optional[str]:
    if not text:
        return None
    try:
        dt = dateparser.parse(text, fuzzy=True)
        if dt:
            return dt.isoformat()
    except Exception:
        return None
    return None

def host_from_url(url: str) -> str:
    try:
        return urlparse(url).netloc.replace("www.", "")
    except Exception:
        return ""

def compute_delta(prev_docs: List[Dict[str, Any]], curr_docs: List[Dict[str, Any]]) -> Dict[str, Any]:
    prev_set = {d["url"] for d in prev_docs}
    curr_set = {d["url"] for d in curr_docs}

    new_urls = curr_set - prev_set
    gone_urls = prev_set - curr_set

    new_items = [d for d in curr_docs if d["url"] in new_urls]
    gone_items = [d for d in prev_docs if d["url"] in gone_urls]

    def group(items):
        out: Dict[str, List[Dict[str, Any]]] = {}
        for it in items:
            out.setdefault(it["bucket"], []).append(it)
        return out

    return {
        "new": group(new_items),
        "gone": group(gone_items),
        "counts": {"new": len(new_items), "gone": len(gone_items)},
    }

# --------------------------- search ---------------------------

@dataclass
class SearchItem:
    title: str
    url: str
    snippet: str
    engine: str

def searxng_search(query: str, language: str = "en", time_range: Optional[str] = None,
                  limit: int = 100, pages: int = 3) -> List[SearchItem]:
    """Query local SearXNG and return up to `limit` results, optionally paginating across `pages`."""
    headers = {"User-Agent": USER_AGENT, "Accept": "application/json"}
    all_items: List[SearchItem] = []

    with httpx.Client(timeout=DEFAULT_TIMEOUT, headers=headers) as client:
        for pageno in range(1, pages + 1):
            params = {"q": query, "format": "json", "language": language, "pageno": pageno}
            if time_range:
                params["time_range"] = time_range

            r = client.get(SEARXNG_URL, params=params)
            r.raise_for_status()
            data = r.json()

            for item in data.get("results", []):
                all_items.append(SearchItem(
                    title=item.get("title") or "",
                    url=item.get("url") or "",
                    snippet=item.get("content") or "",
                    engine=item.get("engine") or ""
                ))

    all_items = dedupe_by_url(all_items)
    return all_items[:limit]

def build_queries(entity: Dict[str, str], days: int) -> Dict[str, List[str]]:
    """Return multiple simpler queries per bucket to improve recall."""
    base = entity["company"]
    domain = entity.get("domain") or ""
    key = f'"{base}"'  # quoted for precision

    news_base = key
    if domain:
        # include domain as an additional anchor in news queries
        news_base = f'({key} OR "{domain}")'

    return {
        "news": [
            f'{news_base} (funding OR raises OR "raised" OR investment OR "Series A" OR "Series B")',
            f'{news_base} (acquisition OR acquired OR merger OR "strategic stake")',
            f'{news_base} (lawsuit OR investigation OR regulator OR "data breach" OR cybersecurity OR outage)',
            f'{news_base} (layoffs OR hiring OR expansion OR "opens" OR "launches")',
        ],
        "reviews": [
            f'{key} (Glassdoor OR Indeed OR AmbitionBox OR Kununu) reviews',
            f'"{base}" employee reviews',
        ],
        "chatter": [
            f'{key} site:reddit.com',
            f'{key} (forum OR community OR "discussion")',
        ],
        "company_site": [
            f'site:{domain} (press OR newsroom OR blog OR "press release")' if domain else f'{key} (press release OR newsroom OR blog)',
            f'site:{domain} (careers OR jobs OR "we are hiring")' if domain else f'{key} (careers OR jobs)',
        ],
        "people": [
            f'{key} (CEO OR CTO OR CFO OR "appointed" OR "joins" OR "steps down")',
            f'{key} ("board of directors" OR "executive team")',
        ],
    }

# --------------------------- fetch + extract ---------------------------

def extract_with_trafilatura(html: str) -> Dict[str, Any]:
    downloaded = trafilatura.extract(
        html,
        include_comments=False,
        include_tables=False,
        with_metadata=True,
        output_format="json"
    )
    if not downloaded:
        return {"title": "", "text": "", "published_at": None}

    j = json.loads(downloaded)
    text = (j.get("text") or "").strip()
    title = (j.get("title") or "").strip()
    published_at = parse_date_guess(j.get("date")) or parse_date_guess(j.get("publication_date"))
    return {"title": title, "text": text, "published_at": published_at}

def playwright_get_html(url: str) -> str:
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(user_agent=USER_AGENT)
        page = context.new_page()
        page.set_default_timeout(15000)

        page.goto(url, wait_until="domcontentloaded")
        try:
            page.wait_for_load_state("networkidle", timeout=8000)
        except Exception:
            pass

        html = page.content()
        context.close()
        browser.close()
        return html

def fetch_and_extract(url: str, use_playwright: bool) -> Dict[str, Any]:
    cached = get_cached(url)
    if cached and cached.get("fetched_at"):
        try:
            fetched_at = dateparser.parse(cached["fetched_at"])
            if datetime.now() - fetched_at < timedelta(hours=24):
                return cached
        except Exception:
            pass

    headers = {"User-Agent": USER_AGENT, "Accept": "text/html,application/xhtml+xml"}
    fetched_at = datetime.now().isoformat()

    title, text, published_at = "", "", None

    # 1) HTTP first
    try:
        with httpx.Client(timeout=DEFAULT_TIMEOUT, headers=headers, follow_redirects=True) as client:
            r = client.get(url)
            r.raise_for_status()
            html = r.text
        out = extract_with_trafilatura(html)
        title, text, published_at = out["title"], out["text"], out["published_at"]
    except Exception:
        pass

    # 2) Playwright fallback
    if use_playwright and ((not text) or (len(text) < 400)):
        try:
            html2 = playwright_get_html(url)
            out2 = extract_with_trafilatura(html2)
            if len(out2["text"] or "") > len(text or ""):
                title, text, published_at = out2["title"], out2["text"], out2["published_at"]
        except Exception:
            pass

    doc = {
        "url": url,
        "title": title,
        "published_at": published_at,
        "source": host_from_url(url),
        "snippet": "",
        "text": text or "",
        "fetched_at": fetched_at,
    }
    upsert_doc(doc)
    return doc

# --------------------------- ranking ---------------------------

def score_item(entity: Dict[str, str], item: SearchItem) -> float:
    company = entity["company"].lower()
    domain = (entity.get("domain") or "").lower()

    MAJOR_SOURCES = {"reuters.com","bloomberg.com","ft.com","wsj.com","economictimes.indiatimes.com",
                 "techcrunch.com","theverge.com","business-standard.com","livemint.com"}

    t = (item.title or "").lower()
    u = (item.url or "").lower()
    s = (item.snippet or "").lower()

    score = 0.0
    score += 2.0 if company in t else 0.0
    score += 1.5 if company in s else 0.0
    score += 2.0 if (domain and domain in u) else 0.0
    score += (fuzz.partial_ratio(company, t) / 100.0)

    host = host_from_url(item.url)
    if host.count(".") <= 1:
        score += 0.2
    if any(host.endswith(d) for d in MAJOR_SOURCES):
        score += 1.5
    return score

def dedupe_by_url(items: List[SearchItem]) -> List[SearchItem]:
    seen = set()
    out = []
    for it in items:
        if it.url and it.url not in seen:
            seen.add(it.url)
            out.append(it)
    return out

# --------------------------- reporting ---------------------------

def fmt_pub(pub: Optional[str]) -> str:
    if not pub:
        return ""
    try:
        return dateparser.parse(pub).strftime("%Y-%m-%d")
    except Exception:
        return pub

def brief_markdown(entity: Dict[str, str], buckets: Dict[str, List[Dict[str, Any]]], days: int, run_id: str, delta: Optional[Dict[str, Any]] = None) -> str:
    title = entity["company"]
    lines = []
    lines.append(f"# Company brief: {title}")
    lines.append("")
    lines.append(f"_Collected on {datetime.now().strftime('%Y-%m-%d %H:%M')} (local). Window: last ~{days} days where possible._")
    lines.append(f"_Run ID: {run_id}_")
    lines.append("")

    if delta:
        lines.append("## Changes since last scan")
        lines.append(f"- New items: **{delta['counts']['new']}**")
        lines.append(f"- Disappeared items: **{delta['counts']['gone']}**")
        lines.append("")

        if delta["counts"]["new"] > 0:
            lines.append("### New items")
            for bucket, items in delta["new"].items():
                lines.append(f"**{bucket.replace('_',' ').title()}**")
                for it in items:
                    lines.append(f"- {it.get('title') or it['url']} ({it.get('source','')})")
                    lines.append(f"  - {it['url']}")
            lines.append("")

    for k, docs in buckets.items():
        lines.append(f"## {k.replace('_', ' ').title()}")
        if not docs:
            lines.append("- (No results)")
            lines.append("")
            continue

        for d in docs:
            pub_str = fmt_pub(d.get("published_at"))
            lines.append(f"- **{d.get('title') or '(untitled)'}**{(' — ' + pub_str) if pub_str else ''}")
            lines.append(f"  - Source: {d.get('source')}")
            lines.append(f"  - URL: {d.get('url')}")
        lines.append("")
    return "\n".join(lines)

def ensure_dir(path: str):
    import os
    os.makedirs(path, exist_ok=True)

def safe_filename(s: str) -> str:
    s = s.strip().lower()
    s = re.sub(r"[^a-z0-9._-]+", "-", s)
    return s[:80] if len(s) > 80 else s

# --------------------------- core scan (shared by UI + CLI) ---------------------------

def run_scan(company_input: str, days: int, per_bucket: int, use_playwright: bool, deep_scan: bool = False) -> Dict[str, Any]:
    ensure_db()
    entity = normalize_company_input(company_input)
    ckey = company_key(entity)

    prev_run_id = get_last_run_id(ckey)

    run_id = str(uuid.uuid4())
    record_run(run_id, query=company_input)
    record_run_meta(run_id, ckey, company_input, days)

    queries = build_queries(entity, days)

    # Deep scan pulls more candidates and pages (slower, better recall)
    pages = 5 if deep_scan else 3
    limit = 200 if deep_scan else 100
    # Keep per_bucket as provided (UI can increase it for deep scan too)

    buckets_raw: Dict[str, List[SearchItem]] = {}
    for bucket, query_list in queries.items():
        merged: List[SearchItem] = []
        for query in query_list:
            time_range = "week" if days <= 10 else ("month" if days <= 45 else ("year" if days > 180 else None))
            merged.extend(searxng_search(query, time_range=time_range, limit=limit, pages=pages))

        merged = dedupe_by_url(merged)
        merged.sort(key=lambda it: score_item(entity, it), reverse=True)
        # Only now cut down to the top per_bucket to fetch content
        buckets_raw[bucket] = merged[:per_bucket]

    buckets_docs: Dict[str, List[Dict[str, Any]]] = {}
    for bucket, items in buckets_raw.items():
        docs = []
        for it in items:
            if not it.url:
                continue
            d = fetch_and_extract(it.url, use_playwright)
            d["snippet"] = it.snippet
            d["title"] = d["title"] or it.title
            docs.append(d)
            record_run_doc(run_id, bucket, d, score_item(entity, it))
            time.sleep(0.2)
        buckets_docs[bucket] = docs

    # delta
    delta = None
    if prev_run_id:
        prev_docs = get_run_docs(prev_run_id)
        curr_docs = get_run_docs(run_id)
        delta = compute_delta(prev_docs, curr_docs)

    # If on watchlist, update last_run_id
    with sqlite3.connect(DB_PATH) as con:
        cur = con.execute("SELECT 1 FROM watchlist WHERE company_key=?", (ckey,))
        if cur.fetchone():
            update_watchlist_last_run(ckey, run_id)

    return {
        "entity": entity,
        "company_key": ckey,
        "run_id": run_id,
        "prev_run_id": prev_run_id,
        "delta": delta,
        "buckets_docs": buckets_docs,
    }

# --------------------------- CLI (Step 7 scheduling) ---------------------------

def cli_scan(args) -> int:
    ensure_dir(args.out_dir)
    result = run_scan(args.scan, args.days, args.per_bucket, args.use_playwright, deep_scan=args.deep_scan)
    entity = result["entity"]
    md = brief_markdown(entity, result["buckets_docs"], args.days, result["run_id"], result["delta"])

    fn = f"{datetime.now().strftime('%Y%m%d_%H%M')}_{safe_filename(company_key(entity))}.md"
    path = f"{args.out_dir}/{fn}"
    with open(path, "w", encoding="utf-8") as f:
        f.write(md)

    print(f"[OK] Saved report: {path}")

    # Also write a PDF next to the markdown (business-friendly)
    pdf_fn = fn.replace(".md", ".pdf")
    pdf_path = f"{args.out_dir}/{pdf_fn}"
    pdf_lines = brief_to_pdf_lines(md)
    write_pdf_report(pdf_path, f"Company brief: {entity['company']}", pdf_lines)
    print(f"[OK] Saved PDF: {pdf_path}")
    if result["delta"]:
        print(f"     New: {result['delta']['counts']['new']} | Gone: {result['delta']['counts']['gone']}")
    return 0

def cli_watchlist_scan(args) -> int:
    ensure_db()
    ensure_dir(args.out_dir)
    wl = list_watchlist()
    if not wl:
        print("[INFO] Watchlist empty. Add companies in the UI first.")
        return 0

    digest_lines = []
    digest_lines.append(f"# Daily digest ({datetime.now().strftime('%Y-%m-%d %H:%M')})")
    digest_lines.append("")

    for w in wl:
        company_input = w["input_value"]
        try:
            result = run_scan(company_input, args.days, args.per_bucket, args.use_playwright, deep_scan=args.deep_scan)
            entity = result["entity"]
            ckey = result["company_key"]
            delta = result["delta"]

            md = brief_markdown(entity, result["buckets_docs"], args.days, result["run_id"], delta)
            fn = f"{datetime.now().strftime('%Y%m%d_%H%M')}_{safe_filename(ckey)}.md"
            path = f"{args.out_dir}/{fn}"
            with open(path, "w", encoding="utf-8") as f:
                f.write(md)

            new_count = delta["counts"]["new"] if delta else 0
            gone_count = delta["counts"]["gone"] if delta else 0
            digest_lines.append(f"## {w['label']}")
            digest_lines.append(f"- Report: {path}")
            digest_lines.append(f"- New: **{new_count}** | Gone: **{gone_count}**")
            digest_lines.append("")

            update_watchlist_last_run(ckey, result["run_id"])
            print(f"[OK] {w['label']} -> {path} (new={new_count}, gone={gone_count})")
        except Exception as e:
            print(f"[ERR] {w['label']}: {e}")
            digest_lines.append(f"## {w['label']}")
            digest_lines.append(f"- Error: {e}")
            digest_lines.append("")

    digest_fn = f"{datetime.now().strftime('%Y%m%d_%H%M')}_digest.md"
    digest_path = f"{args.out_dir}/{digest_fn}"
    with open(digest_path, "w", encoding="utf-8") as f:
        f.write("\n".join(digest_lines))

    print(f"[OK] Saved digest: {digest_path}")


    digest_pdf_path = f"{args.out_dir}/{datetime.now().strftime('%Y%m%d_%H%M')}_digest.pdf"
    write_pdf_report(digest_pdf_path, "Daily Company Signals Digest", digest_lines)
    print(f"[OK] Saved digest PDF: {digest_pdf_path}")
    return 0

def get_latest_run_for_company(company_key_: str) -> Optional[str]:
    with sqlite3.connect(DB_PATH) as con:
        cur = con.execute("""
          SELECT run_id FROM run_meta
          WHERE company_key=?
          ORDER BY created_at DESC
          LIMIT 1
        """, (company_key_,))
        row = cur.fetchone()
    return row[0] if row else None

def get_previous_run_for_company(company_key_: str, current_run_id: str) -> Optional[str]:
    with sqlite3.connect(DB_PATH) as con:
        cur = con.execute("""
          SELECT run_id FROM run_meta
          WHERE company_key=? AND run_id <> ?
          ORDER BY created_at DESC
          LIMIT 1
        """, (company_key_, current_run_id))
        row = cur.fetchone()
    return row[0] if row else None

def build_email_digest_for_watchlist(days_label: str = "last scan") -> str:
    ensure_db()
    wl = list_watchlist()
    now = datetime.now().strftime("%Y-%m-%d %H:%M")

    lines = []
    lines.append(f"Subject: Daily Company Signals Digest ({now})")
    lines.append("")
    lines.append(f"Hi team,")
    lines.append("")
    lines.append(f"Here are the key updates from the {days_label}:")
    lines.append("")

    if not wl:
        lines.append("- Watchlist is empty.")
        return "\n".join(lines)

    for w in wl:
        ckey = w["company_key"]
        label = w["label"]

        latest = get_latest_run_for_company(ckey)
        if not latest:
            lines.append(f"{label}: No scans yet.")
            lines.append("")
            continue

        prev = get_previous_run_for_company(ckey, latest)
        if prev:
            delta = compute_delta(get_run_docs(prev), get_run_docs(latest))
        else:
            delta = None

        bullets = summarize_delta_exec(delta, max_items=4)
        lines.append(f"{label}")
        for b in bullets:
            lines.append(f"- {b}")
        lines.append("")

    lines.append("Regards,")
    lines.append("Local Company Research Agent")
    return "\n".join(lines)

def summarize_delta_exec(delta: Optional[Dict[str, Any]], max_items: int = 5) -> List[str]:
    """
    Returns short bullets like:
    - News: 2 new items (example title…)
    """
    if not delta:
        return ["No previous scan to compare yet."]

    bullets = []
    # flatten new items with bucket
    flattened = []
    for bucket, items in (delta.get("new") or {}).items():
        for it in items:
            flattened.append((bucket, it))

    # prioritize buckets (business-friendly ordering)
    bucket_order = {"news": 0, "people": 1, "reviews": 2, "chatter": 3, "company_site": 4}
    flattened.sort(key=lambda x: bucket_order.get(x[0], 99))

    # count per bucket
    counts = {b: len(items) for b, items in (delta.get("new") or {}).items()}

    # summary header bullets
    total_new = delta.get("counts", {}).get("new", 0)
    total_gone = delta.get("counts", {}).get("gone", 0)
    bullets.append(f"New items found: {total_new}. Disappeared since last scan: {total_gone}.")

    # per-bucket highlights + sample titles
    for bucket in sorted(counts.keys(), key=lambda b: bucket_order.get(b, 99)):
        n = counts[bucket]
        if n <= 0:
            continue
        sample_titles = []
        for (bb, it) in flattened:
            if bb != bucket:
                continue
            t = it.get("title") or it.get("url")
            if t:
                sample_titles.append(t.strip())
            if len(sample_titles) >= 2:
                break
        sample = " | ".join(sample_titles)
        bullets.append(f"{bucket.replace('_',' ').title()}: {n} new — {sample}")

        if len(bullets) >= max_items:
            break

    return bullets[:max_items]


def write_pdf_report(pdf_path: str, title: str, lines: List[str]):
    """
    Very simple PDF writer using ReportLab.
    """
    c = canvas.Canvas(pdf_path, pagesize=A4)
    width, height = A4

    x = 2 * cm
    y = height - 2 * cm
    line_height = 14

    # Title
    c.setFont("Helvetica-Bold", 14)
    c.drawString(x, y, title)
    y -= 1.2 * cm

    c.setFont("Helvetica", 10)
    for raw in lines:
        # wrap long lines crudely
        text = raw.replace("\t", " ")
        while len(text) > 120:
            c.drawString(x, y, text[:120])
            text = text[120:]
            y -= line_height
            if y < 2 * cm:
                c.showPage()
                c.setFont("Helvetica", 10)
                y = height - 2 * cm
        c.drawString(x, y, text)
        y -= line_height

        if y < 2 * cm:
            c.showPage()
            c.setFont("Helvetica", 10)
            y = height - 2 * cm

    c.save()


def brief_to_pdf_lines(markdown_text: str) -> List[str]:
    """
    Convert markdown to plain-ish lines for PDF.
    """
    lines = []
    for line in markdown_text.splitlines():
        line = line.strip()
        if line.startswith("#"):
            line = line.lstrip("#").strip().upper()
        # remove markdown bullets formatting a bit
        line = line.replace("**", "")
        lines.append(line)
    return lines


# --------------------------- Streamlit UI (Step 6) ---------------------------

def ui_main():
    if st is None:
        raise RuntimeError("streamlit is not installed in this environment. Install it or run CLI mode.")

    st.set_page_config(page_title="Local Company Research Agent", layout="wide")
    ensure_db()

    st.title("Local Company Research Agent")
    st.caption("Local-first research: SearXNG search + extraction + watchlist + deltas + scheduled CLI scans.")

    if st.session_state.get("digest_text"):
        st.subheader("Email-style digest")
        digest_text = st.session_state["digest_text"]
        st.code(digest_text, language="text")

        # Export digest as markdown + PDF too
        ensure_dir("reports")
        digest_md_path = f"reports/{datetime.now().strftime('%Y%m%d_%H%M')}_digest.txt"
        with open(digest_md_path, "w", encoding="utf-8") as f:
            f.write(digest_text)

        digest_pdf_path = f"reports/{datetime.now().strftime('%Y%m%d_%H%M')}_digest.pdf"
        write_pdf_report(digest_pdf_path, "Daily Company Signals Digest", digest_text.splitlines())

        with open(digest_pdf_path, "rb") as f:
            st.download_button("Download digest (PDF)", f, file_name=digest_pdf_path.split("/")[-1])


    # Sidebar Watchlist
    st.sidebar.header("Watchlist")
    wl = list_watchlist()

    st.sidebar.markdown("---")
    st.sidebar.subheader("Digest view (email-style)")
    if st.sidebar.button("Generate digest text"):
        st.session_state["digest_text"] = build_email_digest_for_watchlist("most recent scan")
        st.rerun()


    labels = ["(none)"] + [w["label"] for w in wl]
    selected = st.sidebar.selectbox("Pick a company", labels)

    if selected != "(none)":
        chosen = next(w for w in wl if w["label"] == selected)
        if st.sidebar.button("Load into search"):
            st.session_state["q_prefill"] = chosen["input_value"]
        if st.sidebar.button("Remove from watchlist"):
            remove_from_watchlist(chosen["company_key"])
            st.sidebar.success("Removed.")
            st.rerun()

    st.sidebar.markdown("---")
    st.sidebar.subheader("Scheduling (Step 7)")
    st.sidebar.write("Use CLI commands with Windows Task Scheduler:")
    st.sidebar.code(
        'python app.py --watchlist-scan --days 30 --out-dir reports --deep-scan\n'
        'python app.py --scan "acme.com" --days 30 --out-dir reports --deep-scan',
        language="text"
    )

    prefill = st.session_state.get("q_prefill", "")
    q = st.text_input("Company name or website", value=prefill, placeholder="e.g., acme.com or Acme Corporation")

    col1, col2, col3 = st.columns([1, 1, 1])
    with col1:
        days = st.slider("Recency window (days)", 7, 180, 30, step=1)
    with col2:
        per_bucket = st.slider("Max items per section", 3, 20, 8, step=1)
    with col3:
        use_playwright = st.checkbox("Use Playwright fallback", value=True)
    deep_scan = st.checkbox("Deep scan (slower, better coverage)", value=False)

    cA, cB = st.columns([1, 1])
    with cA:
        if st.button("Add to watchlist", disabled=not bool(q.strip())):
            ent = normalize_company_input(q)
            add_to_watchlist(ent, q)
            st.success("Added to watchlist.")
            st.rerun()

    with cB:
        st.write("")

    if st.button("Research", type="primary", disabled=not bool(q.strip())):

        with st.status("Running scan…", expanded=False) as status:
            result = run_scan(q, days, per_bucket, use_playwright, deep_scan=deep_scan)
            status.update(label="Done", state="complete")

        entity = result["entity"]
        buckets_docs = result["buckets_docs"]
        run_id = result["run_id"]
        prev_run_id = result["prev_run_id"]
        delta = result["delta"]

        st.subheader("Executive summary (Top 5)")
        for b in summarize_delta_exec(delta, max_items=5):
            st.write("• " + b)

        st.subheader("What changed since last scan?")
        if prev_run_id and delta:
            st.write(f"New items: **{delta['counts']['new']}** | Disappeared: **{delta['counts']['gone']}**")

            with st.expander("New items (by section)", expanded=True):
                if delta["counts"]["new"] == 0:
                    st.write("No new items.")
                else:
                    for bucket, items in delta["new"].items():
                        st.markdown(f"**{bucket.replace('_',' ').title()}**")
                        for it in items:
                            st.markdown(f"- [{it.get('title') or it['url']}]({it['url']}) — {it.get('source','')}")
            with st.expander("Disappeared items (by section)", expanded=False):
                if delta["counts"]["gone"] == 0:
                    st.write("No disappeared items.")
                else:
                    for bucket, items in delta["gone"].items():
                        st.markdown(f"**{bucket.replace('_',' ').title()}**")
                        for it in items:
                            st.markdown(f"- [{it.get('title') or it['url']}]({it['url']}) — {it.get('source','')}")
        else:
            st.write("No previous scan found for this company yet. Run it once more to see deltas.")

        left, right = st.columns([1, 1])
        with left:
            st.subheader("Results")
            st.write(f"**Entity:** {entity['company']}")
            if entity.get("domain"):
                st.write(f"**Domain:** {entity['domain']}")
            st.caption(f"Run ID: {run_id}")

            for bucket, docs in buckets_docs.items():
                st.markdown(f"### {bucket.replace('_',' ').title()}")
                if not docs:
                    st.write("No results.")
                    continue
                for d in docs:
                    pub_disp = fmt_pub(d.get("published_at"))
                    st.markdown(f"**[{d.get('title') or '(untitled)'}]({d.get('url')})**")
                    meta = f"{d.get('source')}"
                    if pub_disp:
                        meta += f" · {pub_disp}"
                    st.caption(meta)
                    if d.get("snippet"):
                        st.write(d["snippet"])
                    st.divider()

        with right:
            st.subheader("Export")
            md = brief_markdown(entity, buckets_docs, days, run_id, delta)
            st.download_button("Download brief (Markdown)", md, file_name=f"{safe_filename(company_key(entity))}_brief.md")

            ensure_dir("reports")
            pdf_name = f"{safe_filename(company_key(entity))}_brief.pdf"
            pdf_path = f"reports/{pdf_name}"

            pdf_lines = brief_to_pdf_lines(md)
            write_pdf_report(pdf_path, f"Company brief: {entity['company']}", pdf_lines)

            with open(pdf_path, "rb") as f:
                st.download_button("Download brief (PDF)", f, file_name=pdf_name)

            st.subheader("Raw extracted text (debug)")
            bucket_sel = st.selectbox("Section", list(buckets_docs.keys()))
            if buckets_docs.get(bucket_sel):
                doc_sel = st.selectbox("Document", [d.get("title") or d["url"] for d in buckets_docs[bucket_sel]])
                chosen = None
                for d in buckets_docs[bucket_sel]:
                    if (d.get("title") or d["url"]) == doc_sel:
                        chosen = d
                        break
                if chosen:
                    st.code((chosen.get("text") or "")[:12000])
            else:
                st.write("No docs in this section.")

    st.info("Tip: For scheduling, use CLI mode + Windows Task Scheduler to generate daily digest markdown files.")

# --------------------------- entrypoint ---------------------------

def parse_args():
    p = argparse.ArgumentParser(description="Local Company Research Agent (UI + CLI scheduler)")
    p.add_argument("--scan", type=str, help="Run a single scan for the given company name/domain input.")
    p.add_argument("--watchlist-scan", action="store_true", help="Scan all watchlist entries and write a digest.")
    p.add_argument("--days", type=int, default=30, help="Recency window in days.")
    p.add_argument("--per-bucket", type=int, default=8, help="Max items per section.")
    p.add_argument("--use-playwright", action="store_true", default=False, help="Enable Playwright fallback.")
    p.add_argument("--no-playwright", action="store_true", help="Disable Playwright fallback.")
    p.add_argument("--out-dir", type=str, default="reports", help="Output directory for markdown reports.")
    p.add_argument("--deep-scan", action="store_true", help="Use more pages/candidates (slower, better recall).")
    return p.parse_args()

def main():
    args = parse_args()
    # normalize playwright flags
    args.use_playwright = args.use_playwright or (not args.no_playwright)

    if args.scan:
        return cli_scan(args)
    if args.watchlist_scan:
        return cli_watchlist_scan(args)

    # Default: UI mode (streamlit)
    # When launched via `streamlit run app.py`, this main() is executed in that context.
    ui_main()
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
