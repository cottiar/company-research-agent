import re
import json
import time
import sqlite3
import uuid
import argparse
import os
import asyncio
import aiohttp
import sys

# --- CRITICAL FIX FOR WINDOWS ---
# Playwright requires ProactorEventLoop on Windows to run subprocesses (browsers).
# Streamlit/Python might default to SelectorEventLoop, which causes the NotImplementedError.
if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
# --------------------------------

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import List, Optional, Dict, Any
from urllib.parse import urlparse

# Use async playwright
from playwright.async_api import async_playwright
import trafilatura

# PDF Generation - Improved with Platypus for text wrapping
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, PageBreak
from reportlab.lib.units import cm
from reportlab.lib import colors

# UI deps
try:
    import streamlit as st
    import nest_asyncio
    # Apply nest_asyncio to allow asyncio to run inside Streamlit
    nest_asyncio.apply()
except Exception:
    st = None

# separate block for asyncio compatibility
if st is not None:
    try:
        import nest_asyncio
        nest_asyncio.apply()
    except ImportError:
        # Stop the app with a clear error if nest_asyncio is missing
        st.error("Missing dependency: Please run 'pip install nest_asyncio'")
        st.stop()

# --- Configuration via Env Vars ---
SEARXNG_URL = os.getenv("SEARXNG_URL", "http://localhost:8080/search")
# Default to a local LLM (e.g., Ollama) or set to https://api.openai.com/v1
LLM_BASE_URL = os.getenv("LLM_BASE_URL", "http://localhost:11434/v1") 
LLM_API_KEY = os.getenv("LLM_API_KEY", "ollama") # Use 'sk-...' for OpenAI
LLM_MODEL = os.getenv("LLM_MODEL", "llama3") # or 'gpt-4o', etc.

DB_PATH = "data/cache.sqlite"
USER_AGENT = "Mozilla/5.0 (compatible; CompanyResearchAgent/0.2; +http://localhost)"
DEFAULT_TIMEOUT = 15
MAX_CONCURRENT_BROWSERS = 3  # Limit playwright tabs to avoid resource exhaustion

# --------------------------- Storage (SQLite) ---------------------------

def ensure_db():
    os.makedirs("data", exist_ok=True)
    with sqlite3.connect(DB_PATH) as con:
        # Enable WAL mode for better concurrency
        con.execute("PRAGMA journal_mode=WAL;")
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
          created_at TEXT,
          llm_summary TEXT
        )
        """)
        # Schema update for existing DBs to add llm_summary
        try:
            con.execute("ALTER TABLE runs ADD COLUMN llm_summary TEXT")
        except Exception:
            pass

        con.execute("""
        CREATE TABLE IF NOT EXISTS run_meta (
          run_id TEXT PRIMARY KEY,
          company_key TEXT,
          input_value TEXT,
          days INTEGER,
          created_at TEXT
        )
        """)
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
        con.execute("""
        CREATE TABLE IF NOT EXISTS watchlist (
          company_key TEXT PRIMARY KEY,
          label TEXT,
          input_value TEXT,
          created_at TEXT,
          last_run_id TEXT
        )
        """)

# Synchronous DB helpers (SQLite handles simple concurrency well enough for this scale)
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
            "url": row[0], "title": row[1], "published_at": row[2],
            "source": row[3], "snippet": row[4], "text": row[5], "fetched_at": row[6],
        }

def record_run(run_id: str, query: str, summary: str = ""):
    with sqlite3.connect(DB_PATH) as con:
        con.execute("INSERT INTO runs(run_id, query, created_at, llm_summary) VALUES(?,?,?,?)",
                    (run_id, query, datetime.now().isoformat(), summary))

def record_run_meta(run_id: str, company_key_: str, input_value: str, days: int):
    with sqlite3.connect(DB_PATH) as con:
        con.execute("INSERT INTO run_meta(run_id, company_key, input_value, days, created_at) VALUES(?,?,?,?,?)",
                    (run_id, company_key_, input_value, int(days), datetime.now().isoformat()))

def record_run_doc(run_id: str, bucket: str, doc: Dict[str, Any], score: float):
    with sqlite3.connect(DB_PATH) as con:
        con.execute("""
          INSERT OR REPLACE INTO run_docs(run_id, bucket, url, title, source, published_at, score)
          VALUES(?,?,?,?,?,?,?)
        """, (run_id, bucket, doc.get("url"), doc.get("title"), doc.get("source"), doc.get("published_at"), float(score)))

def get_last_run_id(company_key_: str) -> Optional[str]:
    with sqlite3.connect(DB_PATH) as con:
        cur = con.execute("SELECT run_id FROM run_meta WHERE company_key=? ORDER BY created_at DESC LIMIT 1", (company_key_,))
        row = cur.fetchone()
    return row[0] if row else None

def get_run_data(run_id: str):
    with sqlite3.connect(DB_PATH) as con:
        # Get docs
        cur = con.execute("SELECT bucket, url, title, source, published_at, score FROM run_docs WHERE run_id=?", (run_id,))
        docs = [{"bucket": r[0], "url": r[1], "title": r[2], "source": r[3], "published_at": r[4], "score": r[5]} for r in cur.fetchall()]
        
        # Get summary
        cur = con.execute("SELECT llm_summary FROM runs WHERE run_id=?", (run_id,))
        row = cur.fetchone()
        summary = row[0] if row else ""
        
    return docs, summary

def add_to_watchlist(entity: Dict[str, str], original_input: str):
    key = company_key(entity)
    label = entity.get("domain") or entity["company"]
    now = datetime.now().isoformat()
    with sqlite3.connect(DB_PATH) as con:
        con.execute("""
        INSERT INTO watchlist(company_key, label, input_value, created_at, last_run_id)
        VALUES(?,?,?,?,?)
        ON CONFLICT(company_key) DO UPDATE SET label=excluded.label, input_value=excluded.input_value
        """, (key, label, original_input, now, None))

def remove_from_watchlist(company_key_: str):
    with sqlite3.connect(DB_PATH) as con:
        con.execute("DELETE FROM watchlist WHERE company_key=?", (company_key_,))

def list_watchlist() -> List[Dict[str, Any]]:
    with sqlite3.connect(DB_PATH) as con:
        cur = con.execute("SELECT company_key, label, input_value, created_at, last_run_id FROM watchlist ORDER BY created_at DESC")
        rows = cur.fetchall()
    return [{"company_key": r[0], "label": r[1], "input_value": r[2], "created_at": r[3], "last_run_id": r[4]} for r in rows]

def update_watchlist_last_run(company_key_: str, run_id: str):
    with sqlite3.connect(DB_PATH) as con:
        con.execute("UPDATE watchlist SET last_run_id=? WHERE company_key=?", (run_id, company_key_))

# --------------------------- Utils ---------------------------

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

def host_from_url(url: str) -> str:
    try:
        return urlparse(url).netloc.replace("www.", "")
    except Exception:
        return ""

# --------------------------- Async Search & Fetch ---------------------------

@dataclass
class SearchItem:
    title: str
    url: str
    snippet: str
    engine: str

async def searxng_search_async(session: aiohttp.ClientSession, query: str, language: str = "en", time_range: Optional[str] = None, limit: int = 20) -> List[SearchItem]:
    """Async query to local SearXNG."""
    params = {"q": query, "format": "json", "language": language}
    if time_range:
        params["time_range"] = time_range

    try:
        async with session.get(SEARXNG_URL, params=params, timeout=10) as resp:
            if resp.status != 200:
                return []
            data = await resp.json()
            items = []
            for item in data.get("results", []):
                items.append(SearchItem(
                    title=item.get("title") or "",
                    url=item.get("url") or "",
                    snippet=item.get("content") or "",
                    engine=item.get("engine") or ""
                ))
            return items
    except Exception as e:
        print(f"Search Error ({query}): {type(e).__name__} - {e}")
        return []

def extract_with_trafilatura(html: str) -> Dict[str, Any]:
    downloaded = trafilatura.extract(html, include_comments=False, include_tables=False, with_metadata=True, output_format="json")
    if not downloaded:
        return {"title": "", "text": "", "published_at": None}
    j = json.loads(downloaded)
    return {"title": (j.get("title") or "").strip(), "text": (j.get("text") or "").strip(), "published_at": j.get("date")}

async def fetch_url_playwright(context, url: str) -> str:
    """Fetch a single URL using an existing Playwright browser context."""
    page = await context.new_page()
    try:
        await page.goto(url, wait_until="domcontentloaded", timeout=15000)
        content = await page.content()
        return content
    except Exception:
        return ""
    finally:
        await page.close()

async def fetch_and_extract_async(session: aiohttp.ClientSession, browser_context, url: str, use_playwright: bool) -> Dict[str, Any]:
    # Check cache first
    cached = get_cached(url)
    if cached and cached.get("text"):
         # Simple freshness check (optional: add logic to re-fetch if old)
         return cached

    html = ""
    # 1. Try fast HTTP fetch
    try:
        async with session.get(url, timeout=10, headers={"User-Agent": USER_AGENT}) as resp:
            if resp.status == 200:
                html = await resp.text()
    except Exception:
        pass

    out = extract_with_trafilatura(html)
    text = out["text"]

    # 2. Fallback to Playwright if text is too short
    if use_playwright and browser_context and len(text) < 500:
        html_pw = await fetch_url_playwright(browser_context, url)
        out_pw = extract_with_trafilatura(html_pw)
        if len(out_pw["text"]) > len(text):
            out = out_pw

    doc = {
        "url": url,
        "title": out["title"],
        "published_at": out["published_at"],
        "source": host_from_url(url),
        "snippet": "",
        "text": out["text"],
        "fetched_at": datetime.now().isoformat(),
    }
    # Sync write to DB (fast enough for SQLite WAL)
    upsert_doc(doc)
    return doc

# --------------------------- LLM Analysis ---------------------------

async def analyze_with_llm(session: aiohttp.ClientSession, company: str, docs_by_bucket: Dict[str, List[Dict[str, Any]]]) -> str:
    """
    Sends aggregated text to the configured LLM for an executive summary.
    """
    if not docs_by_bucket:
        return "No data found to analyze."

    # Prepare context
    context_lines = []
    for bucket, docs in docs_by_bucket.items():
        if not docs: continue
        context_lines.append(f"--- SECTION: {bucket.upper()} ---")
        for d in docs[:4]: # Top 4 per bucket to save context window
            txt = (d.get("text") or "")[:800].replace("\n", " ") # Truncate
            context_lines.append(f"Title: {d.get('title')}\nSource: {d.get('source')}\nContent: {txt}\n")
    
    context_str = "\n".join(context_lines)
    
    prompt = f"""
    You are a professional market research agent. 
    Analyze the following recent information about the company '{company}'.
    
    Write a concise Executive Briefing that covers:
    1. Key recent news or events (focus on facts).
    2. Strategic moves (launches, mergers, leadership changes).
    3. Market sentiment (if available from reviews/chatter).
    
    Data Context:
    {context_str}
    
    Format: Markdown. Be professional, objective, and cite sources by name if specific.
    """

    payload = {
        "model": LLM_MODEL,
        "messages": [{"role": "user", "content": prompt}],
        "temperature": 0.3
    }

    headers = {"Authorization": f"Bearer {LLM_API_KEY}", "Content-Type": "application/json"}
    
    try:
        # Generic OpenAI-compatible endpoint
        async with session.post(f"{LLM_BASE_URL}/chat/completions", json=payload, headers=headers, timeout=60) as resp:
            if resp.status == 200:
                res_json = await resp.json()
                return res_json["choices"][0]["message"]["content"]
            else:
                err = await resp.text()
                return f"LLM Analysis Failed: {resp.status} - {err}"
    except Exception as e:
        return f"LLM Connection Error: {e}"

# --------------------------- Core Scan Logic ---------------------------

def build_queries(entity: Dict[str, str], days: int) -> Dict[str, List[str]]:
    base = entity["company"]
    domain = entity.get("domain") or ""
    key = f'"{base}"'
    return {
        "news": [f'{key} (funding OR acquisition OR launch OR lawsuit)', f'{key} business news'],
        "reviews": [f'{key} reviews', f'site:reddit.com {key}'],
        "people": [f'{key} (CEO OR CFO OR executive team)'],
    }

async def run_scan_async(company_input: str, days: int, per_bucket: int, use_playwright: bool):
    ensure_db()
    entity = normalize_company_input(company_input)
    ckey = company_key(entity)
    run_id = str(uuid.uuid4())
    
    queries = build_queries(entity, days)
    
    async with aiohttp.ClientSession() as session:
        # 1. Parallel Search
        search_tasks = []
        bucket_map = [] # Keep track of which task belongs to which bucket
        
        for bucket, query_list in queries.items():
            for q in query_list:
                search_tasks.append(searxng_search_async(session, q, limit=per_bucket))
                bucket_map.append(bucket)
        
        search_results = await asyncio.gather(*search_tasks)
        
        # Organize results
        buckets_raw = {}
        for i, items in enumerate(search_results):
            bucket = bucket_map[i]
            if bucket not in buckets_raw: buckets_raw[bucket] = []
            buckets_raw[bucket].extend(items)

        # 2. Fetch Content (Parallel)
        pw_context = None
        pw_browser = None
        playwright_obj = None

        if use_playwright:
            playwright_obj = await async_playwright().start()
            pw_browser = await playwright_obj.chromium.launch(headless=True)
            pw_context = await pw_browser.new_context(user_agent=USER_AGENT)

        try:
            fetch_tasks = []
            doc_meta_map = [] # bucket, search_item
            
            seen_urls = set()

            for bucket, items in buckets_raw.items():
                # Dedup and limit
                unique_items = []
                for it in items:
                    if it.url and it.url not in seen_urls:
                        seen_urls.add(it.url)
                        unique_items.append(it)
                
                # Create fetch tasks
                for it in unique_items[:per_bucket]:
                    fetch_tasks.append(fetch_and_extract_async(session, pw_context, it.url, use_playwright))
                    doc_meta_map.append((bucket, it))

            # Execute fetches
            fetched_docs = await asyncio.gather(*fetch_tasks)

            # 3. Store Results
            buckets_docs = {}
            for i, doc in enumerate(fetched_docs):
                bucket, search_item = doc_meta_map[i]
                if bucket not in buckets_docs: buckets_docs[bucket] = []
                
                # Merge snippet if full text failed
                if not doc["text"]: doc["snippet"] = search_item.snippet
                
                buckets_docs[bucket].append(doc)
                record_run_doc(run_id, bucket, doc, 1.0) # Simplified score

            # 4. LLM Analysis
            llm_summary = await analyze_with_llm(session, entity["company"], buckets_docs)
            record_run(run_id, company_input, llm_summary)
            record_run_meta(run_id, ckey, company_input, days)
            
            # Watchlist update
            update_watchlist_last_run(ckey, run_id)

            return {
                "entity": entity,
                "run_id": run_id,
                "buckets_docs": buckets_docs,
                "llm_summary": llm_summary
            }

        finally:
            if pw_browser: await pw_browser.close()
            if playwright_obj: await playwright_obj.stop()

# --------------------------- Reporting (PDF Fix) ---------------------------

def write_pdf_report_platypus(pdf_path: str, title: str, summary: str, buckets_docs: Dict[str, List[Dict[str, Any]]]):
    doc = SimpleDocTemplate(pdf_path, pagesize=A4, rightMargin=2*cm, leftMargin=2*cm, topMargin=2*cm, bottomMargin=2*cm)
    styles = getSampleStyleSheet()
    story = []

    # Title
    story.append(Paragraph(title, styles['Title']))
    story.append(Spacer(1, 0.5*cm))
    
    # Date
    date_str = datetime.now().strftime("%Y-%m-%d %H:%M")
    story.append(Paragraph(f"Generated on: {date_str}", styles['Normal']))
    story.append(Spacer(1, 1*cm))

    # LLM Summary Section
    if summary:
        story.append(Paragraph("Executive Summary", styles['Heading2']))
        # Clean markdown bolding for PDF
        clean_summary = summary.replace("**", "<b>").replace("n't", "n't") 
        # Note: ReportLab simple markdown support is limited, standard text is safer
        story.append(Paragraph(summary.replace("\n", "<br/>"), styles['BodyText']))
        story.append(Spacer(1, 1*cm))

    # Findings by Bucket
    for bucket, docs in buckets_docs.items():
        if not docs: continue
        story.append(Paragraph(bucket.replace("_", " ").title(), styles['Heading2']))
        
        for d in docs:
            d_title = d.get('title') or d.get('url')
            d_url = d.get('url')
            d_source = d.get('source')
            
            # Item Title
            story.append(Paragraph(f"<b>{d_title}</b>", styles['Heading4']))
            # Source
            story.append(Paragraph(f"<i>Source: {d_source}</i>", styles['Normal']))
            # URL (Wrapped)
            story.append(Paragraph(f"<a href='{d_url}' color='blue'>{d_url}</a>", styles['BodyText']))
            story.append(Spacer(1, 0.3*cm))
        
        story.append(Spacer(1, 0.5*cm))

    try:
        doc.build(story)
    except Exception as e:
        print(f"PDF Gen Error: {e}")

def generate_markdown_report(entity, summary, buckets_docs):
    lines = [f"# Research Report: {entity['company']}"]
    lines.append(f"Date: {datetime.now().strftime('%Y-%m-%d')}\n")
    
    if summary:
        lines.append("## Executive Summary (AI)")
        lines.append(summary)
        lines.append("\n---\n")
    
    for bucket, docs in buckets_docs.items():
        lines.append(f"### {bucket.title()}")
        for d in docs:
            lines.append(f"- **{d.get('title') or 'Link'}** ({d.get('source')})")
            lines.append(f"  - {d['url']}")
        lines.append("")
    return "\n".join(lines)

# --------------------------- UI ---------------------------

def ui_main():
    st.set_page_config(page_title="Agent 0.2", layout="wide")
    st.title("AI Market Research Agent")
    st.caption(f"Powered by {LLM_MODEL} & SearXNG")

    q = st.text_input("Company / Topic")
    
    col1, col2 = st.columns(2)
    with col1:
        days = st.slider("Lookback Days", 7, 90, 30)
    with col2:
        use_playwright = st.checkbox("Deep Scraping (Slower)", value=True)

    if st.button("Run Agent", type="primary"):
        with st.status("Agent Working...", expanded=True) as status:
            st.write("Searching & Scraping (Async)...")
            
            # Run the async scan loop
            try:
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                result = loop.run_until_complete(run_scan_async(q, days, 5, use_playwright))
            except Exception as e:
                st.error(f"Scan failed: {e}")
                st.stop()
            
            status.update(label="Analysis Complete", state="complete")
        
        # Display Results
        st.subheader("Executive Synthesis")
        st.markdown(result["llm_summary"])
        
        st.divider()
        st.subheader("Source Data")
        for bucket, docs in result["buckets_docs"].items():
            with st.expander(f"{bucket.title()} ({len(docs)})"):
                for d in docs:
                    st.write(f"**[{d['title']}]({d['url']})** - {d['source']}")

        # Export
        md_text = generate_markdown_report(result["entity"], result["llm_summary"], result["buckets_docs"])
        st.download_button("Download Report (MD)", md_text, "report.md")
        
        # PDF
        ensure_db() # ensures data dir
        pdf_path = f"data/report_{result['run_id']}.pdf"
        write_pdf_report_platypus(pdf_path, f"Research: {result['entity']['company']}", result["llm_summary"], result["buckets_docs"])
        with open(pdf_path, "rb") as f:
            st.download_button("Download Report (PDF)", f, "report.pdf")

# --------------------------- Entry ---------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--scan", help="CLI mode scan")
    args = parser.parse_args()

    if args.scan:
        print(f"Starting async scan for {args.scan}...")
        res = asyncio.run(run_scan_async(args.scan, 30, 5, True))
        print("\n=== SUMMARY ===\n")
        print(res["llm_summary"])
        pdf_name = f"report_{res['run_id']}.pdf"
        write_pdf_report_platypus(pdf_name, args.scan, res["llm_summary"], res["buckets_docs"])
        print(f"\nSaved PDF: {pdf_name}")
    else:
        ui_main()