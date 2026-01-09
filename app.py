import os
import io
import re
import json
import uuid
import asyncio
import argparse
import logging
from dataclasses import dataclass
from datetime import datetime
from urllib.parse import urlparse

# --- 3rd Party ---
import aiohttp
import sqlite3
import nest_asyncio
from dotenv import load_dotenv  # <--- NEW IMPORT

# PDF & Scraping
from playwright.async_api import async_playwright
import trafilatura
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, PageBreak
from reportlab.lib.units import cm

# UI
try:
    import streamlit as st
    nest_asyncio.apply()
except ImportError:
    st = None

# --- LOAD ENV VARS ---
load_dotenv()  # <--- Loads variables from .env

# --- CONSTANTS & CONFIG ---
DATA_DIR = "data"
DB_PATH = os.path.join(DATA_DIR, "cache.sqlite")
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36"

SEARXNG_URL = os.getenv("SEARXNG_URL", "http://localhost:8080/search")

# --------------------------- DATABASE ---------------------------
def init_db():
    os.makedirs(DATA_DIR, exist_ok=True)
    with sqlite3.connect(DB_PATH) as con:
        con.execute("PRAGMA journal_mode=WAL;")
        con.execute("""
            CREATE TABLE IF NOT EXISTS reports (
                run_id TEXT PRIMARY KEY,
                company TEXT,
                query_params TEXT,
                summary TEXT,
                raw_data TEXT,
                created_at TEXT
            )
        """)

def save_report(run_id, company, params, summary, raw_data):
    with sqlite3.connect(DB_PATH) as con:
        con.execute(
            "INSERT OR REPLACE INTO reports VALUES (?, ?, ?, ?, ?, ?)",
            (run_id, company, json.dumps(params), summary, json.dumps(raw_data), datetime.now().isoformat())
        )

def load_history():
    with sqlite3.connect(DB_PATH) as con:
        cur = con.execute("SELECT run_id, company, created_at FROM reports ORDER BY created_at DESC")
        return [{"id": r[0], "company": r[1], "date": r[2]} for r in cur.fetchall()]

def load_run(run_id):
    with sqlite3.connect(DB_PATH) as con:
        cur = con.execute("SELECT * FROM reports WHERE run_id=?", (run_id,))
        row = cur.fetchone()
        if row:
            return {
                "run_id": row[0], "company": row[1], "params": json.loads(row[2]),
                "summary": row[3], "raw_data": json.loads(row[4]), "created_at": row[5]
            }
    return None

def delete_report(run_id):
    with sqlite3.connect(DB_PATH) as con:
        con.execute("DELETE FROM reports WHERE run_id=?", (run_id,))

# --------------------------- NETWORK CORE (ASYNC) ---------------------------

@dataclass
class SearchResult:
    title: str
    url: str
    snippet: str

async def search_searxng(session, query, limit=5, time_range=None):
    """
    Hits local SearXNG. 
    time_range options: 'day', 'week', 'month', 'year'
    """
    params = {"q": query, "format": "json", "language": "en"}
    if time_range:
        params["time_range"] = time_range
        
    try:
        async with session.get(SEARXNG_URL, params=params, timeout=10) as resp:
            if resp.status != 200: return []
            data = await resp.json()
            results = []
            for item in data.get("results", [])[:limit]:
                results.append(SearchResult(
                    title=item.get("title", ""),
                    url=item.get("url", ""),
                    snippet=item.get("content", "")
                ))
            return results
    except Exception as e:
        print(f"Search Error [{query}]: {e}")
        return []
    
async def fetch_page(session, context, url, use_playwright=False):
    text = ""
    try:
        async with session.get(url, timeout=10, headers={"User-Agent": USER_AGENT}) as resp:
            if resp.status == 200:
                html = await resp.text()
                text = trafilatura.extract(html) or ""
    except:
        pass

    if use_playwright and (not text or len(text) < 500):
        try:
            page = await context.new_page()
            await page.goto(url, wait_until="domcontentloaded", timeout=20000)
            content = await page.content()
            text = trafilatura.extract(content) or ""
            await page.close()
        except:
            pass
            
    return {"url": url, "text": text[:15000]}

async def llm_analyze(session, api_base, api_key, model, prompt):
    headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
    payload = {
        "model": model,
        "messages": [{"role": "user", "content": prompt}],
        "temperature": 0.3
    }
    try:
        async with session.post(f"{api_base}/chat/completions", json=payload, headers=headers, timeout=180) as resp:
            if resp.status != 200:
                err = await resp.text()
                return f"Error: {resp.status} - {err}"
            data = await resp.json()
            return data["choices"][0]["message"]["content"]
    except Exception as e:
        return f"LLM Connection Failed: {e}"

# --------------------------- MAIN LOGIC ---------------------------

async def run_agent(company, days, deep_scrape, api_cfg):
    run_id = str(uuid.uuid4())
    status = st.status("Agent Started", expanded=True)
    
    # --- STRATEGY: TIME-BASED FILTERING ---
    # We don't ask for "CEO" specifically. We ask for "Everything new".
    # We run parallel searches: 
    # 1. "Last Month" (Strict) - catches breaking news (CEO, earnings, lawsuits)
    # 2. "General" (Broad) - catches context, competitors, and older major events
    
    async with aiohttp.ClientSession() as session:
        # --- PHASE 2: SEARCHING ---
        status.write(f"🔍 Searching last 30 days of news for {company}...")
        
        search_tasks = [
            # 1. STRICT RECENCY (The "News" Filter)
            search_searxng(session, f'"{company}" news', limit=5, time_range="month"),
            search_searxng(session, f'"{company}" press release', limit=3, time_range="month"),
            search_searxng(session, f'"{company}" business', limit=3, time_range="month"),
            
            # 2. BROADER CONTEXT (No time limit, but low count)
            search_searxng(session, f'"{company}" strategic outlook', limit=2),
            search_searxng(session, f'"{company}" competitors', limit=2)
        ]
        
        results_nested = await asyncio.gather(*search_tasks)
        
        all_links = {}
        for res_list in results_nested:
            for item in res_list:
                if item.url not in all_links:
                    all_links[item.url] = item

        # --- PHASE 3: SCRAPING ---
        status.write(f"📖 Reading {len(all_links)} pages (Concurrency Limited)...")
        
        pw_obj = None
        pw_browser = None
        pw_context = None
        
        if deep_scrape:
            pw_obj = await async_playwright().start()
            pw_browser = await pw_obj.chromium.launch(headless=True)
            pw_context = await pw_browser.new_context(user_agent=USER_AGENT)

        sem = asyncio.Semaphore(4) 
        
        async def protected_fetch(url):
            async with sem:
                res = await fetch_page(session, pw_context, url, deep_scrape)
                res["title"] = all_links[url].title
                return res

        fetch_tasks = [protected_fetch(url) for url in all_links.keys()]
        docs = await asyncio.gather(*fetch_tasks)

        if pw_browser: await pw_browser.close()
        if pw_obj: await pw_obj.stop()
        
        # --- PHASE 4: SYNTHESIS ---
        status.write("🧠 Thinking (AI Analysis)...")
        
        context_text = ""
        for d in docs:
            if d.get("text"):
                # We feed the AI up to 8000 chars per article to ensure it sees the "meat" of the news
                context_text += f"\n=== SOURCE: {d['title']} ({d['url']}) ===\n{d['text'][:8000]}\n"
        
        # --- PROMPT: GENERAL INTELLIGENCE ---
        prompt = f"""
        You are an expert market intelligence agent.
        
        TARGET COMPANY: {company}
        TODAY'S DATE: {datetime.now().strftime('%Y-%m-%d')}
        
        INSTRUCTIONS:
        1. Review the SOURCE DATA below, which includes news from the **last 30 days**.
        2. Identify the **Single Most Important Event** that happened recently (e.g., leadership change, big acquisition, earnings shock).
        3. List other key developments.
        4. If the news mentions a specific date, cite it.
        
        OUTPUT FORMAT:
        # 🚨 Breaking / Major News (Last 30 Days)
        [If a major event occurred (like a CEO leaving, a lawsuit, or a merger), detail it here FIRST. Be specific.]
        
        # Key Business Updates
        - [Update 1]
        - [Update 2]
        
        # Market Sentiment
        [Brief Summary]
        
        SOURCE DATA:
        {context_text}
        """
        
        summary = await llm_analyze(session, api_cfg['base'], api_cfg['key'], api_cfg['model'], prompt)
        
        save_report(run_id, company, {"days": days}, summary, docs)
        
        status.update(label="Complete!", state="complete", expanded=False)
        return {"summary": summary, "docs": docs, "run_id": run_id}
        
# --------------------------- PDF REPORTING ---------------------------

def create_pdf(filename, company, summary, docs):
    doc = SimpleDocTemplate(filename, pagesize=A4)
    styles = getSampleStyleSheet()
    story = []
    
    story.append(Paragraph(f"Research Report: {company}", styles['Title']))
    story.append(Spacer(1, 0.5*cm))
    story.append(Paragraph(f"Generated: {datetime.now().strftime('%Y-%m-%d')}", styles['Normal']))
    story.append(Spacer(1, 1*cm))
    
    story.append(Paragraph("Executive Synthesis", styles['Heading2']))
    
    for line in summary.split('\n'):
        line = line.strip()
        if line:
            line = re.sub(r'\*\*(.*?)\*\*', r'<b>\1</b>', line)
            line = line.replace('#', '')
            try:
                story.append(Paragraph(line, styles['BodyText']))
            except Exception:
                clean_line = line.replace('<b>', '').replace('</b>', '').replace('<', '').replace('>', '')
                story.append(Paragraph(clean_line, styles['BodyText']))
            story.append(Spacer(1, 0.2*cm))
            
    story.append(PageBreak())
    
    story.append(Paragraph("Source Materials", styles['Heading2']))
    for d in docs:
        if d['text']:
            d_title = str(d.get('title', 'Link')).replace('<', '&lt;').replace('>', '&gt;')
            d_url = str(d['url']).replace('<', '&lt;').replace('>', '&gt;')
            story.append(Paragraph(f"<b>{d_title}</b>", styles['Heading4']))
            story.append(Paragraph(f"<a href='{d_url}' color='blue'>{d_url}</a>", styles['Normal']))
            story.append(Spacer(1, 0.5*cm))
            
    doc.build(story)

# --------------------------- UI ---------------------------

def main():
    st.set_page_config(page_title="Agent Pro", layout="wide")
    init_db()

    if "selected_run_id" not in st.session_state:
        st.session_state["selected_run_id"] = None

    # --- SIDEBAR: HISTORY ---
    st.sidebar.title("🗄️ History")
    
    if st.sidebar.button("➕ New Research", type="primary"):
        st.session_state["selected_run_id"] = None
        st.rerun()

    history = load_history()
    if not history:
        st.sidebar.caption("No reports yet.")
    
    for item in history:
        col1, col2 = st.sidebar.columns([0.8, 0.2])
        with col1:
            if st.button(f"{item['company']}", key=f"sel_{item['id']}"):
                st.session_state["selected_run_id"] = item['id']
                st.rerun()
        with col2:
            if st.button("❌", key=f"del_{item['id']}"):
                delete_report(item['id'])
                if st.session_state["selected_run_id"] == item['id']:
                    st.session_state["selected_run_id"] = None
                st.rerun()

    # --- MAIN AREA ---
    if st.session_state["selected_run_id"]:
        data = load_run(st.session_state["selected_run_id"])
        
        if not data:
            st.error("Report not found.")
            st.session_state["selected_run_id"] = None
            st.stop()

        st.title(f"📁 Report: {data['company']}")
        st.caption(f"Created: {data['created_at'][:16]}")
        st.markdown(data['summary'])
        
        pdf_buffer = io.BytesIO()
        create_pdf(pdf_buffer, data['company'], data['summary'], data['raw_data'])
        pdf_bytes = pdf_buffer.getvalue()
        
        st.download_button(
            label="📄 Download PDF Report",
            data=pdf_bytes,
            file_name=f"report_{data['run_id']}.pdf",
            mime="application/pdf"
        )
        
        if st.button("← Back to Search"):
            st.session_state["selected_run_id"] = None
            st.rerun()
                
    else:
        st.title("🚀 Speed Research Agent")
        
        # --- UI SETTINGS NOW DEFAULT TO .ENV VALUES ---
        with st.expander("⚙️ Settings", expanded=True):
            col1, col2 = st.columns(2)
            
            # Auto-select mode based on .env
            default_mode = 0 if os.getenv("AI_PROVIDER") == "ollama" else 1
            
            with col1:
                mode = st.radio("AI Backend", ["Local (Ollama)", "Cloud (Groq/OpenAI)"], index=default_mode)
            with col2:
                if "Cloud" in mode:
                    api_key = st.text_input("API Key", type="password", value=os.getenv("GROQ_API_KEY", ""))
                    base_url = st.text_input("Base URL", value="https://api.groq.com/openai/v1")
                    model = st.text_input("Model", value=os.getenv("GROQ_MODEL", "llama3-70b-8192"))
                else:
                    api_key = "ollama"
                    # Pull defaults from .env
                    base_url = st.text_input("Local URL", value=os.getenv("OLLAMA_BASE_URL", "http://127.0.0.1:11434/v1"))
                    model = st.text_input("Local Model", value=os.getenv("OLLAMA_MODEL", "llama3"))

        company = st.text_input("Company Name")
        deep_scrape = st.checkbox("Deep Scrape (Slower but better data)", value=False)
        
        if st.button("Run Research", type="primary"):
            if not company:
                st.warning("Enter a company name.")
                st.stop()
                
            api_cfg = {"base": base_url, "key": api_key, "model": model}
            
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            res = loop.run_until_complete(run_agent(company, 30, deep_scrape, api_cfg))
            
            st.session_state["selected_run_id"] = res["run_id"]
            st.rerun()

if __name__ == "__main__":
    if os.name == 'nt':
        asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
    main()