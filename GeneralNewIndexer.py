import os
import re
import json
import asyncio
from typing import List, Dict, Any, Optional
from itertools import cycle
from datetime import datetime
import sys
import boto3
import httpx
from dotenv import load_dotenv
from ddgs import DDGS

# ==========================================
# 1. ENVIRONMENT & ROTATORS
# ==========================================
load_dotenv()

# System Config
NEWS_ARTICLE_COUNT = 1  # Expanded articles per category

GROQ_MODEL = "llama-3.3-70b-versatile"

def load_keys(prefix: str) -> List[str]:
    """Dynamically loads all keys matching a specific prefix from .env."""
    keys = []
    for k, v in os.environ.items():
        if k.startswith(prefix) and v.strip():
            keys.append(v.strip())
    keys.sort()
    if keys:
        print(f"[DEBUG] [SYSTEM] Loaded {len(keys)} keys for prefix '{prefix}'")
    else:
        print(f"[DEBUG] [WARNING] No keys found for '{prefix}'. Using fallback key.")
    return keys if keys else ["DEMO_KEY"]

# Rotators for API Keys
TWELVE_DATA_KEYS = cycle(load_keys("TD_KEY_"))
LOGO_DEV_KEYS = cycle(load_keys("LD_KEY_"))
GROQ_KEYS = cycle(load_keys("GROQ_KEY_"))  # Added Groq Rotator
# ==========================================
# 2. DEFENSIVE PARSERS & CLOUDFLARE AI ENGINE
# ==========================================
def upload_payload_to_e2(payload: dict, filename: str = "General_news.json") -> bool:
    """Uploads the master JSON payload directly to an iDrive e2 bucket."""
    endpoint_url = os.getenv("IDRIVE_ENDPOINT")  # e.g., https://x3v2.s3.us-west-1.idrivee2-22.com
    access_key = os.getenv("IDRIVE_ACCESS_KEY")
    secret_key = os.getenv("IDRIVE_SECRET_KEY")
    bucket_name = os.getenv("IDRIVE_BUCKET")

    if not all([endpoint_url, access_key, secret_key, bucket_name]):
        print("[DEBUG] [ERROR] Missing iDrive e2 credentials in environment variables.")
        return False

    print(f"[DEBUG] [STORAGE] Uploading payload to iDrive e2 ('{bucket_name}/{filename}')...")
    try:
        s3_client = boto3.client(
            "s3",
            endpoint_url=endpoint_url,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key
        )

        s3_client.put_object(
            Bucket=bucket_name,
            Key=filename,
            Body=json.dumps(payload, indent=2),
            ContentType="application/json"
        )
        print("[DEBUG] [STORAGE] Successfully uploaded payload to iDrive e2.")
        return True
    except Exception as e:
        print(f"[DEBUG] [ERROR] Failed to upload to iDrive e2: {e}")
        return False

def safe_get_str(data: Any, key: str, fallback: str) -> str:
    """Guarantees a valid, non-empty string. Prevents None/null parsing crashes."""
    if not isinstance(data, dict):
        return fallback
    val = data.get(key)
    if val is None:
        return fallback
    val_str = str(val).strip()
    if not val_str or val_str.lower() in ("none", "null", "undefined", "unknown"):
        return fallback
    return val_str

def extract_json_payload(raw_text: str) -> Dict[str, Any]:
    """Defensively extracts JSON from raw LLM responses, cleaning markdown formatting."""
    if not raw_text:
        return {}
    try:
        clean_text = re.sub(r"^```(?:json)?\s*", "", raw_text.strip(), flags=re.IGNORECASE)
        clean_text = re.sub(r"\s*```$", "", clean_text)
        
        match = re.search(r'(\{.*\}|\[.*\])', clean_text, re.DOTALL)
        if match:
            return json.loads(match.group(0))
        return json.loads(clean_text)
    except json.JSONDecodeError as e:
        print(f"[DEBUG] [ERROR] JSON Parse Failed. Snippet: {raw_text[:120]}... Error: {e}")
        return {}

async def call_groq_ai(
    client: httpx.AsyncClient, 
    prompt: str, 
    system_msg: str = "You are an executive data extraction AI. Always respond with strictly valid JSON without preamble or explanation."
) -> Dict[str, Any]:
    """Sends requests to Groq API using key rotation."""
    api_key = next(GROQ_KEYS, None)
    
    if not api_key or api_key == "DEMO_KEY":
        print("[DEBUG] [ERROR] Groq credentials missing in .env. Aborting AI request.")
        return {}

    url = "https://api.groq.com/openai/v1/chat/completions"
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json"
    }
    payload = {
        "model": GROQ_MODEL,
        "messages": [
            {"role": "system", "content": system_msg},
            {"role": "user", "content": prompt}
        ],
        "temperature": 0.1,
        "response_format": {"type": "json_object"} # Forces strict JSON out of Llama 3
    }
    
    try:
        response = await client.post(url, headers=headers, json=payload, timeout=25.0)
        response.raise_for_status()
        data = response.json()
            
        raw_output = data.get("choices", [{}])[0].get("message", {}).get("content", "")
        return extract_json_payload(raw_output)
        
    except Exception as e:
        print(f"[DEBUG] [EXCEPTION] Groq API Call Failed: {e}")
        return {}

# ==========================================
# 3. CORE OSINT FETCHERS & PHOTO ENGINES
# ==========================================

async def fetch_entity_hero_photo(query: str, tag: str) -> str:
    """Fetches a high-resolution hero photo for an entity using DuckDuckGo Image Search."""
    clean_query = query.strip() if query else tag
    print(f"[DEBUG] [{tag}] Fetching hero photo for query: '{clean_query}'...")
    try:
        def get_images():
            with DDGS() as ddgs:
                return list(ddgs.images(clean_query, max_results=3))
                
        results = await asyncio.to_thread(get_images)
        if results and isinstance(results, list):
            for img in results:
                image_url = img.get("image") or img.get("thumbnail")
                if image_url:
                    print(f"[DEBUG] [{tag}] Successfully fetched hero photo URL.")
                    return image_url
        print(f"[DEBUG] [{tag}] [WARNING] No hero image found via DDGS.")
        return ""
    except Exception as e:
        print(f"[DEBUG] [{tag}] Hero Photo Fetch Error: {e}")
        return ""

async def fetch_duckduckgo_news(query: str, tag: str, max_results: int = NEWS_ARTICLE_COUNT) -> List[Dict[str, Any]]:
    """Fetches real-time news with full metadata and image photo URLs using DuckDuckGo, including image fallbacks."""
    clean_query = query.strip() if query else ""
    if not clean_query or len(clean_query) < 2:
        print(f"[DEBUG] [{tag}] [WARNING] Invalid or empty news query provided. Using domain tag fallback.")
        clean_query = tag if tag and len(tag) >= 2 else "global technology breakthroughs"

    print(f"[DEBUG] [{tag}] Fetching top {max_results} live news articles for query: '{clean_query}'...")
    try:
        def get_news():
            with DDGS() as ddgs:
                return list(ddgs.news(clean_query, max_results=max_results))
                
        results = await asyncio.to_thread(get_news)
        print(f"[DEBUG] [{tag}] Retrieved {len(results)} news items.")
        
        # Pre-fetch fallback images in case news items lack inline photos
        fallback_images = []
        missing_photos_count = sum(1 for item in results if not item.get("image"))
        if missing_photos_count > 0:
            def get_fallback_imgs():
                with DDGS() as ddgs:
                    return list(ddgs.images(f"{clean_query} news", max_results=missing_photos_count + 2))
            try:
                img_res = await asyncio.to_thread(get_fallback_imgs)
                fallback_images = [i.get("image") or i.get("thumbnail") for i in img_res if i.get("image") or i.get("thumbnail")]
            except Exception:
                fallback_images = []

        articles = []
        fallback_idx = 0
        for item in results:
            photo_url = item.get("image", "")
            if not photo_url and fallback_idx < len(fallback_images):
                photo_url = fallback_images[fallback_idx]
                fallback_idx += 1

            articles.append({
                "title": item.get("title", "Breaking News"),
                "snippet": item.get("body", "No summary available."),
                "date": item.get("date", "")[:10] if item.get("date") else "",
                "photoUrl": photo_url,
                "source": item.get("source", "DuckDuckGo"),
                "url": item.get("url", "")
            })
        return articles
    except Exception as e:
        print(f"[DEBUG] [{tag}] DuckDuckGo News Fetch Error: {e}")
        return []

async def fetch_stock_candles(symbol: str, client: httpx.AsyncClient) -> List[Dict[str, Any]]:
    """Fetches 14-day OHLC candles from Twelve Data using key rotation."""
    key = next(TWELVE_DATA_KEYS)
    print(f"[DEBUG] [{symbol}] Fetching OHLC candles via TwelveData...")
    url = f"https://api.twelvedata.com/time_series?symbol={symbol}&interval=1day&outputsize=14&apikey={key}"
    
    try:
        response = await client.get(url)
        raw_stock = response.json()
        if "values" in raw_stock and isinstance(raw_stock["values"], list):
            print(f"[DEBUG] [{symbol}] Successfully fetched 14 daily candles.")
            return [
                {
                    "time": day["datetime"],
                    "open": float(day["open"]),
                    "high": float(day["high"]),
                    "low": float(day["low"]),
                    "close": float(day["close"])
                }
                for day in reversed(raw_stock["values"])
            ]
        return []
    except Exception as e:
        print(f"[DEBUG] [{symbol}] Candle Fetch Exception: {e}")
        return []

async def ai_enrich_entity(entity_tag: str, raw_name: str, client: httpx.AsyncClient, default_domain: str = "example.com") -> Dict[str, str]:
    """Uses Cloudflare AI to determine official domain name and clean query string."""
    clean_raw = raw_name.strip() if raw_name else entity_tag
    print(f"[DEBUG] [{entity_tag}] Enriching entity metadata for '{clean_raw}'...")
    
    prompt = (
        f"Entity: '{clean_raw}'\n"
        f"Determine the official web domain (e.g., nasa.gov, lakers.com, nvidia.com) and a clean 1-3 word search query.\n"
        f"Return ONLY JSON: {{\"domain\": \"...\", \"cleanName\": \"...\"}}"
    )
    
    data = await call_groq_ai(client, prompt)
    domain = safe_get_str(data, "domain", default_domain)
    clean_name = safe_get_str(data, "cleanName", clean_raw)
    
    print(f"[DEBUG] [{entity_tag}] Enriched -> Domain: '{domain}', Clean Name: '{clean_name}'")
    return {"domain": domain, "cleanName": clean_name}

# ==========================================
# 4. DOMAIN MODULE PIPELINES
# ==========================================

# --- A. FINANCE MODULE ---
async def process_finance_node(client: httpx.AsyncClient) -> Dict[str, Any]:
    print(f"\n[DEBUG] [=== STARTING FINANCE MODULE ===]")
    try:
        def search_finance():
            with DDGS() as ddgs:
                return list(ddgs.text("top stock day gainers today US market finance", max_results=3))
        results = await asyncio.to_thread(search_finance)
        search_context = "\n".join([f"- {r.get('title')}: {r.get('body')}" for r in results])
    except Exception:
        search_context = "NVIDIA, Tesla, and Apple showing strong market volume today."

    ai_extract = await call_groq_ai(
        client, 
        f"Web Search Results:\n{search_context}\n\nExtract 1 top gaining stock ticker and company name.\nReturn JSON: {{\"symbol\": \"NVDA\", \"name\": \"NVIDIA Corporation\"}}"
    )
    
    symbol = safe_get_str(ai_extract, "symbol", "NVDA").upper()
    raw_name = safe_get_str(ai_extract, "name", "NVIDIA Corporation")
    
    enriched = await ai_enrich_entity(symbol, raw_name, client, default_domain="nvidia.com")
    clean_name = enriched["cleanName"]
    domain = enriched["domain"]
    
    news_task = fetch_duckduckgo_news(clean_name, symbol, max_results=NEWS_ARTICLE_COUNT)
    candles_task = fetch_stock_candles(symbol, client)
    hero_photo_task = fetch_entity_hero_photo(clean_name, symbol)
    
    news_data, chart_series, hero_photo = await asyncio.gather(news_task, candles_task, hero_photo_task)
    
    dates = [c["time"] for c in chart_series] if chart_series else []
    prompt = (
        f"Finance Asset: {clean_name} ({symbol})\nNews Articles: {json.dumps(news_data)}\nAvailable Chart Dates: {dates}\n"
        f"Task:\n"
        f"1. Write a 1-sentence executive market insight.\n"
        f"2. Pick ONE date from 'Available Chart Dates' that best corresponds to the catalyst.\n"
        f"3. Provide a 2-word label for the chart marker.\n"
        f"4. Provide a overall sentiment score (-100 to +100).\n"
        f"Return JSON: {{\"remark\": \"...\", \"taggedDate\": \"...\", \"tagText\": \"...\", \"sentiment\": 75}}"
    )
    insights = await call_groq_ai(client, prompt)
    
    return {
        "category": "Finance",
        "entity": clean_name,
        "ticker": symbol,
        "brandLogo": f"https://img.logo.dev/{domain}?token={next(LOGO_DEV_KEYS)}",
        "heroPhoto": hero_photo,
        "insight": safe_get_str(insights, "remark", "Market performance tracking active."),
        "sentimentScore": insights.get("sentiment", 50),
        "chartSeries": chart_series,
        "chartMarker": {
            "time": safe_get_str(insights, "taggedDate", dates[-1] if dates else ""),
            "text": safe_get_str(insights, "tagText", "Market Event"),
            "position": "aboveBar",
            "color": "#e91e63",
            "shape": "arrowDown"
        },
        "newsFeed": news_data
    }

# --- B. SPORTS MODULE ---
async def process_sports_node(client: httpx.AsyncClient) -> Dict[str, Any]:
    print(f"\n[DEBUG] [=== STARTING SPORTS MODULE ===]")
    try:
        def search_sports():
            with DDGS() as ddgs:
                return list(ddgs.text("top trending sports teams athletes major news today", max_results=3))
        results = await asyncio.to_thread(search_sports)
        search_context = "\n".join([f"- {r.get('title')}: {r.get('body')}" for r in results])
    except Exception:
        search_context = "Los Angeles Lakers and Real Madrid dominating headline sports coverage."

    ai_extract = await call_groq_ai(
        client, 
        f"Web Search Results:\n{search_context}\n\nExtract 1 top trending sports team or athlete.\nReturn JSON: {{\"id\": \"LAL\", \"name\": \"Los Angeles Lakers\"}}"
    )
    
    raw_name = safe_get_str(ai_extract, "name", "Los Angeles Lakers")
    sport_id = safe_get_str(ai_extract, "id", "LAL")
    
    enriched = await ai_enrich_entity(sport_id, raw_name, client, default_domain="nba.com/lakers")
    clean_name = enriched["cleanName"]
    domain = enriched["domain"]
    
    news_task = fetch_duckduckgo_news(clean_name, "SPORT", max_results=NEWS_ARTICLE_COUNT)
    hero_photo_task = fetch_entity_hero_photo(clean_name, "SPORT")
    
    news_data, hero_photo = await asyncio.gather(news_task, hero_photo_task)
    
    prompt = (
        f"Sports Subject: {clean_name}\nArticles: {json.dumps(news_data)}\n"
        f"Task:\n"
        f"1. Summarize their current status in 1 sentence.\n"
        f"2. Provide a 1-3 word status tag (e.g., 'Roster Overhaul', 'Championship Pursuit', 'Injury Update').\n"
        f"3. Estimate team/player momentum score (0 to 100).\n"
        f"Return JSON: {{\"remark\": \"...\", \"statusTag\": \"...\", \"momentum\": 82}}"
    )
    insights = await call_groq_ai(client, prompt)
    
    return {
        "category": "Sports",
        "entity": clean_name,
        "brandLogo": f"https://img.logo.dev/{domain}?token={next(LOGO_DEV_KEYS)}",
        "heroPhoto": hero_photo,
        "insight": safe_get_str(insights, "remark", "Active competitive schedule."),
        "status": safe_get_str(insights, "statusTag", "In Season"),
        "momentumScore": insights.get("momentum", 70),
        "newsFeed": news_data
    }

# --- C. RESEARCH MODULE ---
async def process_research_node(client: httpx.AsyncClient) -> Dict[str, Any]:
    print(f"\n[DEBUG] [=== STARTING RESEARCH MODULE ===]")
    try:
        def search_research():
            with DDGS() as ddgs:
                return list(ddgs.text("major scientific research breakthrough organization today space AI", max_results=3))
        results = await asyncio.to_thread(search_research)
        search_context = "\n".join([f"- {r.get('title')}: {r.get('body')}" for r in results])
    except Exception:
        search_context = "NASA Artemis space program and CERN particle physics laboratory announce research updates."

    ai_extract = await call_groq_ai(
        client, 
        f"Web Search Context:\n{search_context}\n\nExtract 1 trending scientific organization, research lab, or mission.\nReturn JSON: {{\"id\": \"NASA\", \"name\": \"NASA Artemis Program\"}}"
    )
    
    raw_name = safe_get_str(ai_extract, "name", "NASA Artemis Program")
    rsch_id = safe_get_str(ai_extract, "id", "NASA")
    
    enriched = await ai_enrich_entity(rsch_id, raw_name, client, default_domain="nasa.gov")
    clean_name = enriched["cleanName"]
    domain = enriched["domain"]
    
    news_task = fetch_duckduckgo_news(clean_name, "RSCH", max_results=NEWS_ARTICLE_COUNT)
    hero_photo_task = fetch_entity_hero_photo(clean_name, "RSCH")
    
    news_data, hero_photo = await asyncio.gather(news_task, hero_photo_task)
    
    prompt = (
        f"Research Entity: {clean_name}\nArticles: {json.dumps(news_data)}\n"
        f"Task:\n"
        f"1. Summarize the research breakthrough in 1 concise sentence.\n"
        f"2. Estimate project completion progress percentage (0 to 100 integer).\n"
        f"3. Name the current development phase (e.g., 'Phase 2: Laboratory Testing', 'Phase 4: Deployment').\n"
        f"4. Specify impact scope (e.g., 'Global', 'Industry-Wide', 'Emerging').\n"
        f"Return JSON: {{\"remark\": \"...\", \"progressPercentage\": 78, \"currentPhase\": \"...\", \"impactScope\": \"...\"}}"
    )
    insights = await call_groq_ai(client, prompt)
    
    return {
        "category": "Research",
        "entity": clean_name,
        "brandLogo": f"https://img.logo.dev/{domain}?token={next(LOGO_DEV_KEYS)}",
        "heroPhoto": hero_photo,
        "insight": safe_get_str(insights, "remark", "Scientific discovery and development active."),
        "researchProgressBar": {
            "percentage": insights.get("progressPercentage", 65),
            "phase": safe_get_str(insights, "currentPhase", "Phase 2: Laboratory Testing"),
            "impactScope": safe_get_str(insights, "impactScope", "Global")
        },
        "newsFeed": news_data
    }

# --- D. MEDICINE MODULE ---
async def process_medicine_node(client: httpx.AsyncClient) -> Dict[str, Any]:
    print(f"\n[DEBUG] [=== STARTING MEDICINE MODULE ===]")
    try:
        def search_med():
            with DDGS() as ddgs:
                return list(ddgs.text("major medical breakthrough clinical trial FDA approval disease news today", max_results=3))
        results = await asyncio.to_thread(search_med)
        search_context = "\n".join([f"- {r.get('title')}: {r.get('body')}" for r in results])
    except Exception:
        search_context = "CRISPR gene therapy and new GLP-1 weight loss drugs show unprecedented clinical trial results."

    ai_extract = await call_groq_ai(
        client, 
        f"Web Search Context:\n{search_context}\n\nExtract 1 trending medical organization, pharmaceutical company, or breakthrough therapy.\nReturn JSON: {{\"id\": \"NVO\", \"name\": \"Novo Nordisk\"}}"
    )
    
    raw_name = safe_get_str(ai_extract, "name", "Novo Nordisk")
    med_id = safe_get_str(ai_extract, "id", "NVO")
    
    enriched = await ai_enrich_entity(med_id, raw_name, client, default_domain="nih.gov")
    clean_name = enriched["cleanName"]
    domain = enriched["domain"]
    
    news_task = fetch_duckduckgo_news(clean_name, "MED", max_results=NEWS_ARTICLE_COUNT)
    hero_photo_task = fetch_entity_hero_photo(clean_name, "MED")
    
    news_data, hero_photo = await asyncio.gather(news_task, hero_photo_task)
    
    prompt = (
        f"Medical Subject: {clean_name}\nArticles: {json.dumps(news_data)}\n"
        f"Task:\n"
        f"1. Summarize the medical breakthrough or regulatory status in 1 sentence.\n"
        f"2. Identify the target condition or disease.\n"
        f"3. Identify the current clinical/regulatory phase (e.g., 'Phase III Trials', 'FDA Approved', 'Pre-clinical').\n"
        f"4. Provide a brief 3-5 word efficacy/impact insight.\n"
        f"Return JSON: {{\"remark\": \"...\", \"targetCondition\": \"...\", \"trialPhase\": \"...\", \"efficacyInsight\": \"...\"}}"
    )
    insights = await call_groq_ai(client, prompt)
    
    return {
        "category": "Medicine",
        "entity": clean_name,
        "brandLogo": f"https://img.logo.dev/{domain}?token={next(LOGO_DEV_KEYS)}",
        "heroPhoto": hero_photo,
        "insight": safe_get_str(insights, "remark", "Monitoring clinical developments and FDA review cycles."),
        "clinicalMetrics": {
            "condition": safe_get_str(insights, "targetCondition", "General Medicine"),
            "phase": safe_get_str(insights, "trialPhase", "Ongoing Research"),
            "efficacy": safe_get_str(insights, "efficacyInsight", "Data pending peer review")
        },
        "newsFeed": news_data
    }

# --- E. EDUCATION MODULE ---
async def process_education_node(client: httpx.AsyncClient) -> Dict[str, Any]:
    print(f"\n[DEBUG] [=== STARTING EDUCATION MODULE ===]")
    try:
        def search_edu():
            with DDGS() as ddgs:
                return list(ddgs.text("major education policy university breakthrough edtech news today", max_results=3))
        results = await asyncio.to_thread(search_edu)
        search_context = "\n".join([f"- {r.get('title')}: {r.get('body')}" for r in results])
    except Exception:
        search_context = "Major universities adopt new AI integration policies while federal student loan restructuring dominates news."

    ai_extract = await call_groq_ai(
        client, 
        f"Web Search Context:\n{search_context}\n\nExtract 1 trending educational institution, policy name, or EdTech platform.\nReturn JSON: {{\"id\": \"DOE\", \"name\": \"Department of Education\"}}"
    )
    
    raw_name = safe_get_str(ai_extract, "name", "Department of Education")
    edu_id = safe_get_str(ai_extract, "id", "DOE")
    
    enriched = await ai_enrich_entity(edu_id, raw_name, client, default_domain="ed.gov")
    clean_name = enriched["cleanName"]
    domain = enriched["domain"]
    
    news_task = fetch_duckduckgo_news(clean_name, "EDU", max_results=NEWS_ARTICLE_COUNT)
    hero_photo_task = fetch_entity_hero_photo(clean_name, "EDU")
    
    news_data, hero_photo = await asyncio.gather(news_task, hero_photo_task)
    
    prompt = (
        f"Education Subject: {clean_name}\nArticles: {json.dumps(news_data)}\n"
        f"Task:\n"
        f"1. Summarize the educational development or policy shift in 1 sentence.\n"
        f"2. Identify the primary affected demographic (e.g., 'K-12 Students', 'Undergraduates', 'Faculty').\n"
        f"3. Determine the adoption/policy trend (e.g., 'Rapid Integration', 'Controversial Rollout', 'Funding Secured').\n"
        f"4. Estimate a systemic impact score (0-100 integer).\n"
        f"Return JSON: {{\"remark\": \"...\", \"demographic\": \"...\", \"trend\": \"...\", \"impactScore\": 85}}"
    )
    insights = await call_groq_ai(client, prompt)
    
    return {
        "category": "Education",
        "entity": clean_name,
        "brandLogo": f"https://img.logo.dev/{domain}?token={next(LOGO_DEV_KEYS)}",
        "heroPhoto": hero_photo,
        "insight": safe_get_str(insights, "remark", "Educational policy and infrastructure shifts ongoing."),
        "policyImpact": {
            "demographic": safe_get_str(insights, "demographic", "General Student Body"),
            "trend": safe_get_str(insights, "trend", "Emerging framework"),
            "systemicScore": insights.get("impactScore", 50)
        },
        "newsFeed": news_data
    }

# ==========================================
# 5. MASTER ORCHESTRATOR & CLI EXECUTION
# ==========================================

async def main():
    timestamp_utc = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    print(f"[DEBUG] [SYSTEM] Starting 5-Node Multi-Domain GNI Engine at {timestamp_utc}...")
    
    limits = httpx.Limits(max_keepalive_connections=30, max_connections=60)
    async with httpx.AsyncClient(limits=limits, timeout=35.0) as client:
        
        results = await asyncio.gather(
            process_finance_node(client), 
            process_sports_node(client), 
            process_research_node(client), 
            process_medicine_node(client), 
            process_education_node(client),
            return_exceptions=True  # Guarantees partial failures won't break the whole upload
        )
        
        # Filter out failed nodes if any exception occurred
        valid_modules = [res for res in results if isinstance(res, dict)]
        
        master_payload = {
            "timestamp": timestamp_utc,
            "engine": "Cloudflare gpt-oss-120b + 5-Node GNI Indexer",
            "modules": valid_modules
        }
        
        # Upload directly to S3 Bucket
        upload_success = upload_payload_to_e2(master_payload)
        
        if upload_success:
            print("[DEBUG] [SYSTEM] Pipeline complete. Terminating process.")
            sys.exit(0)
        else:
            print("[DEBUG] [SYSTEM] Process complete with storage warnings.")
            sys.exit(1)

if __name__ == "__main__":
    asyncio.run(main())