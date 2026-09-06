import os
import asyncio
import httpx
import random
import urllib.parse
import re
from datetime import datetime
from ddgs import DDGS
from dotenv import load_dotenv
import json
from datetime import timedelta
import time
# ==========================================
# SYSTEM INITIALIZATION & 110+ KEY VAULT
# ==========================================
load_dotenv()

def load_keys_from_env(prefix, max_range=50):
    """Dynamically loads keys like TAVILY_KEY_1, NEWSDATAIO_KEY_1, etc."""
    keys = []
    for i in range(1, max_range + 1):
        key = os.getenv(f"{prefix}_KEY_{i}")
        if key:
            keys.append(key)
    return keys

# The Ultimate Intelligence Vault
KEY_POOLS = {
    "TAVILY": load_keys_from_env("TAVILY"),
    "EXA": load_keys_from_env("EXA"),
    "SERP": load_keys_from_env("SERP"),
    "FIRECRAWL": load_keys_from_env("FIRECRAWL"),
    "GNEWS": load_keys_from_env("GNEWS"),
    "NEWSDATAIO": load_keys_from_env("NEWSDATAIO"),
    "CURRENT": load_keys_from_env("CURRENT"),
    "NEWSAPI": load_keys_from_env("NEWSAPI"),
    "SCRAPINGANT": load_keys_from_env("SCRAPINGANT"),
    "WEBSCRAPING": load_keys_from_env("WEBSCRAPING"),
    "SCRAPERAPI": load_keys_from_env("SCRAPERAPI"),
    "APIFY": load_keys_from_env("APIFY"),
    "ABSTRACT": load_keys_from_env("ABSTRACT"),
    "SCRAPEOPS": load_keys_from_env("SCRAPEOPS")
}

class EasyDorkerCustom:
    def __init__(self):
        self.indices = {k: 0 for k in KEY_POOLS.keys()}
        
        # Stateful Key Safety System
        # Tracks epoch timestamp when a specific key is allowed to be used again
        self.key_cooldowns = {provider: [0.0] * len(keys) for provider, keys in KEY_POOLS.items()}
        
        # Enterprise Connection Pooling
        limits = httpx.Limits(max_connections=300, max_keepalive_connections=100)
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"
        }
        self.client = httpx.AsyncClient(timeout=25.0, follow_redirects=True, headers=self.headers, limits=limits)
        
        # Master Throttle
        self.semaphore = asyncio.Semaphore(50)

    def _get_key(self, provider):
        """Stateful Rate-Out Safe Key Balancer. Spreads burden across all available keys."""
        pool = KEY_POOLS.get(provider, [])
        if not pool: return None
        
        current_time = time.time()
        pool_size = len(pool)
        start_idx = self.indices[provider] % pool_size
        
        # Cycle through pool to find a clean, non-rate-limited key
        for offset in range(pool_size):
            candidate_idx = (start_idx + offset) % pool_size
            if current_time >= self.key_cooldowns[provider][candidate_idx]:
                # Advance rotation pointer past this selection for next call
                self.indices[provider] = (candidate_idx + 1) % pool_size
                return pool[candidate_idx]
                
        # Fallback to absolute round-robin if every single key is cooling down
        key = pool[start_idx]
        self.indices[provider] = (start_idx + 1) % pool_size
        return key

    def _track_rateout(self, provider, key, backoff_seconds=300):
        """Call this whenever an API returns a 429 or auth credit failure to bench the key."""
        pool = KEY_POOLS.get(provider, [])
        if key in pool:
            idx = pool.index(key)
            self.key_cooldowns[provider][idx] = time.time() + backoff_seconds
            print(f" [!] Benched exhausted {provider} key at index [{idx}] for {backoff_seconds}s.")

    async def _fetch_serp(self, query, tbm=""):
        """Utilizes SERP keys for standard Google Search or Google News (tbm='nws')."""
        key = self._get_key("SERP")
        if not key: return []
        
        # FIX: Added required 'engine' parameter for SerpApi compatibility
        engine_type = "google_news" if tbm == "nws" else "google"
        url = f"https://serpapi.com/search.json?q={urllib.parse.quote(query)}&engine={engine_type}&api_key={key}"
        
        try:
            resp = await self.client.get(url)
            if resp.status_code == 200:
                data = resp.json()
                # Handle standard search vs news search layouts
                results = data.get('news_results', []) if tbm == "nws" else data.get('organic_results', [])
                return [{"source": "SERP_TITAN", "title": r['title'], "url": r['link']} for r in results[:10]]
            return []
        except Exception: return []

    async def _deploy_breakers(self, query):
        """
        TIER 3 HEAVY BREAKERS: Deploys all scraping proxies to bypass Cloudflare.
        Rotates through Abstract, ScrapingAnt, ScraperAPI, WebScraping, ScrapeOps.
        """
        target_url = urllib.parse.quote(f"https://html.duckduckgo.com/html/?q={urllib.parse.quote(query)}")
        
        # We try them in order of speed/reliability
        endpoints = [
            ("ABSTRACT", f"https://scrape.abstractapi.com/v1/?api_key={self._get_key('ABSTRACT')}&url={target_url}"),
            ("SCRAPINGANT", f"https://api.scrapingant.com/v2/general?url={target_url}&x-api-key={self._get_key('SCRAPINGANT')}"),
            ("SCRAPERAPI", f"http://api.scraperapi.com?api_key={self._get_key('SCRAPERAPI')}&url={target_url}"),
            ("WEBSCRAPING", f"https://api.webscraping.ai/html?api_key={self._get_key('WEBSCRAPING')}&url={target_url}"),
            ("SCRAPEOPS", f"https://proxy.scrapeops.io/v1/?api_key={self._get_key('SCRAPEOPS')}&url={target_url}")
        ]

        from bs4 import BeautifulSoup
        for provider, endpoint_url in endpoints:
            if "None" in endpoint_url: continue # Skip if no key
            try:
                resp = await self.client.get(endpoint_url, timeout=15)
                if resp.status_code == 200 and "result__a" in resp.text:
                    soup = BeautifulSoup(resp.text, 'html.parser')
                    results = []
                    for a_tag in soup.find_all('a', class_='result__a', limit=3):
                        url = urllib.parse.unquote(a_tag['href'].split('uddg=')[1].split('&')[0]) if 'uddg=' in a_tag['href'] else a_tag['href']
                        results.append({"source": f"{provider}_BREAKER", "title": a_tag.get_text(strip=True), "url": url})
                    if results: return results
            except Exception: pass
        return []

    async def fetch_deep_intel(self, url):
        """
        V12.0 HEAVY EXTRACTOR MESH
        Phase 1: Firecrawl (Markdown Optimized)
        Phase 2: Apify Website Content Crawler (JS-Heavy Fallback)
        """
        fc_key = self._get_key("FIRECRAWL")
        ap_key = self._get_key("APIFY")
        
        # --- TIER 1: FIRECRAWL (Markdown for LLMs) ---
        if fc_key:
            try:
                print(f"[*] [TIER 1] Attempting Firecrawl Extraction: {url[:50]}...")
                resp = await self.client.post(
                    "https://api.firecrawl.dev/v0/scrape", 
                    headers={"Authorization": f"Bearer {fc_key}"}, 
                    json={"url": url, "pageOptions": {"onlyMainContent": True}},
                    timeout=30.0
                )
                if resp.status_code == 200:
                    data = resp.json().get('data', {})
                    if data.get('markdown'):
                        return {
                            "source": "FIRECRAWL_MESH", 
                            "content": data.get('markdown'),
                            "metadata": data.get('metadata', {})
                        }
            except Exception as e:
                print(f"[!] Firecrawl failed: {e}")

        # --- TIER 2: APIFY (Ultimate JS-Heavy Fallback) ---
        if ap_key:
            try:
                print(f"[*] [TIER 2] Deploying Apify Crawler for {url[:50]}...")
                # Using the Website Content Crawler (Cheerio or Playwright based)
                # This call starts the actor and waits for completion (synchronous-wait style)
                run_url = f"https://api.apify.com/v2/acts/apify~website-content-crawler/run-sync-get-dataset-items?token={ap_key}"
                
                payload = {
                    "startUrls": [{"url": url}],
                    "maxCrawlPages": 1,
                    "proxyConfiguration": {"useApifyProxy": True}
                }

                resp = await self.client.post(run_url, json=payload, timeout=60.0)
                
                if resp.status_code == 200 or resp.status_code == 201:
                    items = resp.json()
                    if items and len(items) > 0:
                        # Extract the text or markdown from the first page result
                        content = items[0].get('markdown') or items[0].get('text') or items[0].get('description')
                        return {
                            "source": "APIFY_TITAN",
                            "content": content,
                            "url": url
                        }
            except Exception as e:
                print(f"[!] Apify Mesh failure: {e}")

        return None

    async def _fetch_ddg(self, query, start_date=None, end_date=None, max_res=5):
        """
        UPDATED: V13.8 OSINT-Ready DDG Scraper
        - Fixes list comprehension crash.
        - Injects 'after:'/'before:' dorks for historical lookback.
        - Captures snippet (body) and date for downstream processing.
        """
        # Inject temporal dorks if dates are provided
        search_query = query
        if start_date and end_date:
            search_query = f"{query} after:{start_date} before:{end_date}"

        try:
            def sync_ddg():
                with DDGS() as ddgs:
                    # Using text search; results include 'title', 'href', 'body', 'date'
                    return list(ddgs.text(search_query, max_results=max_res))
            
            raw_data = await asyncio.to_thread(sync_ddg)
            
            # Properly map the entire list of results
            return [
                {
                    "source": "DDG_GHOST", 
                    "title": item.get('title'), 
                    "url": item.get('href'),
                    "body": item.get('body'), # Critical for LLM processing
                    "date": item.get('date')   # Often returns metadata if available
                } 
                for item in raw_data
            ]
        except Exception as e:
            # Silent fail for the pipeline to continue
            return []

    async def _fetch_tavily(self, query):
        key = self._get_key("TAVILY")
        if not key: return []
        try:
            resp = await self.client.post("https://api.tavily.com/search", json={"api_key": key, "query": query, "topic": "news"})
            return [{"source": "TAVILY_NEURAL", "title": r['title'], "url": r['url']} for r in resp.json().get('results', [])]
        except Exception: return []

    
    async def _fetch_exa(self, query, platform=None, start_date=None, end_date=None):
        key = self._get_key("EXA")
        if not key: return []
        
        # FIX: Changed useAutoprompt to False when explicit temporal frames or platforms are active
        payload = {"query": query, "numResults": 15, "useAutoprompt": False}
        
        if platform == "X": payload["includeDomains"] = ["x.com", "twitter.com"]
        elif platform == "TIKTOK": payload["includeDomains"] = ["tiktok.com"]
        
        if start_date: payload["startPublishedDate"] = f"{start_date}T00:00:00.000Z"
        if end_date: payload["endPublishedDate"] = f"{end_date}T23:59:59.000Z"

        try:
            resp = await self.client.post("https://api.exa.ai/search", headers={"x-api-key": key, "Content-Type": "application/json"}, json=payload)
            src = f"EXA_{platform}" if platform else "EXA_NEURAL"
            return [{"source": src, "title": r['title'], "url": r['url'], "date": r.get('publishedDate')} for r in resp.json().get('results', [])]
        except Exception: return []

    async def _fetch_gnews(self, query, start_date=None, end_date=None):
        key = self._get_key("GNEWS")
        if not key: return []
        url = f"https://gnews.io/api/v4/search?q={urllib.parse.quote(query)}&lang=en&max=5&apikey={key}"
        if start_date: url += f"&from={start_date}T00:00:00Z"
        if end_date: url += f"&to={end_date}T23:59:59Z"
        try:
            resp = await self.client.get(url)
            return [{"source": "GNEWS", "title": r['title'], "url": r['url'], "date": r.get('publishedAt')} for r in resp.json().get('articles', [])]
        except Exception: return []

    async def _fetch_newsdataio(self, query):
        key = self._get_key("NEWSDATAIO")
        if not key: return []
        try:
            url = f"https://newsdata.io/api/1/news?apikey={key}&q={urllib.parse.quote(query)}&language=en"
            resp = await self.client.get(url)
            return [{"source": "NEWSDATA", "title": r['title'], "url": r['link']} for r in resp.json().get('results', [])]
        except Exception: return []

    async def _fetch_currents(self, query):
        key = self._get_key("CURRENT")
        if not key: return []
        try:
            url = f"https://api.currentsapi.services/v1/search?apiKey={key}&keywords={urllib.parse.quote(query)}&language=en"
            resp = await self.client.get(url)
            return [{"source": "CURRENTS", "title": r['title'], "url": r['url']} for r in resp.json().get('news', [])]
        except Exception: return []

    async def _fetch_newsapi(self, query, start_date=None, end_date=None):
        key = self._get_key("NEWSAPI")
        if not key: return []
        url = f"https://newsapi.org/v2/everything?q={urllib.parse.quote(query)}&apiKey={key}&language=en"
        if start_date: url += f"&from={start_date}"
        if end_date: url += f"&to={end_date}"
        try:
            resp = await self.client.get(url)
            return [{"source": "NEWSAPI", "title": r['title'], "url": r['url'], "date": r.get('publishedAt')} for r in resp.json().get('articles', [])]
        except Exception: return []

    def _extract_fallback_date(self, url, snippet=""):
        """
        V13.2: Extracts hidden date footprints from URLs and strings.
        """
        # 1. URL Path Mapping (e.g. /2026/05/11/)
        url_match = re.search(r'/(20[1-2][0-9])[-/](0[1-9]|1[0-2])[-/](0[1-9]|[12][0-9]|3[0-1])/?', url)
        if url_match:
            return f"{url_match.group(1)}-{url_match.group(2)}-{url_match.group(3)}"
        
        # 2. Raw Timestamp Regex in strings
        snippet_match = re.search(r'\b(20[1-2][0-9])-(0[1-9]|1[0-2])-(0[1-9]|[12][0-9]|3[0-1])\b', snippet)
        if snippet_match:
            return snippet_match.group(0)
        
        # 3. Dynamic "Days Ago" conversion using real-time
        ago_match = re.search(r'(\d+)\s+(day|days)\s+ago', snippet.lower())
        if ago_match:
            from datetime import datetime, timedelta
            days_ago = int(ago_match.group(1))
            past_date = datetime.now() - timedelta(days=days_ago)
            return past_date.strftime("%Y-%m-%d")
            
        return "Unknown"

    def _harden_query(self, query):
        """Forces literal matching upstream but strips conversational noise."""
        stop_words = {"the", "a", "an", "is", "news", "latest", "update", "about"}
        clean_terms = [t for t in query.split() if t.lower() not in stop_words]
        
        # If no quotes exist, lock the core entities into a phrase
        if len(clean_terms) > 1 and not any('"' in t for t in query):
            return f'"{ " ".join(clean_terms) }"'
        return query

    def _enforce_precision(self, query, results):
        """
        V12.92 ADVANCED PRECISION LOCK:
        - Destroys API fallback noise (unrelated crypto, market tickers).
        - Enforces literal multi-word phrase proximity checks.
        """
        if not results: return []
        
        q_lower = query.lower()
        stop_words = {"the", "a", "an", "is", "in", "on", "and", "or", "of", "to", "for", "with", "news", "latest", "today"}
        query_terms = [w.lower() for w in re.findall(r'\w+', query) if len(w) > 2 and w.lower() not in stop_words]
        
        if not query_terms: return results
        
        # Detect explicit quoted phrases or sub-string combinations
        phrase_matches = [p.strip() for p in re.findall(r'["\'](.*?)["\']', q_lower)]
        if not phrase_matches and len(query_terms) > 1:
            # Construct a clean, unified phrase string if no quotes exist
            phrase_matches = [" ".join(query_terms)]

        precise_results = []
        for item in results:
            title = item.get('title', '') or ''
            url = item.get('url', '') or ''
            text_to_check = f"{title} {url}".lower()
            
            # 1. ANTI-FALLBACK GATE: Intercept drifting trending noise (stocks, tickers, crypto)
            # Instantly drop if noise terms appear but were not requested by the operator
            fallback_spam = ["crypto", "bitcoin", "ethereum", "stocks", "nasdaq", "dow jones", "wall street", "s&p 500", "shares hit"]
            has_spam = any(spam in text_to_check for spam in fallback_spam)
            wants_spam = any(spam in q_lower for spam in fallback_spam)
            if has_spam and not wants_spam:
                continue 
                
            # 2. PHRASE PROXIMITY EXTRACTION
            phrase_verified = False
            if phrase_matches:
                for phrase in phrase_matches:
                    if phrase in text_to_check:
                        phrase_verified = True
                        break
            
            # Count baseline keyword token frequency matches
            match_count = sum(1 for term in query_terms if term in text_to_check)
            required_matches = max(1, len(query_terms) // 2)
            
            # Keep if the article passes exact phrase matching OR matches keyword token count thresholds
            if phrase_verified or match_count >= required_matches:
                precise_results.append(item)
                
        return precise_results

    # ==========================================
    # V13.0 PRECISION CORE: ADAPTIVE & CONSENSUS MATRICES
    # ==========================================
    def _init_weights(self):
        """Initializes the self-learning precision weights file."""
        self.weights_file = "precision_weights.json"
        if not os.path.exists(self.weights_file):
            with open(self.weights_file, 'w') as f:
                json.dump({"nav": 1.2, "carrier": 1.5, "strike": 1.3}, f)
        with open(self.weights_file, 'r') as f:
            self.dynamic_weights = json.load(f)

    def _update_weights(self, query):
        """Slowly trains the engine to prefer successful query terms."""
        
        # ADD THIS LINE: Ensure weights are loaded before updating
        if not hasattr(self, 'dynamic_weights'): self._init_weights()
        
        terms = self._clean_tokens(query)
        for term in terms:
            self.dynamic_weights[term] = self.dynamic_weights.get(term, 1.0) + 0.05
        with open(self.weights_file, 'w') as f:
            json.dump(self.dynamic_weights, f)

    def _clean_tokens(self, text):
        stop_words = {"the", "a", "an", "is", "in", "on", "and", "or", "of", "to", "for", "with", "news", "latest"}
        return [w.lower() for w in re.findall(r'\w+', text) if len(w) >= 3 and w.lower() not in stop_words]


    def _score_precision(self, query, title, url=""):
        """Calculates Density + Jaccard Overlap + Learned Weights."""
        if not hasattr(self, 'dynamic_weights'): self._init_weights()
        
        target_text = f"{title} {url}".lower()
        q_tokens = self._clean_tokens(query)
        t_tokens = set(self._clean_tokens(target_text))
        
        if not q_tokens: return 1.0
        
        # Calculate Overlap with Dynamic Weights
        intersection_weight = sum(self.dynamic_weights.get(t, 1.0) for t in q_tokens if t in t_tokens)
        max_possible_weight = sum(self.dynamic_weights.get(t, 1.0) for t in q_tokens)
        
        jaccard_score = intersection_weight / max_possible_weight if max_possible_weight else 0
        
        # Information Density Penalty (Destroys spam/crypto aggregators)
        density_multiplier = 1.0
        if len(t_tokens) > 0:
            density = sum(1 for t in q_tokens if t in t_tokens) / len(t_tokens)
            density_multiplier = min(1.0, density * 5.0 + 0.1)
            
        return jaccard_score * density_multiplier

    # ==========================================
    # PINNACLE FUNCTION 1: fetch_news (ANTI-STRANGLE UPGRADE)
    # ==========================================
    async def fetch_news(self, query):
        """
        V13.2: HIGH-YIELD ACCUMULATOR
        - Fixes the 'Blank Return' bug by softening the literal quote requirement 
          while maintaining Jaccard density ranking.
        """
        async with self.semaphore:
            # Softer Hardening: Only quote if it's explicitly a 2-word strict entity.
            # Queries like "Geopolitical Tensions 2026" remain unquoted to prevent 0-yield APIs.
            clean_terms = [t for t in query.split() if len(t) > 3]
            if len(clean_terms) == 2 and not any('"' in t for t in query):
                hardened_query = f'"{query}"'
            else:
                hardened_query = query
                
            TARGET_YIELD = 5
            
            engines = [
                self._fetch_gnews,
                self._fetch_newsdataio,
                self._fetch_currents,
                self._fetch_newsapi,
                self._fetch_tavily,
                self._fetch_ddg
            ]
            random.shuffle(engines)
            
            master_intel = {}
            
            for engine in engines:
                try:
                    res_list = await engine(hardened_query)
                    
                    # Loosened restriction: Allow items to pass if they hit precision metrics
                    if res_list:
                        for item in res_list:
                            url = item.get('url')
                            if url and url not in master_intel:
                                score = self._score_precision(query, item.get('title', ''), url)
                                # Lowered absolute threshold to 0.15 to prevent blank screens, 
                                # but the ranking pushes the 0.99 items to the top anyway.
                                if score >= 0.15: 
                                    item['precision_score'] = score
                                    master_intel[url] = item
                        
                        if len(master_intel) >= TARGET_YIELD:
                            break
                            
                except Exception:
                    continue
            
            ranked_list = sorted(master_intel.values(), key=lambda x: x.get('precision_score', 0), reverse=True)
            
            if ranked_list and hasattr(self, '_update_weights'):
                self._update_weights(query)
                
            return ranked_list

    # ==========================================
    # PINNACLE FUNCTION 2: fetch_social_news (BUG-FIXED)
    # ==========================================
    async def fetch_social_news(self, platform, query):
        """
        V13.2: SOCIAL ACCUMULATOR + DOMAIN STRICTNESS
        - Fixed the check_auth Firecrawl execution bug.
        """
        async with self.semaphore:
            clean_terms = [t for t in query.split() if len(t) > 3]
            hardened_query = f'"{query}"' if (len(clean_terms) == 2 and not '"' in query) else query
            
            dork_query = f"site:{platform.lower()}.com {hardened_query}"
            TARGET_YIELD = 4
            
            engines = [
                lambda q: self._fetch_exa(q, platform=platform),
                lambda q: self._fetch_serp(dork_query),
                lambda q: self._fetch_ddg(dork_query)
            ]
            random.shuffle(engines)
            
            master_social = {}
            
            for engine_call in engines:
                try:
                    res_list = await engine_call(hardened_query)
                    
                    if res_list:
                        for item in res_list:
                            url = item.get('url', '').lower()
                            if platform.lower() in url and url not in master_social:
                                score = self._score_precision(query, item.get('title', ''), url)
                                if score >= 0.15:
                                    item['embed_ready'] = True
                                    item['precision_score'] = score
                                    master_social[item['url']] = item
                                
                        if len(master_social) >= TARGET_YIELD:
                            break
                except Exception:
                    continue

            if len(master_social) < 3:
                breaker_hits = await self._deploy_breakers(dork_query)
                if breaker_hits:
                    for item in breaker_hits:
                        url = item.get('url', '').lower()
                        if platform.lower() in url and url not in master_social:
                            item['precision_score'] = self._score_precision(query, item.get('title', ''), url)
                            master_social[item['url']] = item
            
            # REMOVED the broken check_auth line here.
            
            ranked_social = sorted(master_social.values(), key=lambda x: x.get('precision_score', 0), reverse=True)
            if ranked_social and hasattr(self, '_update_weights'):
                self._update_weights(query)
                
            return ranked_social

    async def fetch_news_timeline(self, query, start_date, end_date, chunk_days=7, max_parallel_chunks=4):
        """
        V14.0: HIGH-VELOCITY PARALLEL CHRONOLOGICAL SWEEP
        Pre-allocates discrete non-overlapping weekly blocks and executes them 
        concurrently across an internal multi-lane worker pool.
        """
        async with self.semaphore:
            print(f"[*] Deploying Parallel Time-Cutter Engine [{start_date} to {end_date}] -> {query[:40]}")
            
            # Clean and harden incoming query terms
            stop_words = {"the", "a", "an", "is", "news", "latest", "update", "war"}
            clean_terms = [t for t in query.split() if t.lower() not in stop_words]
            hardened_query = query if any('"' in t for t in query) else " ".join(clean_terms)
            
            current_start = datetime.strptime(start_date, "%Y-%m-%d")
            final_end = datetime.strptime(end_date, "%Y-%m-%d")
            
            # STEP 1: Pre-calculate all distinct, non-overlapping time chunks upfront
            date_chunks = []
            while current_start <= final_end:
                current_end = current_start + timedelta(days=chunk_days - 1)
                if current_end > final_end: 
                    current_end = final_end
                    
                date_chunks.append((current_start.strftime("%Y-%m-%d"), current_end.strftime("%Y-%m-%d")))
                current_start = current_end + timedelta(days=1)
            
            print(f"[*] Segmented timeline into {len(date_chunks)} total individual target weeks.")
            
            # STEP 2: Establish a local semaphore to limit active concurrent week queries
            # This implements your logic of running exactly N weeks (e.g., 4) simultaneously 
            chunk_semaphore = asyncio.Semaphore(max_parallel_chunks)
            master_timeline = {}
            
            # STEP 3: Define an isolated worker routine to process an assigned week
            async def process_single_week_chunk(str_start, str_end):
                async with chunk_semaphore:
                    print(f"  [>] Worker Lane Active: Harvesting Chunk {str_start} to {str_end}...")
                    
                    # Concurrently dispatch all 4 endpoint sweeps for THIS specific week
                    results = await asyncio.gather(
                        self._fetch_exa(hardened_query, start_date=str_start, end_date=str_end),
                        self._fetch_newsapi(hardened_query, start_date=str_start, end_date=str_end),
                        self._fetch_gnews(hardened_query, start_date=str_start, end_date=str_end),
                        self._fetch_ddg(hardened_query, start_date=str_start, end_date=str_end, max_res=5),
                        return_exceptions=True
                    )
                    
                    local_processed_items = []
                    query_tokens = [w.lower() for w in clean_terms if len(w) > 2]
                    
                    # Parse entries compiled by the 4 sub-tasks for this week
                    for res_list in results:
                        if isinstance(res_list, list) and res_list:
                            for item in res_list:
                                url = item.get('url')
                                if url:
                                    title_lower = (item.get('title', '') or '').lower()
                                    
                                    # Apply relevance filters
                                    if any(token in title_lower or token in url.lower() for token in query_tokens):
                                        raw_date = item.get('date')
                                        if not raw_date or raw_date == "Unknown":
                                            raw_date = self._extract_fallback_date(url, item.get('title', ''))
                                            
                                        # Clone and update metadata dictionary elements cleanly
                                        processed_item = item.copy()
                                        processed_item['date'] = str(raw_date)[:10] if raw_date else "Unknown"
                                        processed_item['precision_score'] = self._score_precision(query, item.get('title', ''), url)
                                        local_processed_items.append(processed_item)
                    
                    # Micro-pacing delay inside the lane to respect endpoint connection channels
                    await asyncio.sleep(0.4)
                    return local_processed_items

            # STEP 4: Fire all worker tasks into the asyncio event loop simultaneously
            tasks = [process_single_week_chunk(s, e) for s, e in date_chunks]
            all_chunks_results = await asyncio.gather(*tasks)
            
            # STEP 5: Merge isolated local results into the global dictionary to strip duplicates
            for chunk_list in all_chunks_results:
                for item in chunk_list:
                    url = item.get('url')
                    if url and url not in master_timeline:
                        master_timeline[url] = item
                        
            timeline_list = list(master_timeline.values())
            
            # Final chronological and scoring sort
            try:
                timeline_list.sort(key=lambda x: (x.get('date', '0000-00-00'), x.get('precision_score', 0)), reverse=True)
            except Exception:
                pass

            if timeline_list and hasattr(self, '_update_weights'):
                self._update_weights(query)

            return timeline_list

    async def fetch_latest_snapshot(self, administrative_name: str, country: str, max_results: int = 5) -> list:
        """
        V16.5 REAL-TIME TACTICAL SNAPSHOT ENGINE (ENGINE-LEVEL SHORT-CIRCUIT)
        Bypasses temporal constraints to gather the absolute latest indexed entries.
        Optimized to process engines concurrently and terminate pending connections
        the exact moment the requested 'max_results' quota is satisfied.
        """
        from difflib import SequenceMatcher
        import asyncio
        
        async with self.semaphore:
            # Programmatic query hardening with noise filters
            refined_query = f'"{administrative_name}" {country} (conflict OR fighting OR military OR clash) -travel -booking'
            
            print(f"[*] [🛰️ FUZZY SNAPSHOT LAUNCHED] Target: {administrative_name}, {country} (Target Quota: {max_results})")
            
            # 1. Wrap engine invocations into explicit asynchronous Tasks
            tasks = [
                asyncio.create_task(self._fetch_ddg(refined_query, max_res=max_results)),
                asyncio.create_task(self._fetch_tavily(refined_query)),
                asyncio.create_task(self._fetch_exa(refined_query)),
                asyncio.create_task(self._deploy_breakers(refined_query)),
                asyncio.create_task(self._fetch_gnews(refined_query)),
                asyncio.create_task(self._fetch_currents(refined_query))
            ]
            
            master_feed = {}
            admin_target = administrative_name.lower().strip()
            admin_word_count = len(admin_target.split())
            
            # 2. Process engines dynamically as they finish
            while tasks and len(master_feed) < max_results:
                # Wait for whichever search engines respond first
                done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
                tasks = list(pending)  # Update outstanding task array
                
                for completed_task in done:
                    try:
                        res_list = await completed_task
                        if isinstance(res_list, list) and res_list:
                            for item in res_list:
                                url = item.get('url')
                                if url and url not in master_feed:
                                    # Normalize document layout fields
                                    title_text = item.get('title', '') or ''
                                    body_text = item.get('body', '') or ''
                                    
                                    if not body_text:
                                        item['body'] = title_text
                                        body_text = title_text
                                        
                                    combined_text = f"{title_text} {body_text}".lower()
                                    
                                    # ---- NATIVE INLINE FUZZY STRING MATCHING ----
                                    fuzzy_score = 0.0
                                    if admin_target in combined_text:
                                        fuzzy_score = 1.0
                                    else:
                                        text_words = combined_text.split()
                                        if text_words and admin_word_count > 0:
                                            # Match multi-word name phrases using sliding token matrix
                                            for i in range(len(text_words) - admin_word_count + 1):
                                                window_phrase = " ".join(text_words[i:i+admin_word_count])
                                                ratio = SequenceMatcher(None, admin_target, window_phrase).ratio()
                                                if ratio > fuzzy_score:
                                                    fuzzy_score = ratio
                                                    
                                            # Final single word token comparison pass for typos
                                            for word in text_words:
                                                ratio = SequenceMatcher(None, admin_target, word).ratio()
                                                if ratio > fuzzy_score:
                                                    fuzzy_score = ratio

                                    # Only register item if fuzzy match tier is satisfied
                                    if fuzzy_score >= 0.65:
                                        item['fuzzy_match_score'] = fuzzy_score
                                        master_feed[url] = item
                                        
                                    # Break internal loop early if this engine pushes us past our quota
                                    if len(master_feed) >= max_results:
                                        break
                                        
                    except Exception as e:
                        # Fault isolation: an individual engine failure must not kill the sweep
                        print(f"[⚠️] Engine execution anomaly handled gracefully: {e}")
            
            # 3. Quota Short-Circuit Trigger
            if tasks:
                print(f"   ↳ [⚡ ENGINE SHORT-CIRCUIT LOCK] Target quota met ({len(master_feed)}/{max_results}). Canceling {len(tasks)} pending engines early.")
                for pending_task in tasks:
                    pending_task.cancel()
            
            # Final chronological/precision sorting pass on the collected sample size
            ranked_feed = sorted(master_feed.values(), key=lambda x: x.get('fuzzy_match_score', 0), reverse=True)
            
            if ranked_feed and hasattr(self, '_update_weights'):
                self._update_weights(f"{administrative_name} {country}")
                
            return ranked_feed[:max_results]
            
    async def test_engine(self):
        #"""Runs a validation check on the 3 Pinnacle Functions."""
        #print(f"\n[🚀] VISIONSPHERE PINNACLE V12.0 ONLINE")
        
        #print("\n--- 1. Testing News ---")
        #news = await self.fetch_news("aircraft carrier position")
        #for n in news: print(f"[{n['source']}] {n['title']} \n🔗 {n['url']}")

        #print("\n--- 2. Testing Social (X) ---")
        #social = await self.fetch_social_news("X", "Airstrike footage")
        #for s in social: print(f"[{s['source']}] {s['title']} \n🔗 {s['url']}")
        
        print("\n--- 3. Testing Timeline ---")
        timeline = await self.fetch_news_timeline(query="Russia Ukraine war", start_date="2026-04-01", end_date="2026-05-15")
        for t in timeline: print(f"[{t['source']} | {t.get('date', 'N/A')}] {t['title']} \n🔗 {t['url']}")

        await self.client.aclose()

if __name__ == "__main__":
    if os.name == 'nt':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(EasyDorkerCustom().test_engine())