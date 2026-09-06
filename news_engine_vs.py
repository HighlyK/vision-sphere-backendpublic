# 🟢 DROP-IN: Top Imports
import asyncio
import httpx
import feedparser
import listparser
import json
import urllib.parse
import os
import random
from bs4 import BeautifulSoup
from datetime import datetime, timezone
import time
import logging
import re
import libsql_client
from dotenv import load_dotenv
import itertools
from functools import partial
from ddgs import DDGS
from EasyDorkerCustom import EasyDorkerCustom
INTEL_PROFILES = {
    "universal_horizon": {
        "name": "Universal Horizon Monitor",
        "goal": "global shifts including economic volatility, geopolitical conflict, social uprisings, and cultural trends",
        "tags": "market crash, airstrike, protest, riot, championship, fashion week, tech launch, breakthrough",
        "vibe": "all-encompassing, high-velocity, and diverse"
    }
}
load_dotenv()
# ==========================================
# 0. CONFIGURATION & THROTTLES
# ==========================================
MAX_CONCURRENT_TASKS_LIMIT = 10 
# Create the ACTUAL context manager
SEMAPHORE = asyncio.Semaphore(MAX_CONCURRENT_TASKS_LIMIT)
OSM_RATE_LIMIT_LOCK = asyncio.Lock()
class HydraLLMManager:
    def __init__(self):
        self.endpoints = []
        # 1. Load Gemini Keys (1-10) - OpenAI-Compatible Endpoint
        for i in range(1, 11):
            key = os.getenv(f"GEMINI_KEY_{i}")
            if key:
                self.endpoints.append({
                    "provider": "GEMINI_OPENAI",
                    "base_url": "https://generativelanguage.googleapis.com/v1beta/openai",
                    "model": "gemini-3.1-flash-lite", 
                    "key": key
                })

        # 2. Load Cerebras Keys (1-5)
        for i in range(1, 6):
            key = os.getenv(f"CERABRAS_KEY_{i}")
            if key:
                self.endpoints.append({
                    "provider": "CEREBRAS",
                    "base_url": "https://api.cerebras.ai/v1",
                    "model": "gpt-oss-120b",
                    "key": key
                })
                
        # 3. Load Mistral Keys (1-2)
        for i in range(1, 3):
            key = os.getenv(f"MISTRAL_KEY_{i}")
            if key:
                self.endpoints.append({
                    "provider": "MISTRAL",
                    "base_url": "https://api.mistral.ai/v1",
                    "model": "mistral-small-latest",
                    "key": key
                })
                
        # Create an infinite round-robin loop across all loaded keys
        if not self.endpoints:
            print("[🚨] HYDRA_WARNING: No LLM keys found in environment!")
            self.cycle = None
        else:
            print(f"[✅] HYDRA_SYNC: {len(self.endpoints)} API endpoints synchronized.")
            self.cycle = itertools.cycle(self.endpoints)

# Initialize the manager
HYDRA_MANAGER = HydraLLMManager()
# Filter out empty strings/Nones and create an infinite rotation loop
# 🦅 THE 36 PREMIUM INTELLIGENCE DOMAINS (Tactical, Defense, Humanitarian, Cyber)

# The Master 200+ Country List
ALL_NATIONS = [
    "Afghanistan", "Albania", "Algeria", "Andorra", "Angola", "Antigua and Barbuda", "Argentina", "Armenia", "Australia", "Austria", "Azerbaijan",
    "Bahamas", "Bahrain", "Bangladesh", "Barbados", "Belarus", "Belgium", "Belize", "Benin", "Bhutan", "Bolivia", "Bosnia and Herzegovina", "Botswana", "Brazil", "Brunei", "Bulgaria", "Burkina Faso", "Burundi",
    "Cabo Verde", "Cambodia", "Cameroon", "Canada", "Central African Republic", "Chad", "Chile", "China", "Colombia", "Comoros", "Congo", "Costa Rica", "Croatia", "Cuba", "Cyprus", "Czechia",
    "Denmark", "Djibouti", "Dominica", "Dominican Republic", "Ecuador", "Egypt", "El Salvador", "Equatorial Guinea", "Eritrea", "Estonia", "Eswatini", "Ethiopia",
    "Fiji", "Finland", "France", "Gabon", "Gambia", "Georgia", "Germany", "Ghana", "Greece", "Grenada", "Guatemala", "Guinea", "Guinea-Bissau", "Guyana", "Haiti", "Honduras", "Hungary",
    "Iceland", "India", "Indonesia", "Iran", "Iraq", "Ireland", "Israel", "Italy", "Jamaica", "Japan", "Jordan", "Kazakhstan", "Kenya", "Kiribati", "Kosovo", "Kuwait", "Kyrgyzstan",
    "Laos", "Latvia", "Lebanon", "Lesotho", "Liberia", "Libya", "Liechtenstein", "Lithuania", "Luxembourg", "Madagascar", "Malawi", "Malaysia", "Maldives", "Mali", "Malta", "Marshall Islands", "Mauritania", "Mauritius", "Mexico", "Micronesia", "Moldova", "Monaco", "Mongolia", "Montenegro", "Morocco", "Mozambique", "Myanmar",
    "Namibia", "Nauru", "Nepal", "Netherlands", "New Zealand", "Nicaragua", "Niger", "Nigeria", "North Korea", "North Macedonia", "Norway", "Oman", "Pakistan", "Palau", "Palestine", "Panama", "Papua New Guinea", "Paraguay", "Peru", "Philippines", "Poland", "Portugal", "Qatar",
    "Romania", "Russia", "Rwanda", "Saint Kitts and Nevis", "Saint Lucia", "Saint Vincent", "Samoa", "San Marino", "Sao Tome and Principe", "Saudi Arabia", "Senegal", "Serbia", "Seychelles", "Sierra Leone", "Singapore", "Slovakia", "Slovenia", "Solomon Islands", "Somalia", "South Africa", "South Korea", "South Sudan", "Spain", "Sri Lanka", "Sudan", "Suriname", "Sweden", "Switzerland", "Syria",
    "Taiwan", "Tajikistan", "Tanzania", "Thailand", "Timor-Leste", "Togo", "Tonga", "Trinidad and Tobago", "Tunisia", "Turkey", "Turkmenistan", "Tuvalu", "Uganda", "Ukraine", "United Arab Emirates", "United Kingdom", "United States", "Uruguay", "Uzbekistan", "Vanuatu", "Vatican City", "Venezuela", "Vietnam", "Yemen", "Zambia", "Zimbabwe"
]

class DINEI_MOTHER:
    def __init__(self):
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml",
            "Accept-Language": "en-US,en;q=0.5",
            "Connection": "keep-alive"
        }
        # V23 UPGRADE: Viewpoint Clustering Dictionary
        self.story_clusters = {}
        self.processed_urls = [] # To prevent duplicate stream output
        self.max_history = 1000 # Keep memory low for Render's free tier
        self.is_running = True

        self.dorker = EasyDorkerCustom()
    
    async def push_to_turso(self, item):
        """
        Stateless HTTP pipeline push to Turso Edge Database.
        Uses the URL as the unique primary key to prevent duplicates.
        """
        turso_url = os.getenv("TURSO_DATABASE_URL")
        turso_token = os.getenv("TURSO_AUTH_TOKEN")

        if not turso_url or not turso_token:
            return None

        raw_lat = item['location'].get('lat')
        raw_lon = item['location'].get('lon')
        lat = float(raw_lat) if raw_lat is not None else 0.0
        lon = float(raw_lon) if raw_lon is not None else 0.0
        
        intel_id = item['attribution']['url']

        try:
            # create_client over HTTP is stateless. It fires the data and closes instantly.
            async with libsql_client.create_client(url=turso_url, auth_token=turso_token) as t_client:
                await t_client.execute(
                    """
                    INSERT INTO intel_stream 
                    (id, title, intensity, context, source, location_name, latitude, longitude, video_url, photo_url, created_at) 
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                    ON CONFLICT(id) DO UPDATE SET 
                    title=excluded.title, intensity=excluded.intensity, context=excluded.context
                    """,
                    [
                        intel_id,
                        item['title'],
                        item['intensity'],
                        item['context'],
                        item['attribution']['source'],
                        item['location'].get('name', 'Global Newsroom'),
                        lat,
                        lon,
                        item.get('media', {}).get('video'),
                        item.get('media', {}).get('photo')
                    ]
                )
            return True
        except Exception as e:
            err_msg = str(e)
            if "UNIQUE constraint failed" not in err_msg:
                print(f"[!] TURSO_SYNC_FAIL: {err_msg[:80]}...")
            return None

    def track_viewpoint(self, raw_title, src, url):
        """
        Replaces 'deduplicate'. If a story exists, it appends the new source to the cluster
        so users can see multiple perspectives. Returns True if NEW, False if DUPLICATE.
        """
        clean = raw_title.strip().lower()
        if clean in self.story_clusters:
            # It's a duplicate. Save the alternative viewpoint, but don't re-trigger LLM.
            self.story_clusters[clean].append({"src": src, "url": url})
            return False 
        else:
            # It's new. Create the cluster array.
            self.story_clusters[clean] = [{"src": src, "url": url}]
            return True

    # 🟢 DROP-IN: Inside DINEI_MOTHER class
    async def cf_get(self, client, target_url, **kwargs):
        """Masks HTTPX requests through the Cloudflare Worker to bypass 403s/IP bans."""
        cf_worker_url = "https://shiny-union-766e.rmadris473.workers.dev/"
        if 'params' not in kwargs:
            kwargs['params'] = {}
        kwargs['params']['url'] = target_url
        
        try:
            return await client.get(cf_worker_url, **kwargs)
        except Exception:
            # Fallback to direct connection if CF fails
            kwargs['params'].pop('url')
            return await client.get(target_url, **kwargs)

    # ==========================================
    # 1. COGNITIVE PIPELINE
    # ==========================================
    def create_error_block(self, error_type, details, src="SYSTEM"):
        """Ensures system errors match the rich schema to prevent worker crashes."""
        return {
            "intensity": "SYS_ERR 🔴",
            "title": f"[{error_type}] {details}",
            "context": "System diagnostic: No tactical analysis required.",
            "attribution": {
                "source": src,
                "logo": "https://cdn-icons-png.flaticon.com/512/595/595568.png",
                "url": "INTERNAL_LOG"
            },
            "location": { "name": "SYSTEM", "lat": 0.0, "lon": 0.0 },
            "media": { "video": None, "photo": None },
            "timestamp": datetime.now(timezone.utc).isoformat()
        }

    async def extract_media(self, client, url):
        """Extracts playable video embeds and photo previews."""
        assets = {"video": None, "photo": None}
        target_url = url

        # 🚀 STAGE 1: RESOLUTION & EXTRACTION (Google Decoder entirely removed)
        try:
            # Resolve shortlinks (e.g., t.co, vt.tiktok.com) to the final domain securely
            try:
                head_resp = await self.cf_get(client, target_url, follow_redirects=True, timeout=5.0)
                final_url = str(head_resp.url)
            except Exception:
                final_url = target_url

            # --- 1. TIKTOK ---
            if "tiktok.com" in final_url:
                if match := re.search(r'video/(\d+)', final_url):
                    video_id = match.group(1)
                    assets["video"] = f"https://www.tiktok.com/embed/v2/{video_id}"
                    try:
                        encoded_url = urllib.parse.quote(final_url)
                        o_req = await self.cf_get(client, f"https://www.tiktok.com/oembed?url={encoded_url}")
                        if o_req.status_code == 200:
                            assets["photo"] = o_req.json().get("thumbnail_url")
                    except Exception:
                        pass

            # --- 2. X / TWITTER ---
            elif any(domain in final_url for domain in ["x.com", "twitter.com"]):
                if match := re.search(r'status/(\d+)', final_url):
                    tweet_id = match.group(1)
                    assets["video"] = f"https://platform.twitter.com/embed/Tweet.html?id={tweet_id}"
                    try:
                        vx_req = await self.cf_get(client, f"https://api.vxtwitter.com/i/status/{tweet_id}")
                        if vx_req.status_code == 200:
                            if media := vx_req.json().get("media_extended", []):
                                assets["photo"] = media[0].get("thumbnail_url") or media[0].get("url")
                    except Exception:
                        pass

            # --- 3. TELEGRAM & GENERAL FALLBACK ---
            else:
                try:
                    req = await self.cf_get(client, final_url, follow_redirects=True)
                    soup = BeautifulSoup(req.text, "html.parser")
                    
                    assets["video"] = (
                        (soup.find("meta", attrs={"name": "twitter:player"}) or {}).get("content") or
                        (soup.find("meta", property="og:video:secure_url") or {}).get("content")
                    )
                    og_img = soup.find("meta", property="og:image") or soup.find("meta", attrs={"name": "twitter:image"})
                    if og_img:
                        assets["photo"] = og_img.get("content")
                except Exception:
                    pass

        except Exception as e:
            print(f"[!] Extraction Error: {e}")

        for k in ["video", "photo"]:
            if isinstance(assets[k], str):
                assets[k] = assets[k].replace('\\u002F', '/')

        return assets

    async def llm_triage(self, client, raw_title):
        """
        V28 HYDRA ENGINE: 
        Now standardized for Gemini, Cerebras, and Mistral via the official AsyncOpenAI SDK.
        """
        from openai import AsyncOpenAI # Import the stable library

        if not HYDRA_MANAGER.cycle:
            return {"title": raw_title, "intensity": "LOW ⚪", "loc_name": "Unknown", "context": "ALL_KEYS_MISSING 🔴"}

        node = next(HYDRA_MANAGER.cycle)
        
        system_msg = (
            "You are a military intelligence analyst. "
            "1. Translate to English. 2. Intensity: LOW/MODERATE/HIGH/CRITICAL. "
            "3. Loc: Extract EXACT city, region, or country. "
            "4. Context: One-sentence tactical background explaining the 'why'. "
            "5. IT IS FORBIDDEN TO LEAVE LOCATION FIELD EMPTY! ALWAYS PUT ACCURATE LOCATION BASED ON CONTEXT AND DATA THAT WAS GIVEN!, ALWAYS PROVIDED CORRECT PLACE FOR OPEN STREET MAP TO GEOCODE. "
            "Output ONLY valid JSON: {\"title\": \"Title\", \"intensity\": \"Level\", \"loc\": \"Geocodable Location\", \"context\": \"Context\"}"
        )

        try:
            # Initialize the stable Python library client dynamically
            ai_client = AsyncOpenAI(
                base_url=node["base_url"],
                api_key=node["key"],
                max_retries=2
            )
            
            # The SDK automatically handles the /chat/completions URL appending and headers
            response = await ai_client.chat.completions.create(
                model=node["model"],
                messages=[
                    {"role": "system", "content": system_msg},
                    {"role": "user", "content": f"Triage: {raw_title}"}
                ],
                temperature=0.0,
                response_format={"type": "json_object"} 
            )
            
            raw_text = response.choices[0].message.content.strip()

            # JSON Cleaning (in case some models ignore response_format)
            if raw_text.startswith("```json"):
                raw_text = raw_text[7:-3].strip()
            elif raw_text.startswith("```"):
                raw_text = raw_text[3:-3].strip()

            data = json.loads(raw_text)
            
            return {
                "title": data.get("title", raw_title),
                "intensity": data.get("intensity", "MODERATE 🟡"),
                "loc_name": data.get("loc", "Unknown"),
                "context": data.get("context", f"Triage via {node['model']}")
            }

        except Exception as e:
            print(f"[!] TRIAGE FAIL ({node['model']}): {e}")
            return {
                "title": raw_title, 
                "intensity": "LOW ⚪", 
                "loc_name": "Unknown", 
                "context": f"Processing failed on {node['model']}."
            }

    async def osm_geocode(self, client, loc_name):
        """
        Hardened OSM Geocoder: 
        Rejects junk AI outputs and respects Nominatim's strict rate limits.
        """
        if not loc_name or not isinstance(loc_name, str): 
            return None, None
            
        loc_clean = loc_name.strip()
        
        # Blacklist of vague terms the AI might hallucinate
        bad_words = ["none", "unknown", "global", "middle east", "unspecified", "various", "worldwide"]
        if loc_clean.lower() in bad_words or len(loc_clean) < 3: 
            return None, None
            
        url = f"https://nominatim.openstreetmap.org/search?q={urllib.parse.quote(loc_clean)}&format=json&limit=1"
        
        async with OSM_RATE_LIMIT_LOCK:
            await asyncio.sleep(1.2) # Nominatim will ban IPs if you query faster than 1 sec
            try:
                r = await client.get(url, timeout=10)
                data = r.json()
                # Check if Nominatim actually found a match
                if data and isinstance(data, list) and len(data) > 0: 
                    return float(data[0]["lat"]), float(data[0]["lon"])
            except: 
                pass 
                
        return None, None

    # ==========================================
    # 2. BULLETPROOF FETCHERS
    # ==========================================

    async def fetch_x_stealth(self, client):
        """
        V26: X/TWITTER SHADOW-FETCH
        Bypasses X's anti-bot walls by querying Google's real-time index of x.com.
        """

        import urllib.parse
        import random
        import feedparser

        print("[*] Deploying X/Twitter Shadow-Fetch...")

        # 1. THE TARGET MATRIX
        # You can search by specific OSINT accounts OR by kinetic keywords
        tactical_keywords = ["strike", "intercept", "explosion", "deployment", "military_war", "military_conflict_today", "daily_news_feed", "military_escalation", "politics"]
        target_accounts = {
            # 🔴 TIER 1: KINETIC OSINT & RAPID ALERTS (The "First Responders")
            "CORE_OSINT": [
                "OSINTdefender", "clashreport", "Faytuks", "AuroraIntel", "OSINTtechnical", 
                "IntelCrab", "bellingcat", "liveuamap", "DEFCONWSALERTS", "warmapper", 
                "TheStudyofWar", "CovertShores", "CavasShips", "GeoConfirmed", "OAlexanderDK",
                "COUPSURE", "trbrtc", "Archer83Able", "Schizointel", "Osinttechnical"
            ],
            # 🟡 TIER 2: MIDDLE EAST & LEVANT TACTICAL
            "MENA_HOTZONE": [
                "ELINTNews", "IranIntl_En", "jpost", "barakravid", "Joyce_Karam", 
                "hasanalijawad", "JoeTruzman", "InstaNewsAlerts", "AJArabic", "AmichaiStein1",
                "TreyYingst", "NeriZilber", "EmanuelMacron", "manniefabian", "ragipsoylu",
                "QalaatAlMudiq", "Charles_Lister", "michaelh992", "AliHa_97", "GLZRadio",
                "UpdatesYemeni", "South24_net", "FDD", "SinaToossi", "Khaaash"
            ],

            # 🔵 TIER 3: EASTERN EUROPE & EURASIA
            "EURASIA_HOTZONE": [
                "NOELreports", "KyivIndependent", "Tatarigami_UA", "nexta_tv", "region776",
                "TrentTelenko", "KofmanMichael", "MassDara", "RobLee85", "front_ukrainian",
                "bayraktar_1love", "Tendar", "Gerashchenko_en", "PStyleOne1", "wartranslated",
                "IntelArrow", "The_Lookout_N", "Korsas_Military", "RALee85", "shashj"
            ],

            # 🟢 TIER 4: INDO-PACIFIC & ASIA
            "INDO_PACIFIC": [
                "IndoPac_Info", "CollinSLKoh", "t_shugart", "duandang",
                "RFA_Chinese", "WilliamYang120", "DefenseAlerts", "sidhant", "ThePrintIndia",
                "SushantSin", "ShivAroor", "nktpnd", "M_Bhojwani", "jcheng143",
                "Hawkeye1745", "IndoPac_Info", "Strategic_Front", "LCSM_Updates", "C_A_M_P_U_S"
            ],

            # 🏛️ TIER 5: GLOBAL THINK TANKS & DEFENSE MEDIA
            "DEFENSE_ANALYSIS": [
                "TheWarZoneWire", "defense_news", "breakingdefense", "USNINews", "CSIS", 
                "ChathamHouse", "RANDCorporation", "stratfor", "CFR_org", "AtlanticCouncil",
                "RUSI_org", "IISS_org", "SIPRIorg", "CrisisGroup", "EurasiaGroup",
                "ForeignAffairs", "WarOnTheRocks", "TheCipherBrief", "JanesINTEL", "NavalNewscom"
            ],

            # 👔 TIER 6: GEOPOLITICAL FIGURES & DIPLOMATS
            "GLOBAL_LEADERS": [
                "POTUS", "SecBlinken", "StateDept", "10DowningStreet", "EmmanuelMacron", 
                "Scholz", "vonderleyen", "NATO", "jensstoltenberg", "UN",
                "antonioguterres", "ZelenskyyUa", "Kuleba", "RishiSunak", "David_Cameron",
                "FCDOGovUK", "Elysee", "Bundeskanzler", "PolaSecUK", "KariKasper"
            ],

            # ⚔️ TIER 7: CYBER, FLIGHT TRACKING & TECH INTEL
            "TECH_CYBER": [
                "CivMilAir", "Flightradar24", "AircraftSpots", "GDarkconrad", "vxunderground",
                "malwrhunterteam", "BleepinComputer", "TheHackersNews", "Cyber_War_News", "IntelDoge",
                "GossiTheDog", "Unit42_Intel", "CrowdStrike", "Mandiant", "TalosSecurity",
                "threatintel", "RedDrip7", "HackerNews", "CyberScoopNews", "DarkWebInformer"
            ]
        }
    
        # Flatten and sample to stay under dork character limits while rotating focus
        all_accounts = [user for sublist in target_accounts.values() for user in sublist]
        sampled_accounts = random.sample(all_accounts, min(len(all_accounts), 5))
        
        # 2. PAYLOAD CONSTRUCTION (Optimized for Neural Mesh & Stealth Proxying)
        keywords_query = " OR ".join(tactical_keywords)
        accounts_query = " OR ".join([f"from:{user}" for user in sampled_accounts])
        raw_payload = f"({accounts_query}) ({keywords_query})"

        # 3. EXECUTION VIA PINNACLE SOCIAL SNIPER
        # Upgraded to use fetch_social_news which manages the 14-provider mesh
        results = await self.dorker.fetch_social_news(
            platform="X",
            query=raw_payload
        )
        
        if not results:
            print("[!] X Shadow-Fetch: Pinnacle Mesh returned zero hits.")
            return [self.create_error_block("ZERO_HITS", "No fresh X intel secured.", "X TITAN")]

        all_raw_items = []
        for r in results:
            # 4. RAW SIGNAL CLEANING (The X-Scrubbing Protocol)
            raw_title = r.get('title', '')
            
            # Remove platform branding (X/Twitter) and pipe separators
            clean_title = re.sub(r'[-|]\s*X\s*$', '', raw_title, flags=re.IGNORECASE)
            clean_title = re.sub(r'[-|]\s*Twitter\s*$', '', clean_title, flags=re.IGNORECASE)
            
            # Strip embedded URLs and normalize whitespace
            clean_title = re.sub(r'https?://\S+', '', clean_title)
            clean_title = re.sub(r'\s+', ' ', clean_title).strip()
            
            if len(clean_title) < 10:
                continue

            # 5. DEDUPLICATION & METADATA BINDING
            # self.track_viewpoint prevents re-processing known signals in the 24/7 loop
            if self.track_viewpoint(clean_title, "X_TITAN", r['url']):
                all_raw_items.append({
                    "raw_title": clean_title, 
                    "src": r.get('source', 'X_TITAN'), 
                    "url": r['url'],
                    "embed_ready": r.get('embed_ready', True) # Metadata for UI injection
                })
                
        # 6. PARALLEL INTEL PROCESSING (DINEI / Cohere)
        if not all_raw_items:
            return []

        print(f"[*] Processing {len(all_raw_items)} X-intelligence signals via V12 Pipeline...")
        processed = await asyncio.gather(*(self.process_intel(client, item) for item in all_raw_items))
        
        valid_intel = [res for res in processed if res]
        print(f"[+] Secured {len(valid_intel)} unique X/Twitter assets.")
        
        return valid_intel

    async def fetch_tiktok_stealth(self, client):
        """
        V35: TIKTOK TITAN-FETCH
        Bypasses TikTok anti-bot walls via EasyDorkerCustom. 
        Extracts clean intelligence from video descriptions.
        """
        import re
        import random
        print("[*] Deploying TikTok Titan-Fetch Matrix...")

        # 1. THE TACTICAL VECTOR (High-density hashtags)
        tactical_tags = [
            'combat footage', 'ground truth', 'geolocated', 
            'kinetic strike', 'loitering munition', 'artillery duel',
            'troop movement', 'armored column', 'SAM system',
            'air raid sirens', 'battle damage', 'frontline update',
            'tactical retreat', 'breakthrough', 'intercepted'
        ]
        
        selected_tags = random.sample(tactical_tags, k=min(len(tactical_tags), 5))
        tag_payload = " OR ".join(selected_tags)
        
        # 3. EXECUTION VIA PINNACLE SOCIAL SNIPER
        # Swapped 'execute_cascade' for the specialized 'fetch_social_news'
        # This triggers the automatic fallback mesh for high-security social domains
        results = await self.dorker.fetch_social_news(
            platform="TIKTOK",
            query=tag_payload
        )
        
        if not results:
            return [self.create_error_block("ZERO_HITS", "No fresh TikTok intel found.", "TIKTOK TITAN")]
            
        all_raw_items = []
        for r in results:
            # 4. RAW SIGNAL CLEANING (Enhanced Scrubbing Protocol)
            raw_desc = r.get('title', '')
            
            # Strip platform branding and pipe separators (TikTok often appends these)
            clean_desc = re.sub(r'[-|]\s*TikTok\s*$', '', raw_desc, flags=re.IGNORECASE)
            
            # Permanently delete all hashtags to isolate the raw intelligence
            clean_desc = re.sub(r'#\w+', '', clean_desc)
            
            # Strip Emojis and non-intelligence characters that break parsers
            clean_desc = re.sub(r'[^\x00-\x7F]+', ' ', clean_desc)
            
            # Normalize whitespace
            clean_desc = re.sub(r'\s+', ' ', clean_desc).strip()
            
            # If description is too short after scrubbing, use a tactical placeholder
            if len(clean_desc) < 5: 
                clean_desc = f"INTEL_SIGNAL_{random.randint(1000, 9999)}"
            
            # 5. DEDUPLICATION & METADATA BINDING
            # Track_viewpoint prevents double-processing of the same URL/Intel
            if self.track_viewpoint(clean_desc, "TIKTOK_TITAN", r['url']):
                all_raw_items.append({
                    "raw_title": clean_desc, 
                    "src": r.get('source', 'TIKTOK_TITAN'), 
                    "url": r['url'],
                    "embed_ready": r.get('embed_ready', False) # New for V12
                })
                
        # 6. PARALLEL INTEL PROCESSING
        if not all_raw_items:
            return []

        # Dispatch items to the DINEI or LLM processing pipeline
        processed = await asyncio.gather(*(self.process_intel(client, item) for item in all_raw_items))
        
        # Filter out empty results
        valid_intel = [res for res in processed if res]
        print(f"[+] Secured {len(valid_intel)} unique TikTok intelligence assets via V12 Mesh.")
        
        return valid_intel

    async def fetch_telegram_stealth(self, client):
        source_file = "backend//telegram_new_sources.txt"
        #print(f"[*] TELEGRAM DEBUG: Attempting to read {source_file}...")
        
        try:
            with open(source_file, "r") as f:
                targets = [l.strip().lstrip('@') for l in f if l.strip()][:5]
                #print(f"[*] TELEGRAM DEBUG: Successfully loaded {len(targets)} targets: {targets}")
            if not targets:
                #print("[!] TELEGRAM DEBUG: The file exists but appears to be empty or misformatted.")
                return []
        except Exception as e:
            #print(f"[!] TELEGRAM DEBUG: CRITICAL ERROR reading {source_file} - {e}")
            return []

        all_raw = []
        for t in targets:
            #print(f"[*] TELEGRAM DEBUG: Pinging channel @{t}...")
            try:
                # Check the actual HTTP status
                r = await client.get(f"https://t.me/s/{t}", timeout=10)
                if r.status_code != 200:
                    #print(f"[!] TELEGRAM DEBUG: @{t} blocked us! HTTP {r.status_code}. It might not be a public channel.")
                    continue

                soup = BeautifulSoup(r.text, 'html.parser')
                msgs = soup.find_all("div", class_="tgme_widget_message_wrap")[-2:]
                
                #print(f"[*] TELEGRAM DEBUG: @{t} - Found {len(msgs)} recent messages on the page.")
                
                for m in msgs:
                    txt = m.find("div", class_="tgme_widget_message_text")
                    link = m.find("a", class_="tgme_widget_message_date")
                    
                    if not txt or not link:
                        #print(f"[-] TELEGRAM DEBUG: @{t} - Skipped a message (Missing text or date link).")
                        continue
                    
                    # PRE-EXTRACT MEDIA HERE
                    img_url = None
                    photo_div = m.find("div", class_="tgme_widget_message_photo_wrap")
                    if photo_div:
                        match = re.search(r"url\(['\"]?(.*?)['\"]?\)", photo_div.get('style', ''))
                        if match: 
                            img_url = match.group(1)
                            #print(f"[*] TELEGRAM DEBUG: @{t} - Image successfully pre-extracted.")

                    clean_txt = txt.text[:160]
                    
                    # Debug the deduplicator
                    if self.track_viewpoint(clean_txt, f"TG: {t}", link.get('href')):
                        #print(f"[+] TELEGRAM DEBUG: @{t} - New Intel found! Queueing for triage.")
                        all_raw.append({
                            "raw_title": clean_txt,
                            "src": f"TG: {t}",
                            "url": link.get('href'),
                            "media": {"video": None, "photo": img_url} 
                        })
                    else:
                        #print(f"[-] TELEGRAM DEBUG: @{t} - Duplicate intel. Ignored.")
                        pass
                        
            except Exception as e:
                # THIS catches timeouts, connection resets, etc.
                #print(f"[!] TELEGRAM DEBUG: Error processing @{t} - {e}")
                continue
        
        #print(f"[*] TELEGRAM DEBUG: Pipeline sending {len(all_raw)} items to Groq.")
        
        if not all_raw:
            return []
            
        results = await asyncio.gather(*(self.process_intel(client, i) for i in all_raw))
        return [res for res in results if res]

    async def process_intel(self, client, raw_item):
        async with SEMAPHORE:
            # 1. Enhanced Triage (Fixed safe parsing)
            intel = await self.llm_triage(client, raw_item['raw_title'])
            lat, lon = await self.osm_geocode(client, intel.get('loc_name', 'Unknown'))
            
            # 2. Extract Media (RESPECT PRE-SCRAPED MEDIA from Telegram)
            if 'media' in raw_item and raw_item['media']['photo']:
                media_assets = raw_item['media']
            else:
                media_assets = await self.extract_media(client, raw_item['url'])
            
            # 3. Source Logo Mapping
            source_logos = {
                "X": "https://abs.twimg.com/favicons/twitter.2.ico",
                "TG": "https://telegram.org/favicon.ico",
                "TIKTOK": "https://www.tiktok.com/favicon.ico"
            }
            logo = next((v for k, v in source_logos.items() if k in raw_item['src'].upper()), "https://cdn-icons-png.flaticon.com/512/595/595568.png")

            return {
                "intensity": intel.get('intensity', 'MODERATE 🟡'),
                "title": intel.get('title', raw_item['raw_title']),
                "context": intel.get('context', 'No context available.'),
                "attribution": {
                    "source": raw_item['src'],
                    "logo": logo,
                    "url": raw_item['url']
                },
                "location": { "name": intel.get('loc_name', 'Unknown'), "lat": lat, "lon": lon },
                "media": media_assets, # Now successfully passes videos/photos to frontend
                "timestamp": datetime.now(timezone.utc).isoformat()
            }

    async def get_active_hotzones(self, client):
        try:
            url = "https://api.reliefweb.int/v2/disasters?appname=VisionSphere&query[value]=status:current AND primary_type:Complex Emergency&limit=3"
            r = await client.get(url, timeout=10)
            data = r.json().get('data', [])
            return [d['fields']['name'] for d in data] if data else ["Middle East", "Ukraine", "Sudan", "Myanmar"]
        except Exception:
            return ["Middle East", "Ukraine", "Sudan", "Myanmar"]

    async def fetch_gdelt(self, client):
        """
        V23 ULTRA-HARDENED: Uses curl_cffi for TLS Impersonation and 
        proper parameter encoding to bypass GDELT connection resets.
        """
        import urllib.parse
        from curl_cffi.requests import AsyncSession

        # 1. TACTICAL QUERY REFINEMENT
        # Adding 'timespan=1h' reduces server load and increases success rate.
        base_url = "https://api.gdeltproject.org/api/v2/doc/doc"
        params = {
            "query": "(tone<-2 OR military OR attack) -sports",
            "mode": "artlist",
            "format": "json",
            "maxrecords": "10",
            "timespan": "1h"
        }
        
        # Ensure query is properly encoded (Fixes the "Connection Error" caused by spaces)
        encoded_url = f"{base_url}?{urllib.parse.urlencode(params)}"

        # 🛡️ STRATEGY: Browser Impersonation
        # GDELT frequently resets connections that don't look like Chrome/Edge.
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
            "Accept": "application/json, text/plain, */*",
            "Referer": "https://www.gdeltproject.org/"
        }

        for attempt in range(3):
            try:
                # Jittered Backoff
                wait_time = (attempt * 7) + random.uniform(1.0, 3.0)
                if attempt > 0:
                    print(f"[!] GDELT Retry {attempt}/3 - Waiting {wait_time:.1f}s...")
                await asyncio.sleep(wait_time)

                # USE curl_cffi IF AVAILABLE FOR BYPASS, ELSE HTTPX WITH HEADERS
                # Note: We use impersonate="chrome110" to beat TLS fingerprinting
                async with AsyncSession() as s:
                    r = await s.get(encoded_url, headers=headers, impersonate="chrome110", timeout=30)
                
                if r.status_code == 200:
                    data = r.json()
                    articles = data.get('articles', [])
                    if not articles:
                        return []
                    
                    # Process items
                    raw_items = [
                        {
                            "raw_title": a['title'], 
                            "src": f"GDELT ({a.get('sourcecountry', 'INTL')})", 
                            "url": a['url']
                        } 
                        for a in articles 
                        if self.track_viewpoint(a['title'], f"GDELT ({a.get('sourcecountry', 'INTL')})", a['url'])
                    ]
                    
                    results = await asyncio.gather(*(self.process_intel(client, item) for item in raw_items))
                    return [res for res in results if res]

                elif r.status_code == 429:
                    continue # Trigger next retry
                else:
                    print(f"[!] GDELT Status {r.status_code} on attempt {attempt}")

            except Exception as e:
                print(f"[!] GDELT Connection Failure: {type(e).__name__}")
                continue

        # 🛡️ THE FAILSAFE: Activates if all GDELT API attempts fail
        print("[⚡] GDELT API Terminally Blocked. Activating Crawler Fallback...")
        fallback_query = "site:gdeltproject.org (military OR attack) when:1h"
        return await self.fetch_dynamic_crawler(client, fallback_query, "GDELT FAILSAFE")

    async def fetch_premium_matrix(self, client):
        """
        V12.8: ELITE GHOST MATRIX (Sniper 5 Edition)
        - Targeted sharding across 36 Premium Nodes.
        - Uses the 9-provider news mesh (fetch_news).
        - STRICT LIMIT: Returns exactly 5 triaged signals per run.
        """
        import random
        import asyncio
        import re

        # The 36-Node Elite List
        PREMIUM_DOMAINS = {
            "ACLED": "acleddata.com",
            "UCDP": "ucdp.uu.se",
            "CrisisWatch": "crisisgroup.org",
            "GTD (Terrorism)": "start.umd.edu",
            "Bellingcat": "bellingcat.com",
            "South24": "south24.net",
            "Janes": "janes.com",
            "Defense News": "defensenews.com",
            "Breaking Defense": "breakingdefense.com",
            "Defense One": "defenseone.com",
            "Naval News": "navalnews.com",
            "USNI News": "news.usni.org",
            "Army Tech": "army-technology.com",
            "C4ISRNET": "c4isrnet.com",
            "The War Zone": "twz.com",
            "Stratfor": "worldview.stratfor.com",
            "Geopolitical Monitor": "geopoliticalmonitor.com",
            "The Diplomat": "thediplomat.com",
            "Foreign Affairs": "foreignaffairs.com",
            "CFR": "cfr.org",
            "RAND": "rand.org",
            "Cipher Brief": "thecipherbrief.com",
            "War on the Rocks": "warontherocks.com",
            "Long War Journal": "longwarjournal.org",
            "ReliefWeb": "reliefweb.int",
            "ACAPS": "acaps.org",
            "HDX Data": "data.humdata.org",
            "Aid Worker Security": "aidworkersecurity.org",
            "Piracy Center": "icc-ccs.org",
            "DSCA (Arms Sales)": "dsca.mil",
            "NATO News": "nato.int",
            "DOD Press": "defense.gov",
            "OilPrice Geopolitics": "oilprice.com",
            "UN Security Council": "securitycouncilreport.org",
            "Small Arms Survey": "smallarmssurvey.org"
        }
        print(f"[*] Ghost Matrix Pulse: Scanning shard for 5 elite signals...")
        
        # 1. SHARDING: Pick 5 random domains to keep the scan "quiet" and efficient
        sampled_names = random.sample(list(PREMIUM_DOMAINS.keys()), 5)
        raw_pool = []
        
        # 2. MESH FETCHING (Uses all 9 keys from EasyDorker)
        async def fetch_node(name):
            domain = PREMIUM_DOMAINS[name]
            # Search for the most recent intel on this specific node
            query = f"site:{domain} breaking OR exclusive OR update"
            try:
                # Call the 9-provider aggregator you requested
                hits = await self.dorker.fetch_news(query)
                if hits:
                    for h in hits:
                        # Clean title and tag with node name
                        clean_t = re.sub(r'\s*[-|]\s*.*$', '', h.get('title', '')).strip()
                        raw_pool.append({
                            "raw_title": clean_t,
                            "src": f"ELITE: {name.upper()}",
                            "url": h.get('url')
                        })
            except Exception:
                pass

        # Run the 5-node shard in parallel
        await asyncio.gather(*(fetch_node(name) for name in sampled_names))

        if not raw_pool:
            return []

        # 3. AI TRIAGE & LIMITING (The "Sniper 5" Slice)
        # We sort by latest found or just shuffle, then take only the top 5
        random.shuffle(raw_pool) 
        target_pool = raw_pool[:10] # Triage 10 to find 5 winners
        
        ai_sem = asyncio.Semaphore(3)
        async def triage(item):
            async with ai_sem:
                if hasattr(self, 'process_intel'):
                    return await self.process_intel(client, item)
                return item

        triaged_results = await asyncio.gather(*(triage(i) for i in target_pool), return_exceptions=True)
        
        # 4. FINAL FILTER: Keep only valid, unique results and slice to exactly 5
        final_5 = []
        for res in triaged_results:
            if res and not isinstance(res, Exception):
                final_5.append(res)
                if len(final_5) >= 5: break # HARD LIMIT reached

        print(f"[+] Premium Pulse Complete. Secured {len(final_5)} elite assets.")
        return final_5

    async def fetch_dynamic_crawler(self, client, query, cluster_name):
        """V19: Replaces 'Node Silent' with actionable error reporting."""
        url = f"https://news.google.com/rss/search?q={urllib.parse.quote(query)}+when:24h&hl=en-US&gl=US"
        try:
            r = await client.get(url, timeout=15)
            feed = feedparser.parse(r.content)
            
            if not feed.entries:
                # Instead of returning [], we return a 'Visible Error'
                return [{
                    "lvl": "ZERO_RESULTS ⚪",
                    "title": f"No data found for query: [{query}]",
                    "src": cluster_name,
                    "loc": "N/A - Check Query Syntax",
                    "url": url
                }]
            
            raw_items = []
            for e in feed.entries[:3]:
                title = e.title.split(" - ")[0].strip()
                if self.track_viewpoint(title, cluster_name, e.link): # <--- FIXED TO 'cluster_name'
                    raw_items.append({"raw_title": title, "src": cluster_name, "url": e.link})
            
            results = await asyncio.gather(*(self.process_intel(client, item) for item in raw_items))
            return [res for res in results if res]
        except Exception as e:
            return [self.create_error_block("SCRAPE_EXCEPTION", str(e), cluster_name)]

    async def parse_and_fetch_opml(self, client):
        """
        V12.9: OPML LEVIATHAN (20k Matrix Edition)
        - High-velocity regex extraction with RAM caching.
        - Uses the V12.9 Randomized Pulse Mesh (fetch_news).
        - Snipe-targets 10 global nodes per cycle to prevent rate limits.
        """
        import re, random, html, asyncio
        from dataclasses import dataclass

        opml_path = "universal_sources.opml"
        
        @dataclass
        class MatrixNode:
            name: str
            site_query: str

        nodes = getattr(self, '_opml_cache', [])
        
        if not nodes:
            try:
                print(f"[*] OPML Leviathan: Booting cold cache from {opml_path}...")
                with open(opml_path, 'r', encoding='utf-8') as f:
                    content = f.read()
                    # Optimized pattern for your OPML structure
                    pattern = r'text="([^"]+)"\s+title="[^"]+"\s+xmlUrl="([^"]+)"'
                    matches = re.findall(pattern, content)
                    for name, raw_query in matches:
                        nodes.append(MatrixNode(name=name, site_query=html.unescape(raw_query)))
                
                self._opml_cache = nodes # Save to memory for all future cycles
                print(f"[+] OPML Leviathan: {len(nodes)} global sources loaded into RAM.")
            except Exception as e:
                return [self.create_error_block("MATRIX_FAULT", str(e)[:30], "OPML LEVIATHAN")]

        if not nodes:
            return []

        active_shard = random.sample(nodes, min(len(nodes), 5))
        #print(f"[*] Sharding 20k Matrix -> Pulsing {len(active_shard)} global nodes.")

        raw_pool = []
        
        async def node_sniper(node):
            try:
                tactical_dork = f"{node.site_query} breaking OR latest OR update"
                
            
                hits = await self.dorker.fetch_news(tactical_dork)
                
                if hits:
                    for h in hits:
 
                        clean_t = re.sub(r'\s*[-|]\s*.*$', '', h.get('title', '')).strip()
                        raw_pool.append({
                            "raw_title": clean_t, 
                            "src": f"GLOBAL: {node.name}", 
                            "url": h.get('url')
                        })
            except Exception:
                pass

 
        await asyncio.gather(*(node_sniper(node) for node in active_shard))

        if not raw_pool:
            return []

        random.shuffle(raw_pool)
        target_pool = raw_pool[:12] 
        
        ai_sem = asyncio.Semaphore(1)
        async def triage(item):
            async with ai_sem:
                await asyncio.sleep(random.uniform(0.5, 1.2))
                if hasattr(self, 'process_intel'):
                    return await self.process_intel(client, item)
                return item

        print(f"[+] OPML Mesh found {len(raw_pool)} raw signals. Triaging...")
        triaged_results = await asyncio.gather(*(triage(i) for i in target_pool), return_exceptions=True)
        
        final_intel = []
        for res in triaged_results:
            if res and not isinstance(res, Exception):
                final_intel.append(res)
                if len(final_intel) >= 6: break 

        print(f"[✅] OPML Leviathan Complete. Secured {len(final_intel)} unique global assets.")
        return final_intel
    
    async def fetch_and_process_dinei(self, client, config):
        """
        DINEI V3 (LEAN NITRO): 
        Synthesizes vectors and executes multi-provider OSINT searches.
        Pipes directly to Triage without redundant extraction.
        """
        import asyncio
        from DINEI_module import IntelBrain
        from EasyDorkerCustom import EasyDorkerCustom

        # 1. BRAIN SYNTHESIS: Generate Vectors
        #profile_name = config.get('name', 'universal_horizon')
        brain = IntelBrain(profile_config=config)
        vectors = await brain.generate_vectors(count=5)
        
        if not vectors:
            print(f"[!] DINEI_BRIDGE: Brain synthesis returned 0 vectors.")
            return []

        # 2. SEARCH ENGINE INITIALIZATION
        # Ensure we use the class instance if it exists, else init a local one
        dorker = getattr(self, 'dorker', EasyDorkerCustom())
        
        # 3. MULTI-THREADED INTELLIGENCE GATHERING
        async def gather_vector_intel(query):
            """Fetches across News and Social, filtering for valid dictionary objects."""
            tasks = [
                dorker.fetch_news(query),
                dorker.fetch_social_news("X", query),
                dorker.fetch_social_news("TELEGRAM", query)
            ]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            flat_results = []
            for res in results:
                # CRITICAL FIX: Ensure 'res' is a list and items are valid dicts
                if isinstance(res, list):
                    flat_results.extend([r for r in res if isinstance(r, dict)])
            return flat_results

        print(f"[🧠] DINEI_BRIDGE: Processing {len(vectors)} intelligence vectors...")
        search_results = await asyncio.gather(*(gather_vector_intel(v) for v in vectors))
        
        # 4. DEDUPLICATION & KEY NORMALIZATION
        seen_urls = set()
        unique_items = []
        for batch in search_results:
            for item in batch:
                url = item.get('url') if item else None
                if url and url not in seen_urls:
                    seen_urls.add(url)
                    
                    # 🛠️ CRITICAL FIX: Map the Dorker keys to match process_intel's expectations
                    normalized_item = {
                        "raw_title": item.get('title') or item.get('raw_title') or "Untitled Signal",
                        "src": item.get('source') or item.get('src') or "DINEI_MESH",
                        "url": url,
                        "media": {"video": None, "photo": None} # Protects against media extraction KeyErrors
                    }
                    unique_items.append(normalized_item)

        if not unique_items:
            print("[!] DINEI_BRIDGE: No unique assets found in this cycle.")
            return []

        # 5. FINAL TRIAGE: Direct Pipe
        print(f"[✅] DINEI_BRIDGE: Secured {len(unique_items)} assets. Piping to Triage...")
        final_results = await asyncio.gather(*(self.process_intel(client, item) for item in unique_items))
        
        return [res for res in final_results if res]
    
    async def process_worker_burst(self, results, label):
        """
        V12.5: NITRO INTEL PROCESSOR
        Handles O(1) Deduplication, Supabase Uplink, and Telemetry.
        """
        if not results:
            return 0

        new_assets_count = 0
        
        # Ensure processed_urls is a set for O(1) speed (initialize if needed)
        if not hasattr(self, 'processed_urls_set'):
            self.processed_urls_set = set(getattr(self, 'processed_urls', []))

        for item in results:
            # Flexible URL extraction (handles different worker schemas)
            url = item.get('url') or item.get('attribution', {}).get('url')
            if not url: continue
            
            # 🛡️ O(1) DEDUPLICATION (The "Wealth" Moat)
            if url not in self.processed_urls_set:
                self.processed_urls_set.add(url)
                new_assets_count += 1
                
                asyncio.create_task(self.push_to_turso(item))
                
                self._print_telemetry(item, label)

                # 🧠 MEMORY MANAGEMENT (Preventing Memory Leaks)
                if len(self.processed_urls_set) > 5000:
                    # Convert to list to pop oldest, then back to set
                    temp_list = list(self.processed_urls_set)
                    self.processed_urls_set = set(temp_list[-4000:])
        
        return new_assets_count

    def _print_telemetry(self, item, label):
        """Helper to keep the main loop readable."""
        source = item.get('attribution', {}).get('source', 'UNKNOWN')
        intensity = item.get('intensity', 'LOW')
        loc = item.get('location', {})
        
        print(f"\n{'='*40}")
        print(f"📡 SOURCE: {source} [{label}]")
        print(f"🔥 INTENSITY: {intensity}")
        print(f"📍 LOCATION: {loc.get('name')} [{loc.get('lat')}, {loc.get('lon')}]")
        print(f"📝 TITLE: {item.get('title')}")
        print(f"🔗 URL: {item.get('attribution', {}).get('url')}")
        print(f"{'='*40}\n")

    async def health_check_server(self):
        from aiohttp import web
        import os
    
        async def handle(request):
            # This tells Hugging Face (and you) that the Leviathan is still breathing
            return web.Response(text=f"VisionSphere Online | Nodes: {len(self.processed_urls)}")
    
        app = web.Application()
        app.router.add_get('/', handle)
        
        # CRITICAL: Hugging Face uses 7860, Render uses PORT env
        port = int(os.environ.get("PORT", 7860)) 
        
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, '0.0.0.0', port)
        
        print(f"[+] SOVEREIGN_HEARTBEAT: Listening on port {port}")
        await site.start()

    async def execute_stream(self):
        """
        V12.5: SEQUENTIAL NITRO STREAM
        Runs workers one-by-one with 30s gaps and full telemetry.
        """
        print(f"\n{'='*60}\n VISIONSPHERE V12.5: SEQUENTIAL NITRO MODE\n{'='*60}\n")

        limits = httpx.Limits(max_connections=10, max_keepalive_connections=5)

        async with httpx.AsyncClient(
            timeout=60.0, 
            headers=self.headers, 
            limits=limits,
            follow_redirects=True
        ) as client:
            
            # Start Health Check for Render
            asyncio.create_task(self.health_check_server())

            # Define your Worker Matrix
            workers = [
                (self.fetch_x_stealth, "X_SHADOW"),
                (self.fetch_tiktok_stealth, "TIKTOK"),
                (self.fetch_premium_matrix, "PREMIUM"),
                (self.parse_and_fetch_opml, "GLOBAL_OPML"),
                (partial(self.fetch_and_process_dinei, config=INTEL_PROFILES["universal_horizon"]), "DINEI"),
                (self.fetch_gdelt, "GDELT")
            ]
    
            print(f"[🔥] ENGINE_STABLE: Monitoring {len(workers)} vectors sequentially.")

            while True:
                for func, label in workers:
                    try:
                        print(f"\n[▶️] ACTIVATING: {label}...")
                        
                        # 1. FETCH (The Stealth Mission)
                        results = await func(client)
                        
                        # 2. PROCESS (Dedupe, Upload, Print)
                        new_hits = await self.process_worker_burst(results, label)
                        
                        if new_hits > 0:
                            print(f"[✅] {label} cycle complete. Secured {new_hits} new assets.")
                        else:
                            print(f"[~] {label} cycle complete. No new intelligence found.")
                        
                    except Exception as e:
                        print(f"[❌] {label}_CRITICAL_FAIL: {e}")
                    
                    # 🕒 THE CALIBRATED COOL-DOWN
                    # 30s gap between workers prevents "clumping" and IP bans
                    await asyncio.sleep(30)

                print(f"\n[🏁] ROUND_COMPLETE: Restarting global mesh cycle...")
                await asyncio.sleep(10)

# --- [ TEST TRIGGER ] ---
if __name__ == "__main__":
    engine = DINEI_MOTHER()
    try:
        # We use execute_stream() instead of execute() for testing the continuous flow
        asyncio.run(engine.execute_stream())
    except KeyboardInterrupt:
        print("\n[!] User terminated the stream. Shutting down...")