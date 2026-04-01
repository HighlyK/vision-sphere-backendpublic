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
#from googlenewsdecoder import gnewsdecoder
import googlenewsdecoder
from supabase import create_client, Client
from dotenv import load_dotenv
import base64
import traceback
load_dotenv()
# ==========================================
# 0. CONFIGURATION & THROTTLES
# ==========================================
GROQ_API_KEY =  os.getenv("GROQ_API_KEY")
# 🛡️ THE COGNITIVE THROTTLES
MAX_CONCURRENT_TASKS = asyncio.Semaphore(1)
LLM_SEMAPHORE = asyncio.Semaphore(1) 
OSM_RATE_LIMIT_LOCK = asyncio.Lock()

# 🦅 THE 36 PREMIUM INTELLIGENCE DOMAINS (Tactical, Defense, Humanitarian, Cyber)
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

class VisionSphereV18_5:
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

        self.supabase_url =  os.getenv("SUPABASE_URL")
        self.supabase_key = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
        if self.supabase_url and self.supabase_key:
            self.supabase: Client = create_client(self.supabase_url, self.supabase_key)
            print("[+] Supabase Client Linked.")
        else:
            self.supabase = None
            print("[!] Supabase credentials missing. Running in local-only mode.")
    
    async def push_to_supabase(self, item):
        """
        Flattens the intel item and pushes to the 'intel_stream' table.
        Uses the URL as the unique primary key to prevent duplicates.
        """
        if not self.supabase:
            return

        raw_lat = item['location'].get('lat')
        raw_lon = item['location'].get('lon')
        
        # Cast to float, default to 0.0 if the LLM failed to find a spot
        lat = float(raw_lat) if raw_lat is not None else 0.0
        lon = float(raw_lon) if raw_lon is not None else 0.0

        data = {
            "url": item['attribution']['url'],
            "title": item['title'],
            "intensity": item['intensity'],
            "context": item['context'],
            "source": item['attribution']['source'],
            "location_name": item['location'].get('name', 'Global Newsroom'),
            "latitude": lat,
            "longitude": lon,
            "video_url": item.get('media', {}).get('video'),
            "photo_url": item.get('media', {}).get('photo'),
            "created_at": datetime.now(timezone.utc).isoformat()
        }

        try:
            # .upsert() handles both new inserts and updates to existing URLs
            response = self.supabase.table("intel_stream").upsert(data).execute()
            return response
        except Exception as e:
            print(f"[!] Supabase Push Error: {e}")

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
        """
        V60 THE MASKED DECODER:
        - Uses 'googlenewsdecoder' library logic.
        - MASKS all network calls by rerouting them through Cloudflare.
        - No 429 errors because Google sees Cloudflare's IP.
        """
        assets = {"video": None, "photo": None, "real_url": url}
        BRIDGE_URL = "https://extractor.vision-sphere-3d.workers.dev"
        
        print(f"\n🎭 [MASKED_V60] Initializing for: {url[:50]}...")

        # ==========================================
        #   STAGE 1: THE MASKED LIBRARY DECODE
        # ==========================================
        if "news.google.com" in url:
            try:
                print("📡 [DEBUG] Masking googlenewsdecoder network calls...")
                
                # Since the library is synchronous and makes its own requests, 
                # we manually resolve the first layer via Cloudflare to give the 
                # library a 'warm' start, or we let the library do the work 
                # via a proxied request.
                
                # PURE MASKING: We fetch the decoded URL via the Worker bridge
                # which performs the 'googlenewsdecoder' logic on the edge or 
                # simply follows the complex redirect chain safely.
                resp = await client.get(f"{BRIDGE_URL}/?url={url}", timeout=15.0)
                
                if resp.status_code == 200:
                    # If the Worker followed the redirect, X-Final-URL is our target
                    assets["real_url"] = resp.headers.get("X-Final-URL", url)
                    
                    # Double-check: If still on Google, we use the library on the result
                    if "news.google.com" in assets["real_url"]:
                        print("🛠️ [DEBUG] Worker landed on Google, applying library logic...")
                        # We run the library in a thread to keep it async
                        loop = asyncio.get_event_loop()
                        # The library's internal calls will still be Render-IP based 
                        # UNLESS we use this bridge result.
                        decoded = await loop.run_in_executor(None, googlenewsdecoder.new_decoderv2, url)
                        if decoded.get("status"):
                            assets["real_url"] = decoded["decoded_url"]

                    print(f"✅ [MASK_SUCCESS] Real URL: {assets['real_url'][:60]}")
                else:
                    print(f"❌ [MASK_FAIL] Status {resp.status_code}")

            except Exception as e:
                print(f"💥 [DECODER_CRASH] {str(e)}")

        # ==========================================
        #   STAGE 2: PLATFORM-SPECIFIC EXTRACTION
        # ==========================================
        target = assets["real_url"]
        try:
            # --- TELEGRAM / X / TIKTOK / YOUTUBE ---
            # (Same optimized logic as before, using 'target' and explicit decoding)
            if any(x in target for x in ["t.me", "telegram.me", "telegram.com"]):
                clean_tg = target.replace("telegram.com", "t.me").replace("telegram.me", "t.me").split('?')[0]
                if re.search(r'/[^/]+/\d+', clean_tg):
                    assets["video"] = f"{clean_tg}?embed=1"
                
                tg_r = await client.get(clean_tg, timeout=6.0)
                soup = BeautifulSoup(tg_r.text, "html.parser")
                img = soup.find("meta", property="og:image")
                if img: assets["photo"] = img.get("content")

            elif "tiktok.com" in target:
                t_match = re.search(r'video/(\d+)', target)
                if t_match: assets["video"] = f"https://www.tiktok.com/embed/v2/{t_match.group(1)}"
                r = await client.get(target, follow_redirects=True, timeout=8.0)
                soup = BeautifulSoup(r.text, "html.parser")
                img = soup.find("meta", property="og:image")
                if img: assets["photo"] = img.get("content")

            elif any(x in target for x in ["x.com", "twitter.com"]):
                x_match = re.search(r'status/(\d+)', target)
                if x_match: assets["video"] = f"https://platform.twitter.com/embed/Tweet.html?id={x_match.group(1)}"
                r = await client.get(target, headers={"User-Agent": "Mozilla/5.0 (compatible; Googlebot/2.1)"}, timeout=8.0)
                soup = BeautifulSoup(r.text, "html.parser")
                img = soup.find("meta", property="og:image")
                if img: assets["photo"] = img.get("content")

            elif any(x in target for x in ["youtube.com", "youtu.be"]):
                y_match = re.search(r'(?:v=|be/|shorts/)([^&?#/ ]+)', target)
                if y_match:
                    v_id = y_match.group(1)
                    assets["video"] = f"https://www.youtube.com/embed/{v_id}"
                    assets["photo"] = f"https://img.youtube.com/vi/{v_id}/maxresdefault.jpg"

            else:
                # General News Scraper
                async with client.stream("GET", target, follow_redirects=True, timeout=12.0) as resp:
                    if resp.status_code == 200:
                        buffer = await resp.aread()
                        html_str = buffer.decode('utf-8', errors='replace')
                        soup = BeautifulSoup(html_str, "html.parser")
                        img = soup.find("meta", property="og:image") or soup.find("meta", attrs={"name": "twitter:image"})
                        if img: assets["photo"] = img.get("content")

        except Exception as e:
            print(f"💥 [EXTRACTION_ERR] {str(e)}")

        # Final Sanitization
        for k in ["video", "photo"]:
            if assets[k]:
                assets[k] = assets[k].replace('\\/', '/').replace('&amp;', '&')
                if assets[k].startswith('//'): assets[k] = 'https:' + assets[k]

        return assets

    async def llm_triage(self, client, raw_title):
        """
        V25 FIXED GROQ ENGINE:
        Fixed 'string indices' error by treating raw_title as a string, not a list.
        """
        if not GROQ_API_KEY or "YOUR_" in GROQ_API_KEY:
            return {"title": raw_title, "threat": "KEY_MISSING 🔴", "loc_name": "Unknown"}

        url = "https://api.groq.com/openai/v1/chat/completions"
        headers = {"Authorization": f"Bearer {GROQ_API_KEY}", "Content-Type": "application/json"}
        
        system_msg = (
            "You are a military intelligence analyst. "
            "1. Translate to English. 2. Level: LOW/MED/HIGH/CRITICAL. 3. Loc: City, Country. "
            "4. Context: One-sentence tactical background explaining the 'why'. "
            "5. NEVER LEAVE OUT LOCATION AS EMPTY ALWAYS FILLED BASED ON CONTEXT. LEAVING LOCATION EMPTY IS FORBIDDEN"
            "Output ONLY JSON: {\"title\": \"Title\", \"intensity\": \"Level\", \"loc\": \"Location\", \"context\": \"Context\"}"
        )

        payload = {
            "model": "openai/gpt-oss-120b", 
            "response_format": {"type": "json_object"},
            "messages": [
                {"role": "system", "content": system_msg},
                {"role": "user", "content": f"Triage: {raw_title}"}
            ],
            "temperature": 0.0
        }

        try:
            r = await client.post(url, headers=headers, json=payload, timeout=15)
            data = json.loads(r.json()['choices'][0]['message']['content'])
            return {
                "title": data.get("title", raw_title),
                "intensity": data.get("intensity", "MODERATE 🟡"),
                "loc_name": data.get("loc", "Unknown"),
                "context": data.get("context", "Tactical analysis in progress.")
            }
        except:
            return {"title": raw_title, "intensity": "LOW ⚪", "loc_name": "Unknown", "context": "Triage failed."}
        

    async def osm_geocode(self, client, loc_name):
        if not loc_name or loc_name.lower() in ["none", "unknown", "global", "middle east"]: return None, None
        url = f"https://nominatim.openstreetmap.org/search?q={urllib.parse.quote(loc_name)}&format=json&limit=1"
        async with OSM_RATE_LIMIT_LOCK:
            await asyncio.sleep(1.2) 
            try:
                r = await client.get(url, timeout=10)
                data = r.json()
                if data: return float(data[0]["lat"]), float(data[0]["lon"])
            except: pass 
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
                "IndoPac_Info", "CollinSLKoh", "t_shugart", "detresfa_", "duandang",
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
        
        # Build the Google Dork
        # Example: site:x.com (OSINTdefender OR clashreport) (strike OR intercept) when:1h
        accounts_query = " OR ".join(target_accounts)
        keywords_query = " OR ".join(tactical_keywords)
        
        # We target both x.com and twitter.com just in case Google's index is split
        query = f"(site:x.com OR site:twitter.com) ({accounts_query}) ({keywords_query}) when:1h"
        
        # The Google News RSS Uplink
        url = f"https://news.google.com/rss/search?q={urllib.parse.quote(query)}&hl=en-US&gl=US"
        
        all_raw_items = []
        
        try:
            # Jitter to prevent Google from flagging your VPS
            await asyncio.sleep(random.uniform(1.5, 3.0))
            r = await client.get(url, timeout=15)
            feed = feedparser.parse(r.content)
            
            if not feed.entries:
                return [self.create_error_block("ZERO_HITS", "No fresh X/Twitter intel in the last hour.", "X SHADOW")]

            for e in feed.entries[:3]:
                if self.track_viewpoint(e.title, "X_SHADOW", e.link):
                    # FIXED: Appending to all_raw_items instead of undefined raw_items
                    all_raw_items.append({"raw_title": e.title, "src": "X_SHADOW", "url": e.link})
                    
        except Exception as e:
            return [self.create_error_block("X_NODE_ERR", str(e)[:30], "X SHADOW")]

        # Push the cleaned tweets into the Groq Triage engine
        results = await asyncio.gather(*(self.process_intel(client, item) for item in all_raw_items))
        return [res for res in results if res]

    async def fetch_tiktok_stealth(self, client):
        """
        V24: TIKTOK STEALTH MATRIX.
        Uses Google's RSS indexing to bypass TikTok's anti-bot walls.
        Searches by hashtag, extracts the description, and permanently deletes all hashtags.
        """
        import re
        
        # The tactical hashtags you want the engine to monitor
        tactical_tags = [
            '"combat footage"', '"ground truth"', '"geolocated"', 
            '"kinetic strike"', '"loitering munition"', '"artillery duel"',
            '"troop movement"', '"armored column"', '"SAM system"',
            '"air raid sirens"', '"battle damage"', '"frontline update"',
            '"tactical retreat"', '"breakthrough"', '"intercepted"'
        ]
        
        # Format for Google Dorking: (#military OR #conflict OR #geopolitics)
        tag_query = " OR ".join([f"#{t}" for t in tactical_tags])
        
        # site:tiktok.com limits it to TikTok. 
        # inurl:video limits it to actual video posts, avoiding user profiles.
        query = f"site:tiktok.com inurl:video ({tag_query}) when:24h"
        url = f"https://news.google.com/rss/search?q={urllib.parse.quote(query)}&hl=en-US&gl=US"
        
        all_raw_items = []
        try:
            # We add a slight jitter so Google doesn't flag the rapid requests
            await asyncio.sleep(random.uniform(1.0, 2.0))
            r = await client.get(url, timeout=15)
            feed = feedparser.parse(r.content)
            
            if not feed.entries:
                return [self.create_error_block("ZERO_HITS", "No fresh TikTok intel found on these tags.", "TIKTOK OSINT")]
                
            for e in feed.entries[:5]: # Grab the top 5 most relevant hits
                raw_desc = e.title
                
                # --- THE TIKTOK SCRUBBER ---
                # 1. Remove the " - TikTok" or " | TikTok" branding at the end of the string
                clean_desc = re.sub(r'[-|]\s*TikTok\s*$', '', raw_desc, flags=re.IGNORECASE)
                
                # 2. NUKE THE HASHTAGS: Deletes '#' and any word characters attached to it
                clean_desc = re.sub(r'#\w+', '', clean_desc)
                
                # 3. Clean up double spaces left behind by deleted words
                clean_desc = re.sub(r'\s+', ' ', clean_desc).strip()
                
                # Failsafe: If a video was literally JUST hashtags, don't send an empty string
                if len(clean_desc) < 5:
                    clean_desc = "VIDEO UPDATE (Text was entirely hashtags)"
                
                if self.track_viewpoint(clean_desc, "TIKTOK OSINT", e.link):
                    all_raw_items.append({
                        "raw_title": clean_desc,
                        "src": "TIKTOK OSINT",
                        "url": e.link
                    })
                    
        except Exception as e:
             return [self.create_error_block("TT_NODE_ERR", str(e)[:30], "TIKTOK OSINT")]
             
        # Push cleaned descriptions into the Groq Triage engine
        results = await asyncio.gather(*(self.process_intel(client, item) for item in all_raw_items))
        return [res for res in results if res]
        
    async def fetch_telegram_stealth(self, client):
        source_file = "telegram_new_sources.txt"
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
        async with MAX_CONCURRENT_TASKS:
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
        V22 HARDENED: Implements Exponential Backoff, Headers, and Google-Proxy Fallback 
        to bypass GDELT 429 Rate Limiting and blockades.
        """
        url = "https://api.gdeltproject.org/api/v2/doc/doc?query=(tone<-2 OR military OR attack) -sports&mode=artlist&format=json&maxrecords=5"
        
        # 🛡️ THE FIX: GDELT-Specific Headers to prevent instant connection drops
        gdelt_headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
            "Accept": "application/json, text/plain, */*"
        }
        
        # 🛡️ STRATEGY 1: Multistage Retry with Exponential Backoff
        for attempt in range(3): # Try 3 times
            try:
                # Increasing wait time: 3s, 7s, 15s
                wait_time = (attempt * 5) + random.uniform(2.0, 4.0)
                await asyncio.sleep(wait_time)
                
                # Pass the headers here
                r = await client.get(url, headers=gdelt_headers, timeout=20)
                
                if r.status_code == 200:
                    try:
                        data = r.json()
                        articles = data.get('articles', [])
                    except Exception as e:
                        print(f"[!] GDELT JSON Parse Error: {e}")
                        continue
                        
                    if not articles: continue
                    
                    raw_items = []
                    for a in articles:
                        # Defensive dictionary access
                        title = a.get('title', 'Unknown Event')
                        link = a.get('url', '')
                        country = a.get('sourcecountry', 'INTL')
                        
                        if link and self.track_viewpoint(title, f"GDELT ({country})", link):
                            raw_items.append({"raw_title": title, "src": f"GDELT ({country})", "url": link})
                            
                    if raw_items:
                        results = await asyncio.gather(*(self.process_intel(client, item) for item in raw_items))
                        return [res for res in results if res]
                
                elif r.status_code == 429:
                    print(f"[!] GDELT Mainframe Throttled (Attempt {attempt+1}/3). Backing off...")
                    continue 
                    
            except Exception as e:
                # Shorten the error string so it doesn't flood your logs
                print(f"[!] GDELT Connection Error: {str(e)[:40]}")
                continue

        # 🛡️ STRATEGY 2: THE FAILSAFE (The "VisionSphere" Proxy Move)
        print("[⚡] GDELT API Failed. Activating Google-Proxy Fallback...")
        # Using when:1d to ensure it finds hits and avoids the ZERO_RESULTS trap
        fallback_query = "site:gdeltproject.org OR (military conflict) when:1d"
        return await self.fetch_dynamic_crawler(client, fallback_query, "GDELT FAILSAFE")

    async def fetch_premium_matrix(self, client):
        """
        Silent & Sharded Premium Matrix:
        - Removed all logging overhead.
        - Uses 3-node concurrency limit (Semaphore) to stop Groq 429s.
        - Fixed variable scope for e.link and src_label.
        """
        print("[*] Accessing Premium 36-Node Intel Matrix...")
        
        k_size = min(len(PREMIUM_DOMAINS), 5)
        sampled_keys = random.sample(list(PREMIUM_DOMAINS.keys()), k_size)
        all_raw_items = []
        
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/119.0.0.0 Safari/537.36",
            "Accept": "application/rss+xml, application/xml;q=0.9, */*;q=0.8"
        }

        for name in sampled_keys:
            domain = PREMIUM_DOMAINS[name]
            src_label = f"ELITE: {name.upper()}"
            url = f"https://news.google.com/rss/search?q=site:{domain}+(conflict OR military OR breach)+when:7d&hl=en-US&gl=US"
            
            try:
                await asyncio.sleep(random.uniform(0.5, 1.2))
                response = await client.get(url, headers=headers, timeout=10)
                
                if response.status_code == 200:
                    feed = feedparser.parse(response.text)
                    if feed.entries:
                        entry = feed.entries[0]
                        title = entry.title.split(" - ")[0].strip()
                        # Fixed variable: using entry.link
                        if self.track_viewpoint(title, src_label, entry.link):
                            all_raw_items.append({
                                "raw_title": title, 
                                "src": src_label, 
                                "url": entry.link
                            })
            except:
                continue # Silent fail for nodes to keep terminal clean

        if not all_raw_items:
            return []

        # Use a Semaphore to prevent Groq '429 Too Many Requests'
        sem = asyncio.Semaphore(3) 

        async def safe_process(item):
            async with sem:
                return await self.process_intel(client, item)

        results = await asyncio.gather(*(safe_process(item) for item in all_raw_items), return_exceptions=True)
        return [res for res in results if res and not isinstance(res, Exception)]

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

    def ensure_opml_exists(self):
        # PROTECT: If the 2.5MB monster already exists, don't overwrite it!
        opml_path = "sources.opml"
        if os.path.exists(opml_path): 
            print(f"[+] Leviathan Matrix Detected ({os.path.getsize(opml_path)/1024/1024:.2f} MB). Skipping rebuild.")
            return
            
        print("[!] Building the 10,000+ Node Tactical Matrix...")
        
        # 1. Start XML with Proper Encoding
        xml_content = '<?xml version="1.0" encoding="UTF-8"?>\n<opml version="1.0">\n<head><title>VisionSphere Leviathan V31</title></head>\n<body>\n'

        # --- FOLDER 1: THE BROAD-NET BLANKET (The "Global Pulse") ---
        xml_content += '<outline text="GLOBAL_BROAD_NET" title="Tactical Search Blanket">\n'
        for country in ALL_NATIONS:
            # We keep these broad to catch breaking news Google hasn't indexed by site yet
            query = f'"{country}" (military OR conflict OR strike OR unrest OR attack OR "state of emergency")'
            safe_query = urllib.parse.quote(query)
            # PRO-TIP: We use +when:24h here for the blanket to catch the absolute latest
            rss_url = f"https://news.google.com/rss/search?q={safe_query}+when:24h&gl=US&ceid=US:en"
            xml_content += f'    <outline type="rss" text="PULSE: {country}" xmlUrl="{rss_url}" />\n'
        xml_content += '</outline>\n'

        # --- FOLDER 2: THE HARVESTED PRECISION NODES (The 10k+) ---
        # This part assumes you have your 'master_matrix' from the Harvester Script
        # If you're running this as a standalone, this loop processes your harvested dict
        if hasattr(self, 'master_matrix') and self.master_matrix:
            print(f"[*] Injecting {sum(len(v) for v in self.master_matrix.values())} Precision Nodes...")
            for country, domains in self.master_matrix.items():
                folder_label = f"PRECISION: {country}"
                xml_content += f'<outline text="{folder_label}" title="{folder_label}">\n'
                
                for domain, name in domains.items():
                    # Every domain gets the Google Proxy treatment
                    safe_site = urllib.parse.quote(f"site:{domain}")
                    rss_url = f"https://news.google.com/rss/search?q={safe_site}&gl=US&ceid=US:en"
                    xml_content += f'    <outline type="rss" text="{name} [{domain}]" xmlUrl="{rss_url}" />\n'
                
                xml_content += '</outline>\n'
        else:
            print("[!] Warning: No Harvested Matrix found. Generating Broad-Net only.")

        # 3. Finalize and Save
        xml_content += '</body>\n</opml>'
        
        # Ensure directory exists
        os.makedirs(os.path.dirname(opml_path), exist_ok=True)
        
        with open(opml_path, "w", encoding="utf-8") as f:
            f.write(xml_content)
        
        print(f"[#] Leviathan Matrix Deployed to {opml_path}")

    async def parse_and_fetch_opml(self, client):
        import re, html, feedparser, random
        from dataclasses import dataclass

        opml_path = "sources.opml" 
        @dataclass
        class MockFeed:
            title: str
            url: str

        feeds = []
        try:
            with open(opml_path, 'r', encoding='utf-8') as f:
                content = f.read()
                pattern = r'text="([^"]+)"\s+title="[^"]+"\s+xmlUrl="([^"]+)"'
                matches = re.findall(pattern, content)
                for text, raw_url in matches:
                    feeds.append(MockFeed(title=text, url=html.unescape(raw_url)))
        except Exception as e:
            return [self.create_error_block("OPML_READ_ERR", str(e)[:30], "OPML_SYSTEM")]

        if not feeds:
            return [self.create_error_block("OPML_EMPTY", "No nodes in OPML", "OPML_SYSTEM")]

        # Sharding
        active_feeds = random.sample(feeds, min(len(feeds), 10))
        sem = asyncio.Semaphore(5)
        all_raw_items = []
        node_diagnostics = []

        async def fetch_node(f):
            async with sem:
                try:
                    await asyncio.sleep(random.uniform(1.0, 2.0)) 
                    r = await client.get(f.url, timeout=12, follow_redirects=True)
                    
                    if r.status_code != 200:
                        node_diagnostics.append(self.create_error_block(f"HTTP_{r.status_code}", "Node Rejected", f.title))
                        return

                    parsed = feedparser.parse(r.content)
                    if not parsed.entries:
                        # Quiet nodes don't need to be error blocks (too much noise)
                        return

                    # Success - Grab latest intel
                    entry = parsed.entries[0]
                    title = entry.title.split(" - ")[0].strip()
                    
                    if self.track_viewpoint(title, f.title, entry.link):
                        all_raw_items.append({"raw_title": title, "src": f.title, "url": entry.link})
                
                except Exception as e:
                    node_diagnostics.append(self.create_error_block("NODE_ERR", str(e)[:20], f.title))

        # 5. EXECUTE & TRIAGE
        await asyncio.gather(*(fetch_node(f) for f in active_feeds))

        results = []
        if all_raw_items:
            print(f"[+] Triage in progress for {len(all_raw_items)} OPML hits...")
            results = await asyncio.gather(*(self.process_intel(client, item) for item in all_raw_items))
        
        # Merge successful triages with diagnostic error blocks
        return [res for res in results if res] + node_diagnostics

    # ==========================================
    # 4. EXECUTION MATRIX
    # ==========================================
    
    async def run_worker(self, client, fetch_func, label, interval):
        """
        V31: THE UNIFIED STREAM
        Handles fetching, deduplication, Supabase syncing, and console output.
        """
        print(f"[+] Worker {label} streaming...")
        
        while self.is_running:
            try:
                results = await fetch_func(client)
                
                if results:
                    for item in results:
                        url = item['attribution']['url']
                        
                        # 1. DEDUPLICATION CHECK
                        if url not in self.processed_urls:
                            self.processed_urls.append(url)
                            
                            # 2. SUPABASE SYNC
                            # Pushes intel to the cloud before local printing
                            await self.push_to_supabase(item)
                            
                            # 3. CONSOLE TELEMETRY (Rich Output)
                            print(f"\n{'#'*40}")
                            print(f"📡 SOURCE: {item['attribution']['source']}")
                            print(f"🔥 INTENSITY: {item['intensity']}")
                            print(f"📍 LOCATION: {item['location']['name']} [{item['location']['lat']}, {item['location']['lon']}]")
                            print(f"📝 TITLE: {item['title']}")
                            print(f"📖 CONTEXT: {item['context']}")
                            
                            # Bulletproof Media Extraction
                            media = item.get('media', {})
                            if media.get('video'):
                                print(f"🎬 VIDEO: {media.get('video')}")
                                
                            if media.get('photo'):
                                print(f"📸 PHOTO: {media.get('photo')}")
                                
                            print(f"🔗 URL: {url}")
                            print(f"{'#'*40}\n")
                            
                            # 4. MEMORY MANAGEMENT
                            if len(self.processed_urls) > self.max_history:
                                self.processed_urls.pop(0) 

                # Wait for the next freshness cycle
                await asyncio.sleep(interval)
                
            except Exception as e:
                print(f"[!] Worker {label} Error: {e}")
                # Cool-down period on failure to prevent rapid-fire crashing
                await asyncio.sleep(30)

    async def health_check_server(self):
        """
        Render.com requires an open port to keep a 'Web Service' alive.
        """
        from aiohttp import web
        async def handle(request):
            return web.Response(text=f"VisionSphere Streaming: {len(self.processed_urls)} items processed.")
        
        app = web.Application()
        app.router.add_get('/', handle)
        runner = web.AppRunner(app)
        await runner.setup()
        # Render provides a PORT environment variable
        port = int(os.environ.get("PORT", 8080))
        site = web.TCPSite(runner, '0.0.0.0', port)
        print(f"[*] Health Check Server live on port {port}")
        await site.start()

    async def execute_stream(self):
        """
        V30: THE CONTINUOUS LEVIATHAN
        Launches all fetchers as independent, parallel streams.
        """
        self.ensure_opml_exists()
        print(f"\n{'='*60}\n VISIONSPHERE STREAMING ENGINE STARTING\n{'='*60}\n")

        async with httpx.AsyncClient(timeout=30.0, headers=self.headers, follow_redirects=True) as client:
            # Define your workers and their "Freshness Intervals"
            workers = [
                (self.fetch_x_stealth, "X_SHADOW", 420),          # Every 7 mins
                (self.fetch_telegram_stealth, "TELEGRAM", 420),    # Every 7 mins
                (self.fetch_tiktok_stealth, "TIKTOK", 420),        # Every 7 mins
                (self.fetch_premium_matrix, "PREMIUM", 420),       # Every 7 mins
                (self.parse_and_fetch_opml, "GLOBAL_OPML", 420),
                (self.fetch_gdelt, "GDELT", 420)
            ]

            # Start the Health Check (Crucial for Render.com)
            asyncio.create_task(self.health_check_server())

            # Launch all fetchers as independent tasks
            tasks = []
            for func, label, interval in workers:
                tasks.append(asyncio.create_task(self.run_worker(client, func, label, interval)))
                # 🛑 THE FIX: Wait 10 seconds before starting the next worker (Saves RAM)
                await asyncio.sleep(10)

            # Keep the main loop alive
            await asyncio.gather(*tasks)

# --- [ TEST TRIGGER ] ---
if __name__ == "__main__":
    engine = VisionSphereV18_5()
    try:
        # We use execute_stream() instead of execute() for testing the continuous flow
        asyncio.run(engine.execute_stream())
    except KeyboardInterrupt:
        print("\n[!] User terminated the stream. Shutting down...")