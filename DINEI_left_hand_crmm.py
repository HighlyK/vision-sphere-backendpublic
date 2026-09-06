import os
import asyncio
import random
import urllib.parse
import sys
import re
import time
import json
from datetime import datetime
from dotenv import load_dotenv
from curl_cffi.requests import AsyncSession
from bs4 import BeautifulSoup
from bs4 import BeautifulSoup
from cerebras.cloud.sdk import AsyncCerebras
import itertools
import httpx
import cohere
from openai import AsyncOpenAI
from pydantic import *
from typing import List, Optional, Any, Dict
import pandas as pd
from imfdatapy.imf import IFS
from EasyDorkerCustom import EasyDorkerCustom
load_dotenv()
current_yr = datetime.now().year
# ==========================================
# 🛡️ PYDANTIC DOSSIER SCHEMAS (LEFT HAND)
# ==========================================
# ==========================================
# 🛡️ NETWORK RESILIENCE LAYER
# ==========================================
BROWSER_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-User": "?1"
}

class ElasticBase(BaseModel):
    """Base Config for all models to ignore 'hallucinated' extra fields"""
    model_config = ConfigDict(
        extra='ignore',
        populate_by_name=True,
        coerce_numbers_to_str=True # Prevents int/str mismatch crashes
    )

class CountryStatusData(ElasticBase):
    status: str = "Unknown"
    hl: str = "en"
    gl: str = "us"
    iso_2: str = "US"

class FactionList(ElasticBase):
    factions: List[str] = Field(default_factory=list)

class DataPoint(ElasticBase):
    label: str = "Metric"
    value: Any = 0
    unit: str = "N/A"

class LeaderInfo(ElasticBase):
    name: str = "Unknown"
    title: str = "Official"
    photo_url: Optional[str] = None
    influence_score: Any = 5 
    bio_snippet: str = "N/A"

class PartyControl(ElasticBase):
    name: str
    seats: int
    total_seats: int
    flag_url: Optional[str] = None
    notable_members: List[str] = Field(default_factory=list)

class GovData(ElasticBase):
    system_type: str = "Unknown"
    capital: str = "Unknown"
    flag_url: Optional[str] = None
    key_leadership: List[LeaderInfo] = Field(default_factory=list)
    parliament_structure: str = "Unknown"
    legal_system: str = "Unknown"
    party_distribution: List[PartyControl] = Field(default_factory=list)
    active_sanctions: List[str] = Field(default_factory=list)

class EconomicData(ElasticBase):
    gdp_history: List[DataPoint] = Field(default_factory=list)
    population_history: List[DataPoint] = Field(default_factory=list)
    inflation_rate: str = "Unknown"
    currency_trend: List[DataPoint] = Field(default_factory=list) 
    primary_exports: List[str] = Field(default_factory=list)
    major_trading_partners: List[str] = Field(default_factory=list)
    economic_status: str = "Volatile"
    debt_to_gdp: str = "N/A"

class FactionData(ElasticBase):
    name: str = "Unknown Faction"
    leader: Optional[str] = "Unknown"  # Add Optional
    leader_photo_url: Optional[str] = None
    estimated_manpower: Optional[str] = "N/A"  # Add Optional
    weaponry_tier: Optional[str] = "Unknown"  # Add Optional
    primary_objectives: List[str] = Field(default_factory=list)
    controlled_territory: Optional[str] = "N/A"  # Add Optional
    allies: List[str] = Field(default_factory=list)
    influence_score: Any = 5

class StrategicDossier(ElasticBase):
    country: str
    status: str
    last_updated: str
    macro_economy: Optional[EconomicData] = None
    governance: Optional[GovData] = None
    active_factions: Dict[str, FactionData] = Field(default_factory=dict)
    latest_events: List[Dict[str, Any]] = Field(default_factory=list)

def parse_llm_payload(raw_content: Any, model_class: Any) -> Any:
    """Standardized Pydantic V2 extraction logic with recursion shield."""
    if isinstance(raw_content, model_class):
        return raw_content
    
    content_str = ""
    if isinstance(raw_content, str):
        content_str = raw_content
    elif hasattr(raw_content, 'text'):
        content_str = raw_content.text
    else:
        return model_class()
        
    try:
        cleaned = re.sub(r'```json|```', '', content_str).strip()
        match = re.search(r'(\{.*\})', cleaned, re.DOTALL)
        json_str = match.group(1) if match else cleaned
        return model_class.model_validate_json(json_str)
    except Exception:
        return model_class()

class CFNetworkAdapter:
    """Masks outbound requests through Cloudflare with exponential backoff."""
    def __init__(self, worker_url=None):
        raw_url = "https://shiny-union-766e.rmadris473.workers.dev/"
        if raw_url and not raw_url.startswith(("http://", "https://")):
            raw_url = f"https://{raw_url}"
        self.cf_worker_url = raw_url

    async def get(self, session: httpx.AsyncClient, target_url: str, retries=2, **kwargs):
        if not target_url.startswith(("http://", "https://")):
            target_url = f"https://{target_url}"

        if self.cf_worker_url:
            if 'params' in kwargs and kwargs['params']:
                qs = urllib.parse.urlencode(kwargs.pop('params'))
                connector = "&" if "?" in target_url else "?"
                target_url = f"{target_url}{connector}{qs}"
            request_url = f"{self.cf_worker_url.rstrip('/')}/"
            kwargs['params'] = {"url": target_url}
        else:
            request_url = target_url

        for attempt in range(retries):
            await asyncio.sleep(random.uniform(0.5, 1.5))
            try:
                resp = await session.get(request_url, **kwargs)
                if resp.status_code in [200, 401, 403, 404, 429, 500, 503]:
                    return resp
                await asyncio.sleep(1.5 ** attempt)
            except Exception:
                pass
        return None

class IntelBrain:
    """
    ULTRA-STABLE ELITE INTEL ENGINE
    Features OSINT-grade complex prompting, robust JSON scavenging, 
    and native asynchronous dual-engine redundancy.
    """
    def __init__(self, target_country: str):
        self.target_country = target_country
        self.cerebras_clients = []
        self.samba_clients = []
    
        # Load Cerebras Keys (1-10)
        for i in range(1, 11):
            c_key = os.getenv(f"CERABRAS_KEY_{i}")
            if c_key:
                self.cerebras_clients.append(AsyncCerebras(api_key=c_key))
                
        for i in range(1, 11):
            s_key = os.getenv(f"SAMBA_KEY_{i}") or os.getenv("SAMBANOVA_API_KEY")
            if s_key and s_key not in [getattr(c, 'api_key', '') for c in self.samba_clients]:
                self.samba_clients.append(AsyncOpenAI(base_url="https://api.sambanova.ai/v1", api_key=s_key))
        
        if not self.cerebras_clients and not self.samba_clients:
            print(f"[🚨 CRITICAL] No API keys found for IntelBrain!")
        else:
            print(f"[✅] IntelBrain Synchronized: {len(self.cerebras_clients)} Cerebras | {len(self.samba_clients)} SambaNova engines loaded.")

    async def _query_llm(self, prompt: str, temperature: float = 0.1) -> str:
        """
        Rotates keys and models seamlessly across Cerebras and SambaNova with rate-limit mitigation.
        """
        available_engines = []
        if self.cerebras_clients: available_engines.append("cerebras")
        if self.samba_clients: available_engines.append("samba")
        
        if not available_engines: return ""

        for attempt in range(4):
            engine = random.choice(available_engines)
            try:
                if engine == "cerebras":
                    client = random.choice(self.cerebras_clients)
                    resp = await client.chat.completions.create(
                        model="zai-glm-4.7",
                        messages=[{"role": "user", "content": prompt}],
                        temperature=temperature
                    )
                    return resp.choices[0].message.content
                    
                elif engine == "samba":
                    client = random.choice(self.samba_clients)
                    resp = await client.chat.completions.create(
                        model="DeepSeek-V3.1",
                        messages=[{"role": "user", "content": prompt}],
                        temperature=temperature
                    )
                    return resp.choices[0].message.content

            except Exception as e:
                err_str = str(e).lower()
                if "429" in err_str or "too_many_requests" in err_str:
                    cooling_time = 10.0 * (attempt + 1) + random.uniform(1.0, 2.0)
                    print(f"   [⏳ {engine.upper()} 429] Target queue saturated. Backing off for {cooling_time:.1f}s...")
                    await asyncio.sleep(cooling_time)
                else:
                    print(f"   [⚠️] {engine.upper()} API Error: {e}")
                    await asyncio.sleep(2.0)
                    
        return ""

    def _extract_json(self, text: str, default_factory: Any) -> Any:
        """
        Surgical JSON extraction. Bypasses LLM conversational filler,
        fixes trailing commas, and rescues truncated payloads.
        """
        try:
            match = re.search(r'(\{.*\}|\[.*\])', text, re.DOTALL)
            if match:
                clean_content = match.group(0)
                clean_content = re.sub(r',\s*([\}\]])', r'\1', clean_content)
                return json.loads(clean_content)
        except Exception:
            pass
        return default_factory

    async def assess_country_status(self) -> Dict[str, Any]:
        """Deep geopolitical scope of the target nation."""
        print(f"\n[🌐 SCOPE] AI Global Analyst: Deep-Scoping {self.target_country}...")
        prompt = f"""
        ### ROLE: SENIOR GEOPOLITICAL RISK ANALYST
        ### TARGET: {self.target_country}
        ### DATE CONTEXT: May 2026
        ### MISSION:
        Conduct a high-fidelity ontological assessment of the target's current macroscopic stability. 
        You must evaluate military mobilization, civil unrest, state of emergency declarations, 
        insurgency momentum, and foreign proxy interventions.
        ### CLASSIFICATION PROTOCOL:
        Assign exactly ONE of the following status codes based on these strict definitions:
        - CIVIL_WAR: Widespread, sustained kinetic combat between the state and organized non-state actors controlling territory.
        - INVASION: Active presence of hostile foreign military elements violating sovereignty.
        - UNREST: High-frequency protests, riots, martial law, or localized asymmetric attacks without total state collapse.
        - PEACEFUL: Standard administrative governance with negligible kinetic disruption.
        ### OUTPUT MANDATE:
        Return a strict, parsable JSON object. Do not include markdown, explanations, or preamble.
        {{
            "status": "[CLASSIFICATION]",
            "hl": "en",
            "gl": "US",
            "iso_2": "[Exact 2-letter ISO 3166-1 alpha-2 country code]"
        }}
        """
        
        raw = await self._query_llm(prompt)
        data = self._extract_json(raw, {"status": "UNREST", "iso_2": self.target_country[:2].upper()})
        print(f"   [🌐 SCOPE-DEBUG] Status: {data.get('status')} | ISO: {data.get('iso_2')}")
        return data

    async def identify_factions(self) -> List[str]:
        """Maps the order of battle and active insurgency networks."""
        prompt = f"""
        ### ROLE: TACTICAL BATTLEFIELD MAPPER
        ### TARGET THEATER: {self.target_country}
        ### DATE CONTEXT: May 2026
        ### MISSION:
        Execute a comprehensive Order of Battle (OOB) extraction. Identify the absolute most critical, 
        heavily armed, and politically significant factions actively operating within the target theater.
        
        ### TARGET PARAMETERS:
        1. Include the primary state military apparatus (e.g., 'National Armed Forces', 'Tatmadaw', 'SAF').
        2. Include the most lethal non-state armed groups, insurgency coalitions, or separatist militias.
        3. Do not list political parties unless they possess a direct, active paramilitary wing.
        4. Limit the output to the TOP 4 entities based on troop count, territorial control, and kinetic activity.
        ### OUTPUT MANDATE:
        Return ONLY a raw JSON array of strings containing the formal names of these factions. 
        No other text is permitted.
        ["Faction Name 1", "Faction Name 2", "Faction Name 3"]
        """
        
        raw = await self._query_llm(prompt)
        return self._extract_json(raw, ["National Armed Forces", "Local Resistance Groups"])

    async def discover_entities(self, target_country: str = None) -> Dict[str, str]:
        """Extracts De Jure vs De Facto leadership and financial baselines."""
        target = target_country or self.target_country
        prompt = f"""
        ### ROLE: INTELLIGENCE TARGETEER & FINANCIAL ANALYST
        ### TARGET JURISDICTION: {target}
        ### DATE CONTEXT: May 2026
        ### MISSION:
        Identify the exact formal nomenclature for key governance and economic pillars within the jurisdiction. 
        You must distinguish between De Jure (legally recognized) and De Facto (actually in control) entities 
        if a coup, shadow government, or transitional council exists.
        ### EXTRACTION REQUIREMENTS:
        1. regime: The formal name of the current ruling body, junta, or administrative council.
        2. opposition: The primary recognized shadow government, coalition, or unified political opposition front.
        3. leader: The current acting Head of State or supreme military commander. Provide the full formal name.
        4. currency_iso: The official 3-letter fiat currency code (e.g., USD, MMK, RUB).
        ### OUTPUT MANDATE:
        Return ONLY a strict JSON object. No preamble, no postscript.
        {{
            "regime": "...",
            "opposition": "...",
            "leader": "...",
            "currency_iso": "..."
        }}
        """
        
        raw = await self._query_llm(prompt)
        return self._extract_json(raw, {"regime": target, "opposition": "Opposition", "leader": "Unknown", "currency_iso": "USD"})

    async def generate_local_dorks(self, target: str, category: str) -> List[str]:
        """Generates elite, native-language cyber-OSINT search vectors."""
        current_date_str = datetime.now().strftime("%B %Y")
        prompt = f"""
        ### ROLE: ELITE CYBER-OSINT OPERATOR
        ### TARGET: {target}
        ### DOMAIN CATEGORY: {category}
        ### CURRENT DATE: {current_date_str}
        ### MISSION:
        Engineer high-precision, Google/DuckDuckGo search engine dorks designed to bypass 
        state-sponsored media proxies and surface raw, unfiltered intelligence, deep-web leaks, 
        and informal shadow-economy data.
        ### TACTICAL DIRECTIVES:
        1. NATIVE SCRIPT INJECTION: You MUST translate the target name and key terms into the 
           target's native language script (e.g., Arabic, Burmese, Cyrillic, Spanish). 
           Local truth is rarely published in English.
        2. CATEGORY EXPLOITATION:
           - If ECONOMY: Target strings like "black market rate", "parallel exchange", "price of bread", "fuel shortage" in the native script.
           - If LEADERSHIP: Target strings like "leaked decree", "cabinet reshuffle", "sanctions list", "flight tracking".
           - If FACTION: Target strings like "filetype:kml", "filetype:pdf", "order of battle", "telegram leak", "combat footage", "territory map".
        3. AVOID NOISE: Exclude generic terms like 'news' or 'update'. Use operators like `site:`, `filetype:`, or exact match quotes `""`.
        ### OUTPUT MANDATE:
        Return a JSON object containing an array of exactly 5 highly lethal search strings.
        {{
            "dorks": [
                "\"native_script_target\" informal market rate 2026",
                "\"target_name\" \"order of battle\" filetype:pdf",
                "..."
            ]
        }}
        """
        
        raw = await self._query_llm(prompt, temperature=0.3)
        data = self._extract_json(raw, {})
        return data.get("dorks", [f'"{target}" {category} leak OR report'])

    async def generate_dinei_profile(self, status: str) -> Dict[str, str]:
        """Engineers the algorithmic filter for real-time SIGINT scraping."""
        await asyncio.sleep(random.uniform(0.5, 1.5))
        
        prompt = f"""
        ### ROLE: SIGNALS INTELLIGENCE (SIGINT) ROUTER
        ### TARGET DOMAIN: {self.target_country}
        ### CURRENT THREAT LEVEL: {status}
        ### MISSION:
        Construct a DINEI (Dynamic Intelligence Network Extraction Interface) Profile. 
        This profile acts as the semantic filter for our autonomous web-scraping swarm, 
        dictating exactly what data nodes to ingest and which to ignore.
        ### ENGINEERING PROTOCOL:
        1. 'name': A formalized, tactical title for this specific intelligence feed.
        2. 'goal': A 2-sentence mission statement defining the exact intelligence gap this monitor fills.
        3. 'tags': A highly comma-separated list of 10-15 hyper-specific keywords. 
           - If status is UNREST/CIVIL_WAR: Include kinetic terms (airstrike, protest, barricade, drone, casualty).
           - If status is PEACEFUL: Include structural terms (election, GDP, trade agreement, legislation).
           - ALWAYS include informal economy markers (inflation, shortage, black market).
        4. 'vibe': The analytical lens (e.g., 'kinetic_military', 'macro_economic', 'civil_unrest').
        ### OUTPUT MANDATE:
        Return ONLY valid JSON.
        {{
            "name": "...",
            "goal": "...",
            "tags": "tag1, tag2, tag3...",
            "vibe": "..."
        }}
        """
        
        raw = await self._query_llm(prompt, temperature=0.4)
        return self._extract_json(raw, {
            "name": f"{self.target_country} Tactical Monitor",
            "goal": "Continuous extraction of emergent geopolitical events.",
            "tags": f"{self.target_country}, conflict, market volatility, unrest, leadership",
            "vibe": "analytical"
        })

    async def fetch_dinei_intel(self, profile: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Integrated News Fetcher: Consumes the Brain's SIGINT profile."""
        try:
            import sys
            import os
            import httpx
            
            try:
                from news_engine_vs import DINEI_MOTHER
            except Exception as import_err:
                print(f"   [🚨 IMPORT FATAL] Found the file, but it crashed on load: {import_err}")
                import traceback
                traceback.print_exc()
                return []

            engine = DINEI_MOTHER()
            print(f"   [📡 SIGINT] Initiating capture with profile: {profile.get('name', 'Tactical Monitor')}")
            
            # 3. FIX THE MISSING ARGUMENT: news_engine_vs requires a 'client' to be passed
            async with httpx.AsyncClient() as dummy_client:
                raw_items = await engine.fetch_and_process_dinei(client=dummy_client, config=profile)
            
            processed = []
            for item in (raw_items or [])[:12]:
                processed.append({
                    "title": item.get('title', 'Intelligence Intercept'),
                    "timestamp": item.get('published', datetime.now().isoformat()),
                    "source": item.get('source', 'SHADOW_OSINT'),
                    "url": item.get('url', '#'),
                    "context": item.get('context', 'Algorithmic extraction complete.')
                })
            return processed
            
        except Exception as e:
            print(f"   [!] DINEI_FETCH_ERROR: {e}")
            import traceback
            traceback.print_exc()
            return []

class CohereAnalyst:
    """Deep synthesis of raw text into structured strategic profiles, diversified across Cohere, OpenRouter, and OpenZen."""
    def __init__(self, max_concurrent_tasks=4, target_country=None):
        self.semaphore = asyncio.Semaphore(max_concurrent_tasks)
        self.target_country = target_country
        
        self.providers = {"cohere": [], "openrouter": [], "openzen": []}
        
        # 1. Load Cohere Keys (1-10)
        for i in range(1, 11):
            k = os.getenv(f"COHERE_KEY_{i}")
            if k: self.providers["cohere"].append(cohere.AsyncClientV2(api_key=k))
            
        # 2. Load OpenRouter Keys (1-10)
        for i in range(1, 11):
            k = os.getenv(f"OPENROUTER_KEY_{i}")
            if k: self.providers["openrouter"].append(AsyncOpenAI(base_url="https://openrouter.ai/api/v1", api_key=k))

        # 3. Load OpenZen Keys (1-10)
        for i in range(1, 11):
            k = os.getenv(f"OPENZEN_KEY_{i}")
            # Configured to hit the OpenZen endpoint provided by opencode.ai
            oz_url = os.getenv("OPENZEN_BASE_URL", "https://opencode.ai/zen/v1") 
            if k: self.providers["openzen"].append(AsyncOpenAI(base_url=oz_url, api_key=k))
            
        total_keys = sum(len(v) for v in self.providers.values())
        print(f"   [✅] Analyst Matrix Loaded: {total_keys} keys across 3 providers.")

    async def _safe_api_call(self, prompt: str, model_class: Any, retries=5):
        # Filter out empty providers
        available_providers = [p for p, clients in self.providers.items() if clients]
        if not available_providers:
            print("   [⚠️ CRITICAL] NO API KEYS FOUND FOR ANALYST. Returning skeleton.")
            return model_class()
            
        current_prompt = prompt
        for attempt in range(retries):
            # Randomly pick provider and client to spread the load globally
            provider_name = random.choice(available_providers)
            client = random.choice(self.providers[provider_name])
            
            await asyncio.sleep(random.uniform(1.0, 3.0))
            try:
                async with self.semaphore:
                    extracted_text = ""
                    
                    if provider_name == "cohere":
                        resp = await client.chat(
                            model="command-a-plus-05-2026",
                            messages=[{"role": "user", "content": current_prompt}],
                            temperature=0.1,
                            response_format={"type": "json_object"},
                            p=0.01
                        )
                        extracted_text = "".join(item.text for item in resp.message.content if hasattr(item, 'text') and item.text)
                        
                    elif provider_name == "openrouter":
                        resp = await client.chat.completions.create(
                            model="gpt-oss-120b",
                            messages=[{"role": "user", "content": current_prompt}],
                            temperature=0.1,
                            response_format={"type": "json_object"}
                        )
                        extracted_text = resp.choices[0].message.content
                        
                    elif provider_name == "openzen":
                        resp = await client.chat.completions.create(
                            # Updated to the correct ultra-free model slug
                            model="nemotron-3-ultra-free",
                            messages=[{"role": "user", "content": current_prompt}],
                            temperature=0.1,
                            response_format={"type": "json_object"}
                        )
                        extracted_text = resp.choices[0].message.content
                    
                    # Clean and validate
                    json_str = re.sub(r"^```json|```$", "", extracted_text.strip(), flags=re.IGNORECASE).strip()
                    parsed_result = model_class.model_validate_json(json_str)
                    
                    if getattr(parsed_result, 'capital', None) == "Unknown" and attempt < retries - 1:
                        raise ValueError("LLM returned empty 'Unknown' placeholders instead of real data.")
                        
                    return parsed_result
                    
            except Exception as e:
                err_str = str(e).lower()
                is_429 = "429" in err_str or "too_many_requests" in err_str or "rate limit" in err_str
                is_auth = "401" in err_str or "authenticationerror" in err_str or "unauthorized" in err_str

                print(f"   [⚕️ SELF-HEALING] {provider_name.upper()} Attempt {attempt+1} failed: {type(e).__name__}.")
                
                if is_429:
                    cooling_time = 8.0 * (attempt + 1) + random.uniform(1.0, 3.0)
                    print(f"   [⏳ {provider_name.upper()} 429] Rate limited. Cooling down for {cooling_time:.1f}s...")
                    await asyncio.sleep(cooling_time)
                elif is_auth:
                    print(f"   [🚨 {provider_name.upper()} AUTHENTICATION] API Key rejected or invalid Base URL. Skipping prompt mutation...")
                    await asyncio.sleep(2.0)
                else:
                    print(f"   [⚠️ Format Error] Structural issue detected on {provider_name.upper()}. Mutating prompt blueprint...")
                    current_prompt = prompt + f"\n\nERROR IN PREVIOUS OUTPUT:\n{e}\nFIX THE JSON STRUCTURE AND ENSURE NO PLACEHOLDERS ARE USED."
                    await asyncio.sleep(1.0 ** attempt)
                
        print(f"   [⚠️ CRITICAL] Failed to heal data structure after {retries} attempts. Returning safe skeleton.")
        return model_class()
        
    async def synthesize_macro_economy(self, target: str, intel_blobs: str):
        current_yr = datetime.now().year
        prompt = f"""
        ### ROLE: ELITE MACROECONOMIC FORECASTER
        ### OBJECTIVE: FORCED DATA EXTRACTION & INTERPOLATION FOR {target}
        ### DATA SOURCE: {intel_blobs}
        ### DATE: May {current_yr}
        ### MANDATORY EXTRACTION PROTOCOLS:
        1. **15-YEAR HARD RECONSTRUCTION ({current_yr-15} to {current_yr})**:
           - Every single year MUST have a unique numeric entry.
           - If a year is not in the text, you MUST calculate a linear interpolation based on the closest available years. 
           - **CRITICAL**: If the text describes "economic collapse," "sanctions," or "unrest" in 2024-2026, you MUST reflect a downward trend in the 2025/2026 GDP values. DO NOT carry over 2024 values.
        2. **ZERO-TOLERANCE NUMERICS**:
           - **EXCHANGE RATE**: If the target is not the USA, a rate of "1.0" is a HARD FAILURE. Extract the actual market or black-market rate (e.g., MMK/USD should be ~3000-5000, not 1). Scour the text for currency devaluation mentions.
           - **GDP/POP**: Must be pure integers/floats. No strings.
        3. **FORCED ANALYSIS**:
           - "economic_status": Connect the dots. If there is "UNREST" (status), explain exactly how that is draining the GDP or inflating the currency.
        ### FORBIDDEN STRINGS (DO NOT USE):
        - "Unknown", "N/A", "Not specified", "0.0", "1.0", "Linear Interpolation Estimate".
        ### STRICT JSON OUTPUT (NO PLACEHOLDERS):
        {{
            "gdp_history": [
                {{"label": "Year", "value": 0.0, "unit": "USD"}}
            ],
            "population_history": [
                {{"label": "Year", "value": 0, "unit": "Count"}}
            ],
            "inflation_rate": "Numeric % (e.g. '12.4%')",
            "currency_trend": [
                {{"label": "May 2026 Rate", "value": 0.0, "unit": "ISO/USD"}}
            ],
            "primary_exports": ["Actual Commodity 1", "Actual Commodity 2"],
            "major_trading_partners": ["Actual Nation 1", "Actual Nation 2"],
            "economic_status": "High-density analysis of conflict-economy nexus.",
            "debt_to_gdp": "Numeric %"
        }}
        """
        return await self._safe_api_call(prompt, EconomicData)

    async def synthesize_governance(self, target: str, intel_blobs: str):
        """
        REGIME ARCHITECT PROTOCOL: ZERO-LAZINESS EXTRACTION
        Forced population of all metrics. Placeholders result in logic-fail.
        """
        current_date = datetime.now().strftime("%B %Y")
        
        prompt = f"""
        ### IDENTITY: SUPREME GEO-INT ARCHITECT
        ### OBJECTIVE: CRITICAL REGIME MAPPING FOR {target}
        ### DATA SOURCE: {intel_blobs}
        ### TIMESTAMP: {current_date}
        ### 1. NUMERIC INTEGRITY MANDATE (NO ZEROS ALLOWED):
        - **TOTAL_SEATS**: This field MUST represent the nominal/constitutional capacity of the legislature (e.g., Myanmar=440, Thailand=500). Outputting '0' is a CRITICAL FAILURE. Use your internal knowledge of {target}'s constitution if the text is silent.
        - **INFLUENCE_SCORE**: Assign a logic-based integer (1-10). 
          - Military Junta Leader / Supreme Leader: 10
          - Prime Minister / President: 9
          - Key General / Major Opposition Leader: 7-8
          - MANDATORY: This value MUST NOT be 0.
        - **SEATS**: Use the actual current control count. If a party is banned, seats = 0, but `total_seats` MUST remain the full chamber size.
        ### 2. ASSET FORGING (WIKIMEDIA PROTOCOL):
        - **NO BROKEN LINKS**: You are FORBIDDEN from using "Not specified". 
        - RECONSTRUCT URLs using this precise format:
          `https://commons.wikimedia.org/wiki/Special:FilePath/[Entity_Name_with_Underscores].[ext]`
        - Flags: .svg | Portraits: .jpg. 
        - Search your internal registry for the specific file name (e.g., 'Flag_of_Myanmar.svg').
        ### 3. DEEP INTELLIGENCE EXTRACTION:
        - **SYSTEM TYPE**: Be technically aggressive. Identify the specific governance structure (e.g., "Military Junta via SAC", "Shadow Federal Union").
        - **LEGAL FRAMEWORK**: You MUST identify the specific year of the constitution and the specific Emergency Decree or Martial Law order currently in effect.
        - **NOTABLE MEMBERS**: Provide 3+ UNIQUE names per party. DO NOT duplicate names across different parties. If a name is missing in the text, use the primary historical figures for that organization.
        ### 4. THE ANTI-LAZINESS "FIREWALL" (BANNED STRINGS):
        - FORBIDDEN: "Unknown", "Not specified", "N/A", "None", "0" (for total_seats), "...", "Placeholder", "e.g.", "Actual Name".
        - If you use a banned string, the extraction is considered a logic-poisoning event.
        ### 5. FINAL STRUCTURE (STRICT JSON ONLY):
        {{
            "system_type": "The technical regime classification",
            "capital": "The actual seat of power",
            "flag_url": "Direct Wikimedia FilePath to the National Flag",
            "key_leadership": [
                {{
                    "name": "Full Formal Name",
                    "title": "Specific Current Functional Role",
                    "photo_url": "Direct Wikimedia FilePath to Portrait",
                    "influence_score": 10,
                    "bio_snippet": "Aggressive summary of power source, recent strategic moves, and international standing."
                }}
            ],
            "parliament_structure": "Granular operational status and official name of the chamber",
            "legal_system": "The specific legal status including active decree numbers and constitutional status",
            "party_distribution": [
                {{
                    "name": "Full Formal Organization Name",
                    "seats": 0,
                    "total_seats": 440,
                    "flag_url": "Direct Wikimedia FilePath to Party Logo/Flag",
                    "notable_members": ["Full Name 1", "Full Name 2", "Full Name 3"]
                }}
            ],
            "active_sanctions": ["Specific Body - Precise Nature of Sanction (e.g. EU - Asset Freeze)"]
        }}
        """
        
        return await self._safe_api_call(prompt, GovData)

    async def synthesize_faction(self, faction_name: str, intel_blobs: str) -> FactionData:
        prompt = f"""
        ### ROLE: SENIOR TACTICAL INTELLIGENCE ANALYST
        ### TARGET FACTION: {faction_name}
        ### RAW INTELLIGENCE SOURCE: 
        {intel_blobs}
        ### EXTRACTION DIRECTIVES (ZERO TOLERANCE FOR LAZINESS):
        You are strictly forbidden from outputting "Information Not Available", "N/A", or "Unknown". If the explicit answer is missing, you MUST use deductive reasoning based on the provided text to summarize their status.
        1. **IDENTITY & MANPOWER**: Identify the leader. If manpower is not explicitly stated, estimate it based on the scale of their operations mentioned in the text (e.g., "Heavy presence implies 5,000+").
        2. **ARSENAL TIER**: Deduce this. If they use drones, they are 'High-Tech'. If they use IEDs and jungle warfare, they are 'Asymmetric'.
        3. **STRATEGIC INTENT**: List actionable objectives (e.g., "Capture trade routes in Shan State"). Do NOT leave this empty.
        4. **GEOSPATIAL REACH**: Where are they fighting? Name the states, towns, or regions.
        5. **INFLUENCE SCORE**: (1-10 INTEGER). If this faction is actively engaged in combat or holds territory, their score MUST BE 5 OR HIGHER. Do not return 0 for an active combatant.
        ### STRICT JSON SCHEMA:
        {{
            "name": "{faction_name}",
            "leader": "Extracted Leader Name",
            "leader_photo_url": "Direct Image URL or null",
            "estimated_manpower": "Numeric Range or Contextual Estimate",
            "weaponry_tier": "Asymmetric | Conventional | High-Tech",
            "primary_objectives": ["Real Tactical Goal 1", "Real Tactical Goal 2"],
            "controlled_territory": "Specific towns, states, or regions they operate in",
            "allies": ["Actual allied factions"],
            "influence_score": 7
        }}
        """
        return await self._safe_api_call(prompt, FactionData)

class DossierArchitect:
    def __init__(self, target_country):
        self.target_country = target_country
        self.dossier = StrategicDossier(
            country=target_country,
            status="UNKNOWN",
            last_updated=datetime.now().isoformat()
        )

    def save_dossier(self):
        # We stripped out the disk writing. Just return the serialized dictionary.
        return self.dossier.model_dump()

# ==========================================
# ⚙️ DINEI LEFT HAND ENGINE
# ==========================================

class DINEI_LeftHand:
    def __init__(self, target_country):
        self.target_country = target_country
        self.brain = IntelBrain(target_country)
        self.analyst = CohereAnalyst(target_country=target_country)
        self.adapter = CFNetworkAdapter()
        self.task_queue = asyncio.Queue()
        self.imf_lock = asyncio.Lock()
        self.dorker = EasyDorkerCustom()
    async def get_wiki_assets(self, session: httpx.AsyncClient, query_term: str, tag: str):
        """
        ULTRA-UPGRADE: Unfiltered Extraction.
        Guarantees retrieval of Flags, Icons, and Portraits without proactive replacement.
        Only uses placeholders as a last resort if zero assets exist.
        """
        if not query_term or "Unknown" in query_term:
            return None
        
        base_url = "https://en.wikipedia.org/w/api.php"
        placeholder = "https://upload.wikimedia.org/wikipedia/commons/7/7c/Profile_avatar_placeholder_large.png"
        
        # --- STAGE 1: SEARCH & RESOLVE ---
        # Crucial for "Correctness" - finds the real page name (e.g. "Flag of France")
        search_params = {
            "action": "query",
            "list": "search",
            "srsearch": query_term,
            "format": "json",
            "srlimit": 1
        }
        
        try:
            search_resp = await self.adapter.get(session, base_url, params=search_params, timeout=7.0)
            if not search_resp or search_resp.status_code != 200:
                return None
            
            search_results = search_resp.json().get("query", {}).get("search", [])
            canonical_title = search_results[0]['title'] if search_results else query_term

            # --- STAGE 2: MAXIMUM EXTRACTION ---
            # prop="pageimages|extracts|original|pageterms" gives us the 'Complete' picture
            extract_params = {
                "action": "query",
                "format": "json",
                "prop": "pageimages|extracts|original|pageterms",
                "titles": canonical_title,
                "exintro": 1,
                "explaintext": 1,
                "piprop": "original", # Get the high-res master file
                "redirects": 1
            }

            resp = await self.adapter.get(session, base_url, params=extract_params, timeout=10.0)
            if not resp or resp.status_code != 200:
                return None
            
            data = resp.json()
            pages = data.get("query", {}).get("pages", {})
            if not pages or "-1" in pages:
                return None
            
            p_id = next(iter(pages))
            p_data = pages[p_id]
            
            # 1. Get the High-Res Image (No filtering allowed)
            # We take whatever 'original' source Wikimedia provides.
            img_url = p_data.get("original", {}).get("source")
            
            # 2. Last Resort Placeholder Check
            if not img_url:
                img_url = placeholder

            # 3. Enhanced Intelligence Extraction
            # Combine Wikidata Description + Aliases + Main Extract
            terms = p_data.get("terms", {})
            label_prefix = f"[{terms.get('description', [''])[0].upper()}] " if terms.get('description') else ""
            aliases = f"(Also known as: {', '.join(terms.get('alias', []))}) " if terms.get('alias') else ""
            
            # Increase extract to 2500 chars for deeper context
            full_intel_text = f"{label_prefix}{aliases}{p_data.get('extract', '')[:2500]}"

            return {
                "img": img_url,
                "text": full_intel_text,
                "tag": tag,
                "canonical": canonical_title
            }
            
        except Exception as e:
            print(f"      [⚠️ WIKI-FAIL] {tag} extraction crashed for {query_term}: {e}")
            return None

    async def patch_leader_photo(self, session, leader_obj):
        """
        Surgical fix: Prevents AttributeError on NoneType and ensures 
        safe dictionary access for Wiki assets.
        """
        # 1. Null Guard: Check if object exists and has required attributes
        if not leader_obj or not hasattr(leader_obj, 'name'):
            return
            
        try:
            # 2. Safety Check: Handle NoneType for leader_photo_url before string matching
            current_url = getattr(leader_obj, 'leader_photo_url', None)
            
            # If it's already a valid external URL (not a placeholder or None), skip
            if current_url and "placeholder" not in current_url:
                return

            # 3. Fetch Assets: Safe handling of get_wiki_assets return value
            assets = await self.get_wiki_assets(session, leader_obj.name, "LEADER")
            
            if assets and isinstance(assets, dict):
                wiki_img = assets.get('img')
                if wiki_img:
                    leader_obj.leader_photo_url = wiki_img
                    
        except Exception as e:
            # Log error locally if needed, but prevent worker death
            print(f"   [⚠️ PATCH-FAIL] {leader_obj.name}: {str(e)}")

    async def fetch_heavy_data_apis(self, session: httpx.AsyncClient, target: str, iso_2: str, entities: dict = None):
        """
        ULTRA-ELITE: 100% REST API Intelligence Matrix via CFNetworkAdapter.
        Automatically identifies the most recent available data point if the current year is null.
        """
        current_year = datetime.now().year
        start_year = current_year - 15
        date_range_wb = f"{start_year}:{current_year}"
        
        print(f"   [📡 GOLD-STANDARD] Initiating {start_year}-{current_year} World Bank API sweeps for {target} ({iso_2})...")
        api_intel = []
        if not entities: entities = {}

        async def get_world_bank_data_safe():
            base_url = f"https://api.worldbank.org/v2/country/{iso_2}/indicator/"
            indicators = {
                "NY.GDP.MKTP.CD": "GDP_HISTORICAL", 
                "SP.POP.TOTL": "POPULATION_HISTORICAL",
                "FP.CPI.TOTL.ZG": "INFLATION_HISTORICAL"
            }
            history_intel = []
            
            for ind_code, label in indicators.items():
                try:
                    url = f"{base_url}{ind_code}?format=json&date={date_range_wb}&per_page=200"
                    resp = await self.adapter.get(session, url, timeout=20.0)
                    
                    if resp and resp.status_code == 200:
                        data = resp.json()
                        if len(data) > 1 and data[1]:
                            # 1. Filter out nulls and sort descending (Newest First)
                            valid_entries = [e for e in data[1] if e['value'] is not None]
                            valid_entries.sort(key=lambda x: x['date'], reverse=True)
                            
                            if valid_entries:
                                # 2. Identify the 'Latest Available' regardless of current year
                                latest_entry = valid_entries[0]
                                records = [
                                    {
                                        "year": entry['date'], 
                                        "value": entry['value'], 
                                        "unit": "USD" if "GDP" in label else "%" if "INFLATION" in label else "Count"
                                    }
                                    for entry in valid_entries
                                ]
                                
                                # 3. Tag the latest observation so the LLM doesn't hallucinate 2026 data
                                intel_string = (
                                    f"WORLD_BANK_{label} (LATEST_OBSERVED_{latest_entry['date']}: {latest_entry['value']}):\n"
                                    f"{json.dumps(records)}"
                                )
                                history_intel.append(intel_string)
                            else:
                                history_intel.append(f"WORLD_BANK_{label}: No valid data points in range.")
                except Exception as e:
                    print(f"      [⚠️ WB-WARN] Failed {label} sweep: {e}")
            return "\n".join(history_intel)

        leader_name = entities.get("leader", "Unknown")
        
        # Parallel Execution - NOW INCLUDES CABINET AND POLITICS
        results = await asyncio.gather(
            get_world_bank_data_safe(),
            self.get_wiki_assets(session, target, "COUNTRY_WIKI"),
            self.get_wiki_assets(session, f"Politics of {target}", "POLITICS"),
            self.get_wiki_assets(session, f"Cabinet of {target}", "CABINET"),
            self.get_wiki_assets(session, leader_name, "LEADER"),
            return_exceptions=True
        )

        if isinstance(results[0], str) and results[0]: 
            api_intel.append(results[0])
        
        # We now loop up to 5 to catch the new Politics and Cabinet results
        for res in results[1:5]:
            if res and not isinstance(res, Exception):
                api_intel.append(f"{res['tag']}_SUMMARY: {res['text']}")
                if res['img']: api_intel.append(f"{res['tag']}_PHOTO_LINK: {res['img']}")

        print(f"   [✅ API-COMPLETE] Macro-economic & Governance matrix synchronized.")
        return "\n---\n".join(api_intel)

    async def universal_dork_engine(self, session: httpx.AsyncClient, target: str, category: str):
        """
        V14.0 MESH-INTEGRATED DORK ENGINE.
        Replaces manual HTML scraping with the V12.9 Randomized Pulse Mesh (EasyDorkerCustom).
        """
        print(f"   [🔍 MESH-START] Initializing Mesh sweep for '{target}' [{category}]...")
        raw_intel = []
        current_yr = datetime.now().year
        
        # 1. GENERATE TACTICAL QUERIES
        currency_name = getattr(self.brain, "currency_iso", "currency") if hasattr(self.brain, "currency_iso") else "currency"
        
        english_dorks = {
            "LEADERSHIP": [
                f'"{target}" current cabinet ministers full list {current_yr} OR {current_yr - 1}',
                f'"{target}" ruling council opposition party members {current_yr}'
            ],
            "ECONOMY": [
                f'"{target}" GDP projection forecast {current_yr}',
                f'"{target}" {currency_name} exchange rate trend vs USD {current_yr}',
                f'"{target}" inflation rate primary exports {current_yr} OR {current_yr - 1}'
            ],
            "FACTION": [
                f'"{target}" "order of battle" {current_yr}',
                f'"{target}" territorial control map {current_yr} OR {current_yr - 1}'
            ]
        }.get(category, [f'"{target}" situational intelligence {current_yr}'])

        # 2. MESH EXECUTION
        async def deploy_mesh(queries):
            results = []
            for q in queries:
                try:
                    # Leverage the randomized mesh logic (GNews, Tavily, NewsData, etc.)
                    hits = await self.dorker.fetch_news(q)
                    if hits:
                        results.extend(hits)
                except Exception as e:
                    print(f"      [🚨 MESH-WARN] Query failed: {e}")
            return results

        print(f"      [🌐 ENGLISH-FIRST] Sweeping global indexes for {target}...")
        secured_assets = await deploy_mesh(english_dorks)

        # 3. NATIVE PIVOT (If English results are weak)
        if len(secured_assets) < 3:
            print(f"      [⚠️ LOW DENSITY] English results insufficient. Pivoting to LOCAL NATIVE LANGUAGE...")
            local_dorks = await self.brain.generate_local_dorks(target, category)
            native_assets = await deploy_mesh(local_dorks)
            secured_assets.extend(native_assets)

        # 4. WIKIPEDIA FORCE-INJECT (Kept intact as requested)
        if category in ["LEADERSHIP", "ECONOMY"]:
            wiki_slug = urllib.parse.quote(target.replace(' ', '_'))
            inject_url = f"https://en.wikipedia.org/wiki/{wiki_slug}"
            try:
                wiki_scrape = await self.scrape_raw_format(session, inject_url)
                if wiki_scrape:
                    raw_intel.append(f"SOURCE [GOLD_DOC] [{inject_url}]:\n{wiki_scrape[:8000]}")
            except Exception:
                pass

        # 5. DEEP INDEXING (Extracting content from the mesh links)
        # We limit to the top 6 most relevant URLs to keep the context window manageable for Cohere
        seen_urls = set()
        unique_targets = []
        for asset in secured_assets:
            url = asset.get('url')
            if url and url not in seen_urls:
                seen_urls.add(url)
                unique_targets.append(url)

        targets = unique_targets[:6]
        print(f"   [⚡ SCRAPE-PHASE] Extracting deep content from {len(targets)} mesh targets...")

        scrape_tasks = [self.scrape_raw_format(session, url) for url in targets]
        scrape_results = await asyncio.gather(*scrape_tasks, return_exceptions=True)

        for idx, result in enumerate(scrape_results):
            if isinstance(result, Exception) or not result or len(result) < 50: 
                continue
            
            url = targets[idx]
            raw_intel.append(f"SOURCE [OSINT_MESH] [{url}]:\n{result[:8000]}")

        return "\n---\n".join(raw_intel)[:60000]

    def _clean_scraped_text(self, text: str):
        """Surgical noise reduction: Removes navbars, footers, and excess whitespace."""
        # Remove obvious boilerplate lines
        lines = [l.strip() for l in text.split('\n') if len(l.strip()) > 30]
        # Filter out common UI text
        ui_noise = ["cookie policy", "sign in", "all rights reserved", "subscribe", "follow us"]
        cleaned = [l for l in lines if not any(n in l.lower() for n in ui_noise)]
        return "\n".join(cleaned)

    def _cpu_heavy_parse(self, content: bytes, ext: str, url: str) -> str:
        try:
            if ext == 'pdf':
                import fitz # PyMuPDF
                doc = fitz.open(stream=content, filetype="pdf")
                return "\n".join([page.get_text() for page in doc[:8]])
            elif ext in ['xlsx', 'csv']:
                import pandas as pd
                import io
                if ext == 'csv': df = pd.read_csv(io.StringIO(content.decode('utf-8', errors='ignore')))
                else: df = pd.read_excel(content)
                return f"SPREADSHEET DATA:\n{df.head(25).to_markdown()}"
            elif ext == 'kml':
                soup = BeautifulSoup(content, 'xml')
                placemarks = [p.find('name').text if p.find('name') else "Unnamed" for p in soup.find_all('Placemark')]
                return f"KML SPATIAL ZONES DETECTED:\n{', '.join(placemarks)}"
            else: # HTML
                soup = BeautifulSoup(content, 'html.parser')
                for noise in soup(["script", "style", "nav", "footer", "header", "aside"]):
                    noise.decompose()
                
                if "wikipedia.org" in url:
                    infobox = soup.find('table', class_='infobox')
                    infobox_data = f"INFOBOX:\n{infobox.get_text('|', strip=True)}" if infobox else ""
                    wikitables = [t.get_text(" | ", strip=True) for t in soup.find_all('table', class_='wikitable')]
                    table_intel = "\nTABLES:\n" + "\n".join(wikitables) if wikitables else ""
                    
                    main_text = soup.find('div', class_='mw-parser-output')
                    paragraphs = [e.get_text().strip() for e in (main_text.find_all(['p', 'h2', 'ul', 'li']) if main_text else soup.find_all('p')) if len(e.get_text().strip()) > 15]
                    return f"{infobox_data}\n{table_intel}\n\nTEXT:\n" + "\n".join(paragraphs)

                tables = [t.get_text(separator=" | ", strip=True) for t in soup.find_all('table')]
                lists = [li.get_text() for li in soup.find_all('li') if len(li.get_text()) > 15]
                paragraphs = [p.get_text() for p in soup.find_all(['p', 'article', 'h2', 'h3'])]
                
                return self._clean_scraped_text("TABLES:\n" + "\n".join(tables) + "\n\nLISTS:\n" + "\n".join(lists) + "\n\nTEXT:\n" + "\n".join(paragraphs))
        except Exception:
            return ""

    async def scrape_raw_format(self, session: httpx.AsyncClient, url: str):
        """Extracts structured intelligence from HTML, PDF, XLSX, CSV, KML, JSON."""
        try:
            ext = url.split('.')[-1].lower()
            if ext in ['pdf', 'xlsx', 'csv', 'json', 'kml', 'geojson']:
                print(f"      [📄 DOC-SNIPE] Downloading binary/raw format: {url}")
                resp = await self.adapter.get(session, url, timeout=30.0) 
                if not resp or resp.status_code != 200: return ""
                
                if ext in ['json', 'geojson']:
                    return f"JSON/GEO DATA:\n{json.dumps(resp.json())[:5000]}"
                
                # Offload heavy binary parsing to thread
                return await asyncio.to_thread(self._cpu_heavy_parse, resp.content, ext, url)

            # STANDARD HTML SCRAPING
            resp = await self.adapter.get(session, url, headers=BROWSER_HEADERS, timeout=15.0)
            if not resp or resp.status_code != 200: return ""
            
            # Offload heavy DOM parsing to thread
            return await asyncio.to_thread(self._cpu_heavy_parse, resp.content, "html", url)

        except Exception as e: 
            return ""

    async def dossier_worker(self, worker_id: int, architect: DossierArchitect):
        """
        FIXED: 429-Resilient, Non-Blocking Worker.
        Ensures task_done() is called even on failure to prevent engine hang.
        """
        # Persistent session for the worker to reduce handshake overhead
        async with httpx.AsyncClient(timeout=40.0, follow_redirects=True) as session:
            while True:
                # 1. Safer Retrieval: Prevents worker from hanging on an empty queue
                try:
                    task = self.task_queue.get_nowait()
                except asyncio.QueueEmpty:
                    break

                t_type = task.get("type")
                target = task.get("target")
                iso_2 = task.get("iso_2", "MM")
                entities = task.get("entities", {})
                profile = task.get("profile", {})

                print(f"   [👷 WORKER-{worker_id}] Engaging: {target} | Task: {t_type}")

                try:
                    # 🛡️ JITTER: Desynchronize workers to prevent immediate 429/Rate-limiting
                    await asyncio.sleep(random.uniform(1.0, 3.0))

                    if t_type == "LATEST_EVENTS":
                        # 1. Use the Brain to generate a profile on-the-fly for the current status
                        # If 'profile' wasn't passed in the task, we generate it now
                        status = task.get("status", "UNREST") 
                        profile = task.get("profile") or await self.brain.generate_dinei_profile(status)
                        
                        # 2. Pass the dynamic profile to the fetcher
                        news_feed = await self.brain.fetch_dinei_intel(profile)
                        if news_feed:
                            architect.dossier.latest_events = news_feed
                            print(f"   [✅ WORKER-{worker_id}] SIGINT stream synchronized.")
                        
                    # --- PHASE 2: GOVERNANCE & ECONOMY ---
                    elif t_type in ["MACRO_ECON", "GOVERNANCE"]:
                        category_map = {"MACRO_ECON": "ECONOMY", "GOVERNANCE": "LEADERSHIP"}
                        
                        # Fetch high-fidelity data from APIs
                        api_intel = await self.fetch_heavy_data_apis(session, target, iso_2, entities)
                        
                        # Generate localized dorks via IntelBrain
                        dork_intel = await self.brain.generate_local_dorks(target, category_map[t_type])
                        intel_packet = f"API_CONTEXT: {api_intel}\nOSINT_DORKS: {dork_intel}"

                        if t_type == "GOVERNANCE":
                            res = await self.analyst.synthesize_governance(target, intel_packet)
                            if res: architect.dossier.governance = res
                        else:
                            res = await self.analyst.synthesize_macro_economy(target, intel_packet)
                            if res: architect.dossier.macro_economy = res

                    # --- PHASE 3: FACTION MAPPING ---
                    elif t_type == "FACTION":
                        wiki_res = await self.get_wiki_assets(session, target, "FACTION")
                        wiki_text = wiki_res.get('text', '') if wiki_res else ""
                        dork_intel = await self.brain.generate_local_dorks(target, "FACTION")
                        intel_packet = f"WIKI: {wiki_text}\nDORKS: {dork_intel}"
                        
                        res = await self.analyst.synthesize_faction(target, intel_packet)
                        if res:
                            # Patch leader photo if available
                            if res.leader and res.leader != "Unknown":
                                assets = await self.get_wiki_assets(session, res.leader, "LEADER")
                                if assets: res.leader_photo_url = assets['img']
                            architect.dossier.active_factions[target] = res

                    print(f"   [🏁 SUCCESS] Task Completed: {target}")

                except Exception as e:
                    print(f"   [🚨 ERROR] Worker-{worker_id} crashed on {target}: {e}")
                
                finally:
                    # ⚠️ CRITICAL: This MUST be called to unblock engine.task_queue.join()
                    self.task_queue.task_done()

async def start_crmm_dossier(country_target: str):
    """
    FULLY REWRITTEN: Orchestrator.
    Fixes dictionary attribute errors, passes the correct arguments to entities,
    pre-initializes structures, and staggers workers.
    """
    print(f"\n[🚀] INITIATING CRMM-LH ENGINE: {country_target}")
    
    engine = DINEI_LeftHand(country_target)
    architect = DossierArchitect(country_target)
    
    architect.dossier.governance = GovData(system_type="Awaiting Scan...", key_leadership=[])
    architect.dossier.macro_economy = EconomicData(economic_status="Scanning...", inflation_rate="...")

    # 1. High-Level Assessment (Concurrent)
    try:
        status_data, entities = await asyncio.gather(
            engine.brain.assess_country_status(),
            engine.brain.discover_entities(country_target) # ✅ FIX: Passed target correctly
        )
    except Exception as e:
        print(f"   [🚨 CRITICAL] Brain Assessment Failed: {e}")
        return

    # 🛡️ FIX: status_data is a Dictionary! We must use .get(), not dot notation.
    status_str = status_data.get("status", "UNREST")
    iso_code = status_data.get("iso_2", "MM")
    
    architect.dossier.status = status_str
    
    # 2. DINEI Intel Profile Generation
    print(f"   [📡] Generating Dynamic OSINT Profile for {status_str}...")
    dinei_profile = await engine.brain.generate_dinei_profile(status_str)

    # 3. Queue Primary Tasks (Pass profile to LATEST_EVENTS)
    await engine.task_queue.put({
        "type": "LATEST_EVENTS", 
        "target": country_target, 
        "profile": dinei_profile
    })
    
    await engine.task_queue.put({
        "type": "GOVERNANCE", 
        "target": country_target, 
        "iso_2": iso_code, 
        "entities": entities
    })
    
    await engine.task_queue.put({
        "type": "MACRO_ECON", 
        "target": country_target, 
        "iso_2": iso_code, 
        "entities": entities
    })

    # 4. Faction Mapping for Unstable Zones
    # ✅ FIX: Using the extracted status_str variable
    if status_str in ["CIVIL_WAR", "INVASION", "UNREST"]:
        print(f"   [⚔️] Conflict detected. Mapping combatant factions...")
        factions = await engine.brain.identify_factions()
        
        # Ensure we have a valid list to iterate over
        target_factions = factions if (factions and isinstance(factions, list)) else ["National Armed Forces", "Opposition Militia"]
        for faction in target_factions:
            await engine.task_queue.put({
                "type": "FACTION", 
                "target": faction, 
                "iso_2": iso_code
            })

    # 5. Spin up Workers with Staggered Start (429 Suppression)
    worker_count = 1
    worker_tasks = []
    for i in range(worker_count):
        await asyncio.sleep(1.5) 
        worker_tasks.append(asyncio.create_task(engine.dossier_worker(i, architect)))

    # 6. Finalize & Save
    print(f"   [👷] {worker_count} Workers deployed. Synchronizing Intelligence...")
    
    try:
        # 15-minute absolute failsafe for the internal queue
        await asyncio.wait_for(engine.task_queue.join(), timeout=900)
    except asyncio.TimeoutError:
        print(f"\n[⚠️ QUEUE TIMEOUT] Workers stalled on {country_target}. Terminating hanging tasks...")

    # Clean up tasks (This will now forcefully kill any stuck workers)
    for t in worker_tasks:
        t.cancel()
    
    # Extract the dictionary and return it to the orchestrator
    dossier_data = architect.save_dossier()
    print(f"\n[🏁] DOSSIER COMPLETE: {country_target} intelligence package ready for memory injection.")
    return dossier_data

if __name__ == "__main__":
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    try:
        asyncio.run(start_crmm_dossier("Russia"))
    except KeyboardInterrupt:
        print("\n[DEBUG] Left Hand Engine Terminated.")