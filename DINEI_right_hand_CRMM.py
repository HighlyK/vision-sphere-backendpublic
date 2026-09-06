import os
import re
import sys
import json
import random
import asyncio
import httpx
import logging
from datetime import datetime
from typing import List, Dict, Any, Optional, Tuple
import hashlib
from dotenv import load_dotenv
from shapely.geometry import shape, mapping, LineString
from shapely.ops import split
load_dotenv()

# Setup Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - [READIYED-AI] - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)


class KeyPoolManager:
    """
    Manages high-throughput rotation and failover for 10 Cohere API keys 
    and 10 NVIDIA NIM API keys to handle parallel intelligence processing.
    """
    def __init__(self):
        self.cohere_keys: List[str] = []
        self.nvidia_keys: List[str] = []
        
        # Ingest up to 10 keys for each provider from environment variables
        for i in range(1, 11):
            c_key = os.getenv(f"COHERE_KEY_{i}")
            n_key = os.getenv(f"NVIDIA_NIM_KEY_{i}")
            if c_key: self.cohere_keys.append(c_key)
            if n_key: self.nvidia_keys.append(n_key)

        logging.info(f"[🔑 POOL INITIALIZED] Loaded {len(self.cohere_keys)} Cohere keys and {len(self.nvidia_keys)} NVIDIA NIM keys.")

    def get_cohere_key(self) -> str:
        if not self.cohere_keys:
            raise ValueError("CRITICAL: Zero valid Cohere keys found in key pool.")
        return random.choice(self.cohere_keys)

    def get_nvidia_key(self) -> str:
        if not self.nvidia_keys:
            raise ValueError("CRITICAL: Zero valid NVIDIA NIM keys found in key pool.")
        return random.choice(self.nvidia_keys)

class GeoJSONSovereigntyCutter:
    """
    Executes true mathematical spatial splitting using Shapely topology.
    Guarantees zero overlapping polygons and perfectly flush frontlines.
    """
    @staticmethod
    def slice_geometry_by_frontline(geometry: Dict[str, Any], frontline_coords: List[List[float]]) -> tuple:
        """
        Splits a GeoJSON Polygon/MultiPolygon using a continuous LineString vector.
        frontline_coords format: [[lon1, lat1], [lon2, lat2], ...]
        """
        try:
            poly_shape = shape(geometry)
            frontline = LineString(frontline_coords)
            
            # Execute topological cut
            split_collection = split(poly_shape, frontline)
            
            if len(split_collection.geoms) < 2:
                # The line didn't actually intersect the polygon properly
                return None, None
                
            geom_a = mapping(split_collection.geoms[0])
            geom_b = mapping(split_collection.geoms[1])
            
            return geom_a, geom_b
            
        except Exception as e:
            logging.error(f" [❌ GIS CLIP FAILURE] Topology engine failed to cut geometry: {e}")
            return None, None

    @staticmethod
    def get_bounding_box(geometry: Dict[str, Any]) -> List[float]:
        """Calculates the [min_lon, min_lat, max_lon, max_lat] to give the AI spatial awareness."""
        try:
            poly = shape(geometry)
            return list(poly.bounds)
        except Exception:
            return [0.0, 0.0, 0.0, 0.0]

class AIConflictAnalyzer:
    """
    Dispatches concurrent operational text streams to Cohere and NVIDIA NIM 
    to parse and extract structural intelligence indicators and frontline geometries.
    """
    def __init__(self, pool: KeyPoolManager, client: httpx.AsyncClient):
        self.pool = pool
        self.client = client

    async def analyze_with_nvidia(self, news_content: str, location: str, bbox: List[float], known_factions: List[str] = None) -> Dict[str, Any]:
        """
        Queries NVIDIA NIM endpoint using the official AsyncOpenAI SDK wrapper
        with a high-capacity reasoning model to compute localized control vectors and line geometry.
        """
        from openai import AsyncOpenAI
        import re
        import json
        import logging

        api_key = self.pool.get_nvidia_key()
        if not api_key:
            logging.warning(" [⚠️ NVIDIA] Execution halted: No API key available in the key pool.")
            return {}

        # Instantiate official OpenAI async client mapped directly to NVIDIA NIM endpoints
        nv_client = AsyncOpenAI(
            base_url="https://integrate.api.nvidia.com/v1", 
            api_key=api_key
        )

        # Deeply descriptive, multi-layered system instruction block to eliminate processing errors
        nvidia_instruction = (
            "You are 'READIYED-AI-CORE', an elite, globally-agnostic C4ISR and Geospatial Intelligence (GEOINT) parsing engine "
            "embedded within a high-concurrency automated OSINT pipeline. Your operational mandate is to ingest unstructured asymmetric "
            "warfare logs, battlefield dispatches, frontline reports, and tactical media updates from ANY theater of conflict "
            "(e.g., Eastern Europe, Southeast Asia, Middle East), and instantly extract the structural sovereignty parameters "
            "for a specific target boundary with absolute mathematical, logical, and country-neutral discipline.\n\n"
            
            "CRITICAL STRUCTURAL, ANTI-BIAS, AND EVALUATION CONSTRAINTS:\n\n"
            
            "1. TACTICAL STATUS CLASSIFICATION:\n"
            "   - 'stable': Absolute, undisputed hegemonic control by a single administrative/military force. Zero active kinetic friction, "
            "artillery/missile strikes, frontline probing, or internal insurgent operations within this specific reporting window.\n"
            "   - 'contested': Active kinetic engagements, shifting tactical frontlines, urban sieges, artillery duels, air/drone strike campaigns, "
            "or overlapping physical zones of control. If combat is happening, it is automatically contested.\n"
            "   - 'captured': A terminal, verified transition of territorial sovereignty during this reporting window. The defending or previous "
            "occupying force has been entirely routed, bypassed, or destroyed, and the offensive force has established secure physical presence.\n\n"
            
            "2. THE ANTI-LAZINESS ACTOR RESOLUTION PROTOCOL (CRITICAL):\n"
            "   - DO NOT DEFAULT TO LAZY PLACEHOLDERS: You are strictly forbidden from default-labeling actors as generic 'Government/Local', "
            "'Unknown Faction', or 'Government Forces' unless the text provides absolutely zero identifying characteristics. \n"
            "   - 'faction': Extract the exact nomenclature or clear operational identity of the DE FACTO dominant or offensive military actor "
            "holding/claiming the majority of the ground in the theater (e.g., 'Armed Forces of Ukraine', 'Russian Federation Forces', "
            "'3rd Territorial Defense Brigade', 'Regional Alliance Militia'). Normalize names for consistency.\n"
            "   - 'opposing_faction': Extract the rival kinetic challenger actively fighting, defending against, or shelling that dominant force. "
            "If the tactical status is verified as truly 'stable' or 'captured' with zero lingering active hostile threat within the polygon boundary, "
            "you MUST output 'None'. Do NOT reference historical enemies, only active combatants identified in the logs.\n\n"
            
            "3. GEOMETRIC BISECTION MATRIX (THE CONTROL RATIO):\n"
            "   Calculate a precise floating-point value between 0.00 and 1.00 representing the estimated physical geographic surface area "
            "currently held by the primary 'faction'. This float directly dictates downstream automated polygon slicing algorithms. "
            "Apply this strict geometric heuristic matrix based strictly on semantic text indicators:\n"
            "   - Hegemonic / Undisputed Stable / Newly Consolidated Captured: 1.00\n"
            "   - Dominant force advancing through municipal limits, routing/pushing back defenders: 0.60 to 0.75\n"
            "   - Active street-to-street urban bisection / fluid frontline split: 0.45 to 0.55\n"
            "   - Encircled/sieged but stubbornly holding the fortified urban core or local stronghold: 0.30 to 0.40\n"
            "   - Dominant force pushed back, holding only outer defensive perimeters, checkpoints, or rural outskirts: 0.15 to 0.25\n"
            "   - Completely overrun, neutralized, or pinned with near-zero spatial presence: 0.01 to 0.10\n\n"
            
            "4. GEOSPATIAL FRONTLINE VECTOR GENERATION (CRITICAL FOR CONTESTED STATUS):\n"
            "   - If and ONLY if the status is 'contested', you must plot a series of physical coordinate steps representing the frontline.\n"
            "   - You MUST generate the 'frontline_vector' as an array of coordinate arrays: [[lon1, lat1], [lon2, lat2], ...]\n"
            "   - These coordinates MUST fall logically within the provided geospatial Bounding Box constraint matrix.\n"
            "   - The vector must form a clean, continuous line string that bisects or separates the dominant faction from the opponent.\n"
            "   - If the status is 'stable' or 'captured', output null for the 'frontline_vector'.\n\n"
            
            "5. ANTI-DEFLECTION GUARDRAILS & TEMPORAL DISCIPLINE:\n"
            "   - REJECT PEACEFUL BIAS: If a dispatch details explosions, clashes, ambushes, or missile impacts, do NOT lazily flag the sector "
            "as 'stable' or '1.00' control out of programmatic inertia. Parse the violence and mathematically map the friction.\n"
            "   - Ignore propagandistic future claims or psychological warfare (e.g., 'we will liberate the city by next week'). Map ONLY the present, real-time tactical reality on the ground.\n"
            "   - Ignore legacy historical context (e.g., 'the town, which changed hands three times last year...'). Focus entirely on the dynamic kinetic event described within the active logging window.\n\n"
            
            "OUTPUT FORMATTING MANDATE (CRITICAL EXECUTION):\n"
            "You must return ONLY a valid, minified raw JSON object. Do NOT wrap the output in markdown code blocks (never use ```json or backticks). "
            "Do NOT include analytical commentary, meta-reasoning, introductory pleasantries, or conversational text. Your output must begin with "
            "the character '{' and end with the character '}'.\n\n"
            
            "STRICT JSON SCHEMA:\n"
            "{\"status\":\"contested|captured|stable\",\"faction\":\"Exact Normalized Name\",\"opposing_faction\":\"Exact Normalized Name|None\",\"ratio\":0.55,\"frontline_vector\":[[lon1,lat1],[lon2,lat2]]|null}"
        )

        known_factions_str = ", ".join(known_factions) if known_factions else "None established yet."

        # Segmented user query context injecting spatial constraints AND memory
        nvidia_user_content = (
            f"TARGET ANCHOR GEOGRAPHY: '{location}'\n"
            f"BOUNDING BOX MATRIX: {bbox}\n"
            f"KNOWN FACTIONS ON MAP: [{known_factions_str}]\n"
            f"(CRITICAL: If the combatants in the text match any of these known factions, use the EXACT name from the list to maintain global map consistency.)\n\n"
            f"Intelligence stream:\n"
            f"\"\"\"\n"
            f"{news_content}\n"
            f"\"\"\""
        )

        try:
            # Native SDK execution utilizing the specified NIM model matrix
            nv_response = await nv_client.chat.completions.create(
                model="qwen/qwen3-coder-480b-a35b-instruct",
                messages=[
                    {"role": "system", "content": nvidia_instruction},
                    {"role": "user", "content": nvidia_user_content}
                ],
                temperature=0.0,
                top_p=0.01
            )

            raw_text = nv_response.choices[0].message.content.strip()
            
            # Clean markdown code-fence injections if generated by the model
            clean_json = re.sub(r"^```json|```$", "", raw_text, flags=re.IGNORECASE).strip()
            return json.loads(clean_json)

        except Exception as e:
            logging.warning(f"  [⚠️ NVIDIA NIM TIMEOUT] Cascading failover loop triggered: {e}")
            return {}

    async def analyze_with_cohere(self, news_content: str, location: str, bbox: List[float], known_factions: List[str] = None)-> Dict[str, Any]:
        """
        Ensemble validation check hitting Cohere's Command models via official SDK
        for structured tactical intelligence cross-examination.
        """
        import cohere
        
        api_key = self.pool.get_cohere_key()
        if not api_key:
            logging.warning(" [⚠️ COHERE] No API key available in pool.")
            return {}

        cohere_system_instruction = (
            "You are 'READIYED-AI-CORE-COHERE', an elite, globally-agnostic C4ISR and Geospatial Intelligence (GEOINT) parsing engine "
            "optimized for automated open-source intelligence (OSINT) pipelines. Your core operational mandate is to extract hyper-precise "
            "tactical control metrics from unstructured news intelligence reports for specified localized administrative boundaries anywhere "
            "in the world, completely free of geographic or country-specific bias.\n\n"
            
            "You must evaluate the provided text and output ONLY a valid, raw, minified JSON object matching a strict analytical schema. "
            "Do NOT include any conversational preamble, notes, meta-reasoning, postscripts, or Markdown formatting code blocks (never use ```json "
            "or backticks). Your output must strictly begin with the character '{' and end with the character '}' to prevent downstream parsing failures.\n\n"
            
            "CRITICAL METRIC SCHEMATIC SPECIFICATIONS:\n\n"
            
            "1. 'status': Must be exactly one of the following literal strings:\n"
            "   - 'stable': Absolute, undisputed hegemonic control by a single unified force. Zero active kinetic friction, shelling, drone strikes, "
            "frontline probing, or active defensive engagements within this tracking window.\n"
            "   - 'contested': Active kinetic engagements, fluid frontlines, ambushes, tactical skirmishes, artillery/missile strikes, or ongoing "
            "bisected territorial control. If physical combat is occurring, the area is automatically contested.\n"
            "   - 'captured': A terminal, verified shift of territorial sovereignty during this reporting window. The previous occupying or defending "
            "force has been fully routed or destroyed, and the offensive force has established complete localized control.\n\n"
            
            "2. 'faction': Extract the exact nomenclature or operational identity of the DE FACTO dominant or offensive military/paramilitary actor "
            "holding the majority of physical ground inside the zone. Normalize the entity name for consistency (e.g., 'Armed Forces of Ukraine', "
            "'State Military Force', 'Regional Alliance Militia'). \n"
            "   - STRICT ANTI-LAZINESS GUARDRAIL: Do NOT default to generic placeholders like 'Government/Local' or 'Unknown Faction' "
            "if the text provides specific actor identities or context clues. Isolate the real entities.\n\n"
            
            "3. 'opposing_faction': Extract the rival kinetic challenger actively attacking, defending against, or shelling that dominant force. "
            "If the tactical status is verified as truly 'stable' or 'captured' with zero active hostile kinetic threat remaining inside the boundary "
            "polygon, you MUST return 'None'. Do NOT list historical or non-present adversaries.\n\n"
            
            "4. 'ratio': A floating-point value between 0.00 and 1.00 representing the estimated geographic surface area held by the primary 'faction'. "
            "This float directly drives downstream automated polygon bisection geometry cuts. Use this strict matrix:\n"
            "   - True Hegemonic Control / Stable / Newly Consolidated Captured: 1.00\n"
            "   - Dominant force advancing through local limits, driving out defenders: 0.60 to 0.75\n"
            "   - Active street-to-street urban bisection / balanced fluid frontline: 0.45 to 0.55\n"
            "   - Encircled/sieged but holding a fortified core or local stronghold: 0.30 to 0.40\n"
            "   - Dominant force holding only outer defensive perimeters or rural outskirts: 0.15 to 0.25\n\n"
            
            "5. 'frontline_vector': If 'status' is 'contested', plot a continuous physical line vector string as an array of numerical pairs: "
            "[[lon1, lat1], [lon2, lat2], ...]. These points must be enclosed entirely inside the targeted geospatial Bounding Box space matrix "
            "provided in the user payload to support true topological slicing. If status is stable or captured, populate this key with null.\n\n"
            
            "ANTI-DEFLECTION & LINGUISTIC TRIGGER MANDATE:\n"
            "If the unstructured log mentions keywords indicating kinetic friction—such as 'clash', 'fighting', 'attack', 'ambush', 'shelling', "
            "'strike', 'bombardment', 'offensive', or 'seized'—in direct relation to the target location, you are STRICTLY FORBIDDEN from classifying "
            "the status as 'stable' or defaulting the control ratio blindly to '1.0'. You must break the peaceful bias, parse the conflict, "
            "and map the competing combatants accurately based on the report text."
        )

        known_factions_str = ", ".join(known_factions) if known_factions else "None established yet."

        cohere_user_instruction = (
            f"TARGET ANCHOR LOCATION TO ASSESS: '{location}'\n"
            f"BOUNDING BOX BOUNDS FOR VECTOR CLIPPING: {bbox}\n"
            f"KNOWN FACTIONS ON MAP: [{known_factions_str}]\n"
            f"(CRITICAL: If the combatants in the text match any of these known factions, use the EXACT name from the list to maintain global map consistency.)\n\n"
            f"RAW TACTICAL INTELLIGENCE TEXT:\n"
            f"\"\"\"\n"
            f"{news_content}\n"
            f"\"\"\"\n\n"
            f"INSTRUCTION: Extract the spatial attributes and active combat profiles for '{location}' strictly based on the real tactical events. "
            f"Populate the JSON keys exactly: 'status', 'faction', 'opposing_faction', 'ratio', 'frontline_vector'."
        )

        try:
            # Initialize official Cohere Async client
            co = cohere.AsyncClientV2(api_key=api_key)
            
            # Execute chat completion via Command-A / Command-R-Plus
            response = await co.chat(
                model="command-a-plus-05-2026",
                messages=[
                    {"role": "system", "content": cohere_system_instruction},
                    {"role": "user", "content": cohere_user_instruction}
                ],
                temperature=0.0,
                response_format={"type": "json_object"},
                p=0.01
            )
            
            # Safe list comprehension to piece together response text (V2 Compatible)
            extracted_text = "".join(
                item.text for item in response.message.content 
                if hasattr(item, 'text') and item.text
            )
            
            # Strip away markdown block fences if any slipped past the JSON constraint
            clean_json = re.sub(r"^```json|```$", "", extracted_text.strip(), flags=re.IGNORECASE).strip()
            return json.loads(clean_json)
            
        except Exception as e:
            logging.warning(f"  [⚠️ COHERE FAULT] Failover triggered: {e}")
            return {}

class CRMM_RIGHTHAND:
    """
    Main Orchestrator engine. Consumes raw target data arrays, physically separates 
    stable geometries from active threats, processes AI layers concurrently with 
    universal guardrails, and outputs standard clean GeoJSON for Cesium.
    
    Completely country-agnostic: handles any conflict theater dynamically.
    Features stateful memory for faction color-coding and true Shapely topology cutting.
    """
    def __init__(self):
        self.key_manager = KeyPoolManager()
        self.cutter = GeoJSONSovereigntyCutter()
        
        # ─── STATEFUL FACTION MEMORY ───
        self.faction_memory: Dict[str, str] = {}
        # High-visibility tactical palette for active actors
        self.tactical_palette = ["#d93838", "#2a75e6", "#e6a129", "#9b59b6", "#1abc9c", "#e67e22", "#34495e"]

    def _get_dynamic_faction_color(self, faction: str) -> str:
        """
        Checks persistent memory first. If the faction is new, assigns a color and remembers it.
        Guarantees that once a faction is identified, it maintains visual consistency across the map.
        """
        faction_clean = faction.strip().lower()
        if not faction_clean or "stable" in faction_clean or "baseline" in faction_clean:
            return "#27ae60"  # Tactical Stable Green

        # Check memory first (No await expressions inside means this remains atomic/thread-safe in async context)
        if faction_clean in self.faction_memory:
            return self.faction_memory[faction_clean]

        # Assign a new color based on current palette rotation and store it
        color_index = len(self.faction_memory) % len(self.tactical_palette)
        assigned_color = self.tactical_palette[color_index]
        
        self.faction_memory[faction_clean] = assigned_color
        logging.info(f"   [🧠 FACTION MEMORY] Registered new actor '{faction}' with color {assigned_color}")
        
        return assigned_color

    def _assign_cesium_styling(self, status: str, faction: str) -> Dict[str, Any]:
        """
        Assigns standard web map properties to render colors, lines, and extrusions natively.
        Enforces bright green for stable areas and handles any dynamic actor cleanly.
        """
        # ─── COLOR PROFILE MAPPING ───
        if status == "stable":
            color = "#27ae60"  # Enforce bright tactical green for all baseline/stable sectors
        else:
            color = self._get_dynamic_faction_color(faction)

        # ─── STATUS MODIFIERS (Extrusion & Opacity) ───
        if status == "captured":
            height, ext_height, opacity = 12000, 15000, 0.65
            stroke = "#ffffff"
        elif status == "contested":
            height, ext_height, opacity = 5000, 9000, 0.45
            stroke = "#ff0000"
        else:
            # Stable baseline mapping layers
            height, ext_height, opacity = 2000, 4000, 0.30
            stroke = "#4caf50"

        return {
            "fill": color,
            "fill-opacity": opacity,
            "stroke": stroke,
            "stroke-width": 2.5,
            "height": height,
            "extrudedHeight": ext_height
        }

    async def _process_single_active_theater(
        self, 
        item: Dict[str, Any], 
        analyzer: Any, 
        semaphore: asyncio.Semaphore
    ) -> List[Dict[str, Any]]:
        """
        Processes a single active theater completely asynchronously, wrapping API requests
        and geometry-splitting protocol into a self-contained concurrent execution thread.
        """
        location_name = item.get("location", "Unknown Sector")
        geometry = item.get("geojson_boundaries")
        news_excerpt = item.get("news_content", "")
        
        if not geometry: 
            return []

        # Enforce rate-limiting & key pool coordination limits using a shared Semaphore barrier
        async with semaphore:
            logging.info(f"   [⏳ AI INFERENCE START] Launching dual-model evaluations for: '{location_name}'...")
            
            # Calculate the exact bounding box so the AI knows where on earth to draw the frontline
            bbox = self.cutter.get_bounding_box(geometry)
            
            # ─── MEMORY INJECTION ───
            # Dynamically grab whatever factions have already been discovered by other concurrent workers
            current_known_factions = list(self.faction_memory.keys())
            
            # Run NVIDIA NIM and Cohere simultaneously (Passing the memory matrix)
            nv_task = asyncio.create_task(analyzer.analyze_with_nvidia(news_excerpt, location_name, bbox, current_known_factions))
            co_task = asyncio.create_task(analyzer.analyze_with_cohere(news_excerpt, location_name, bbox, current_known_factions))
            
            nv_res, co_res = await asyncio.gather(nv_task, co_task, return_exceptions=True)
        
        nv_metrics = nv_res if isinstance(nv_res, dict) else {}
        co_metrics = co_res if isinstance(co_res, dict) else {}
        
        status = co_metrics.get("status") or nv_metrics.get("status") or "stable"
        faction = co_metrics.get("faction") or nv_metrics.get("faction") or "Government/Local"
        opposing_faction = co_metrics.get("opposing_faction") or nv_metrics.get("opposing_faction") or "None"
        frontline_coords = co_metrics.get("frontline_vector") or nv_metrics.get("frontline_vector")

        # ─── UNIVERSAL ANTI-LAZINESS OVERRIDE (FIXED) ───
        news_lower = news_excerpt.lower()
        universal_conflict_keywords = [
            "clash", "fighting", "battle", "combat", "attack", "seized", "captured", 
            "shelling", "strike", "forces", "engaged", "offensive", "advance", "ambush"
        ]
        has_active_conflict = any(kw in news_lower for kw in universal_conflict_keywords)
        
        if has_active_conflict:
            # Force the status to contested if violence is detected
            if status == "stable":
                status = "contested"
                
            # ONLY destroy the AI's faction name if it returned a lazy/empty placeholder
            lazy_labels = ["government", "government/local", "unknown", "none", "unknown faction", ""]
            if faction.lower().strip() in lazy_labels:
                # Make the fallback string unique to the location so it gets a fresh color, not red
                faction = f"Unidentified Armed Element ({location_name})"
                
            if opposing_faction.lower().strip() in lazy_labels:
                opposing_faction = "Defending Territorial Force"

        # Catch edge cases where opposing force is missing in a contested zone
        if status == "contested" and (opposing_faction.lower() == "none" or opposing_faction == faction):
            opposing_faction = f"Opposing Force to {faction}"

        # ─── GEOMETRIC BOUNDARY CUTTING PROTOCOL ───
        if status == "contested" and frontline_coords and len(frontline_coords) >= 2:
            logging.info(f"     ↳ [✂️ GIS TOPOLOGY CUT] Slicing '{location_name}' along AI frontline vector: {frontline_coords}")
            
            # Execute perfect Shapely topology slice
            geom_a, geom_b = self.cutter.slice_geometry_by_frontline(geometry, frontline_coords)
            
            if geom_a and geom_b:
                feature_a = {
                    "type": "Feature",
                    "geometry": geom_a,
                    "properties": {
                        "location_name": f"{location_name} ({faction} Controls)",
                        "status": status,
                        "faction": faction,
                        "opposing_faction": opposing_faction,
                        "intelligence_context": news_excerpt,
                        **self._assign_cesium_styling(status, faction)
                    }
                }
                feature_b = {
                    "type": "Feature",
                    "geometry": geom_b,
                    "properties": {
                        "location_name": f"{location_name} ({opposing_faction} Controls)",
                        "status": status,
                        "faction": opposing_faction,
                        "opposing_faction": faction,
                        "intelligence_context": news_excerpt,
                        **self._assign_cesium_styling(status, opposing_faction)
                    }
                }
                return [feature_a, feature_b]
            
            logging.warning(f"     ↳ [⚠️ GIS CLIP MISS] Frontline vector did not intersect polygon perfectly for '{location_name}'.")

        # Fallback standard execution for uncut polygons (stable, captured, or failed cuts)
        location_label = f"{location_name} ({faction} Sector)" if status != "stable" else location_name
        if status == "contested":
            location_label = f"{location_name} (Contested Sector)"

        feature = {
            "type": "Feature",
            "geometry": geometry,
            "properties": {
                "location_name": location_label,
                "status": status,
                "faction": faction,
                "opposing_faction": opposing_faction,
                "intelligence_context": news_excerpt,
                **self._assign_cesium_styling(status, faction)
            }
        }
        return [feature]

    async def process_intelligence_sieve(self, raw_input_dataset: List[Dict[str, Any]]) -> Dict[str, Any]:
        # 1. Optimize HTTP Client connection limits to support massive concurrent fan-out pools
        limits = httpx.Limits(max_keepalive_connections=20, max_connections=100)
        
        async with httpx.AsyncClient(limits=limits, timeout=30.0) as http_client:
            analyzer = AIConflictAnalyzer(self.key_manager, http_client)
            features_collection: List[Dict[str, Any]] = []

            # =====================================================================
            # STAGE 1: DATA PIPELINE SPLIT
            # =====================================================================
            active_assets = []
            missed_assets = []
            
            for item in raw_input_dataset:
                if "news_content" in item and item.get("news_content", "").strip():
                    active_assets.append(item)
                else:
                    missed_assets.append(item)

            # =====================================================================
            # STAGE 2: INSTANT RAM PROCESSING (Missed / Baseline Boundaries)
            # =====================================================================
            logging.info(f" [🟢 BASELINE] Instantly compiling {len(missed_assets)} stable/baseline boundaries...")
            
            for item in missed_assets:
                geometry = item.get("missed_geojson_boundaries")
                if not geometry: 
                    continue
                
                feature = {
                    "type": "Feature",
                    "geometry": geometry,
                    "properties": {
                        "location_name": item.get("missed_location", "Unknown Sector"),
                        "status": "stable",
                        "intelligence_context": "No active conflict indicators flagged within current tracking window.",
                        **self._assign_cesium_styling("stable", "Stable Baseline")
                    }
                }
                features_collection.append(feature)

            # =====================================================================
            # STAGE 3: CONCURRENT AI PROCESSING & THEATER RECONCILIATION
            # =====================================================================
            total_active = len(active_assets)
            if total_active > 0:
                logging.info(f" [🧠 AI PIPELINE] Fanning out concurrent extractions across {total_active} active theaters...")
                
                # Bounded concurrency: allow up to 12 active theaters to execute completely in parallel
                # Adjust based on the exact size of your API key rotation arrays
                semaphore = asyncio.Semaphore(7)
                
                # Construct task grid for complete concurrent asset discovery
                tasks = [
                    self._process_single_active_theater(item, analyzer, semaphore)
                    for item in active_assets
                ]
                
                # Execute all target lookups completely in parallel
                parallel_results = await asyncio.gather(*tasks)
                
                # Flatten out compiled features into the primary feature array safely
                for theater_features in parallel_results:
                    features_collection.extend(theater_features)

            # =====================================================================
            # STAGE 4: PACKAGE FINAL PAYLOAD
            # =====================================================================
            return {
                "type": "FeatureCollection",
                "crs": {
                    "type": "name",
                    "properties": {"name": "urn:ogc:def:crs:OGC:1.3:CRS84"}
                },
                "features": features_collection
            }

async def run_intelligence_pipeline(target_country):
    """
    Orchestrates the end-to-end data harvest, AI conflict vector analysis,
    and Cesium-optimized GeoJSON translation.
    """
    
    TARGET_COUNTRY = target_country
    OUTPUT_FILENAME = f"admin3_conflict_{target_country}.geojson"
    print("\n" + "="*80)
    print(f"📡 INITIATING LIVE DATA HARVEST & GEOLOCATION SIFTING FOR: {TARGET_COUNTRY.upper()}")
    print("="*80)
    
    try:
        # Dynamic import of the geocoding data sieve module
        from crmm_data_boudary_location import CRMMGeoCoderNewsBoundary
        sieve = CRMMGeoCoderNewsBoundary(target_country=TARGET_COUNTRY)
        
        print(f"⏳ Querying active theater milestones and caching administrative shapes...")
        live_sieve_data = await sieve.get_crmm_data()
        
        if live_sieve_data is None:
            logging.error("🚨 CRITICAL: get_crmm_data() executed but returned None. Verify 'return unified_dataset' is appended to the method.")
            return

    except ImportError as ie:
        logging.error(f"❌ CRITICAL: 'crmm_data_boudary_location.py' not found in system path. Execution halted. Details: {ie}")
        return
    except Exception as e:
        logging.error(f"💥 PIPELINE ERROR during data harvest: {e}")
        return

    # 2. Check if assets were captured
    total_assets = len(live_sieve_data) if live_sieve_data else 0
    if total_assets == 0:
        print("⚠️ WARNING: The data sieve returned 0 active or baseline assets for this country context.")
        print("Aborting compilation to prevent empty GeoJSON payload generation.")
        return

    # 3. Initialize Core Compiler Engine
    print("\n" + "="*80)
    print("🛰️ INITIALIZING SATELLITE CORE GEOJSON COMPILER GENERATION")
    print("="*80)
    
    # Instantiate your master orchestrator class
    compiler = CRMM_RIGHTHAND() 
    
    print(f"🧠 Processing {total_assets} tactical boundary vectors through AI validation queues...")
    compiled_geojson = await compiler.process_intelligence_sieve(live_sieve_data)
    
    if not compiled_geojson or "features" not in compiled_geojson:
        logging.warning("⚠️ WARNING: Compiler returned an invalid or empty GeoJSON geometry dictionary.")
        return

    # 4. Save out clean, Cesium-ready GeoJSON features to disk (Enforcing UTF-8 Safeguards)
    try:
        with open(OUTPUT_FILENAME, "w", encoding="utf-8") as out_file:
            json.dump(compiled_geojson, out_file, indent=2, ensure_ascii=False)
            
        print("\n" + "="*80)
        print(f"🟢 SUCCESS: Clean Cesium-optimized GeoJSON written completely to: {OUTPUT_FILENAME}")
        print(f"📊 Total Complex Operational Vectors Layered: {len(compiled_geojson['features'])}")
        print("="*80 + "\n")
    except Exception as io_err:
        logging.error(f"💥 FILE IO ERROR: Failed to write GeoJSON payload to disk: {io_err}")


if __name__ == "__main__":
    # Fix for Windows event loop policies when running high-concurrency async tasks
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    # Standard context execution block
    try:
        asyncio.run(run_intelligence_pipeline(target_country="Ukraine"))
    except KeyboardInterrupt:
        print("\n🛑 Pipeline manual override triggered by operator. Exiting cleanly.")