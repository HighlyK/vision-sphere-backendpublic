import os
import re
import sys
import json
import asyncio
import httpx
import urllib.parse
from datetime import datetime
from typing import List, Dict, Any, Optional
from dotenv import load_dotenv
from cerebras.cloud.sdk import AsyncCerebras
from openai import AsyncOpenAI
import spacy
import difflib
from EasyDorkerCustom import EasyDorkerCustom
import random

# Load environment variables
load_dotenv()

# Setup Logging
import logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - [GEO-SIEVE] - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)

nlp = spacy.load("en_core_web_lg")


class CRMMGeoCoderNewsBoundary:
    def __init__(self, target_country: str):
        self.target_country = target_country
        self.dorker = EasyDorkerCustom()
        
        cbr_key = os.getenv("CERABRAS_KEY_7") or os.getenv("CERABRAS_API_KEY")
        self.cbr_client = AsyncCerebras(api_key=cbr_key) if cbr_key else None
        
        samba_key = os.getenv("SAMBA_KEY_10")
        self.samba_client = AsyncOpenAI(
            base_url="https://api.sambanova.ai/v1", 
            api_key=samba_key
        ) if samba_key else None
        
        if not self.cbr_client and not self.samba_client:
            logging.warning("⚠️ CRITICAL: Neither Cerebras nor SambaNova keys found in environment variables.")
        
        # HTTP Client for Geocoding
        self.http_client = httpx.AsyncClient(timeout=30.0, headers={
            'User-Agent': 'VisionSphere_Data_Sieve/1.0',
            'Accept': 'application/json'
        })
        
        # Rate Limiting Semaphore to protect Nominatim/Geoapify
        self.geocode_semaphore = asyncio.Semaphore(2) 

        # ─── GLOBAL IN-MEMORY TOWNSHIP REGISTRY ───
        self.township_boundaries: Dict[str, Dict[str, Any]] = {}

    def _fuzzy_match_ram_ledger(self, extracted_loc: str, threshold: float = 0.75) -> Optional[Dict[str, Any]]:
        """
        Takes a spaCy extracted token and finds the closest matching official boundary in RAM.
        Handles missing suffixes like 'Township' or 'District' and slight misspellings.
        """
        clean_target = re.sub(r'[^\w\s-]', '', extracted_loc).strip().lower()
        if not clean_target or len(clean_target) < 3:
            return None

        # 1. Substring & Token Match (Catches "kyaukme" inside "kyaukme township")
        for ram_key, boundary_data in self.township_boundaries.items():
            # If the spoken town name is anywhere inside the official RAM string
            if clean_target == ram_key or clean_target in ram_key.split() or clean_target in ram_key:
                return boundary_data
                
        # 2. Difflib Fuzzy Matching (Catches slight spelling drifts like "Myawady" vs "Myawaddy")
        ram_keys = list(self.township_boundaries.keys())
        matches = difflib.get_close_matches(clean_target, ram_keys, n=1, cutoff=threshold)
        
        if matches:
            best_match_key = matches[0]
            logging.info(f"   ↳ [🧠 FUZZY RESOLVE] Corrected spelling drift: '{clean_target}' -> '{best_match_key}'")
            return self.township_boundaries[best_match_key]
            
        return None

    async def preload_all_township_boundaries(self, country_codes: List[str], adm_level: str = "ADM2") -> bool:
        """
        Upfront Global Batch Extraction (Fully Upgraded): Downloads all administrative boundaries 
        for all target countries concurrently. Explicitly targets and resolves 302 redirect 
        traversal down to GitHub/CDN storage nodes, collects full dataset batches, tracks 
        granular feature metrics, and registers assets safely.
        """
        # Expanded country code translation matrix for global conflict zones and monitoring theaters
        iso2_to_iso3 = {
            "AF": "AFG", "AX": "ALA", "AL": "ALB", "DZ": "DZA", "AS": "ASM", "AD": "AND", "AO": "AGO", "AI": "AIA", "AQ": "ATA", "AG": "ATG",
            "AR": "ARG", "AM": "ARM", "AW": "ABW", "AU": "AUS", "AT": "AUT", "AZ": "AZE", "BS": "BHS", "BH": "BHR", "BD": "BGD", "BB": "BRB",
            "BY": "BLR", "BE": "BEL", "BZ": "BLZ", "BJ": "BEN", "BM": "BMU", "BT": "BTN", "BO": "BOL", "BQ": "BES", "BA": "BIH", "BW": "BWA",
            "BV": "BVT", "BR": "BRA", "IO": "IOT", "BN": "BRN", "BG": "BGR", "BF": "BFA", "BI": "BDI", "CV": "CPV", "KH": "KHM", "CM": "CMR",
            "CA": "CAN", "KY": "CYM", "CF": "CAF", "TD": "TCD", "CL": "CHL", "CN": "CHN", "CX": "CXR", "CC": "CCK", "CO": "COL", "KM": "COM",
            "CD": "COD", "CG": "COG", "CK": "COK", "CR": "CRI", "HR": "HRV", "CU": "CUB", "CW": "CUW", "CY": "CYP", "CZ": "CZE", "CI": "CIV",
            "DK": "DNK", "DJ": "DJI", "DM": "DMA", "DO": "DOM", "EC": "ECU", "EG": "EGY", "SV": "SLV", "GQ": "GNQ", "ER": "ERI", "EE": "EST",
            "SZ": "SWZ", "ET": "ETH", "FK": "FLK", "FO": "FRO", "FJ": "FJI", "FI": "FIN", "FR": "FRA", "GF": "GUF", "PF": "PYF", "TF": "ATF",
            "GA": "GAB", "GM": "GMB", "GE": "GEO", "DE": "DEU", "GH": "GHA", "GI": "GIB", "GR": "GRC", "GL": "GRL", "GD": "GRD", "GP": "GLP",
            "GU": "GUM", "GT": "GTM", "GG": "GGY", "GN": "GIN", "GW": "GNB", "GY": "GUY", "HT": "HTI", "HM": "HMD", "VA": "VAT", "HN": "HND",
            "HK": "HKG", "HU": "HUN", "IS": "ISL", "IN": "IND", "ID": "IDN", "IR": "IRN", "IQ": "IRQ", "IE": "IRL", "IM": "IMN", "IL": "ISR",
            "IT": "ITA", "JM": "JAM", "JP": "JPN", "JE": "JEY", "JO": "JOR", "KZ": "KAZ", "KE": "KEN", "KI": "KIR", "KP": "PRK", "KR": "KOR",
            "KW": "KWT", "KG": "KGZ", "LA": "LAO", "LV": "LVA", "LB": "LBN", "LS": "LSO", "LR": "LBR", "LY": "LBY", "LI": "LIE", "LT": "LTU",
            "LU": "LUX", "MO": "MAC", "MG": "MDG", "MW": "MWI", "MY": "MYS", "MV": "MDV", "ML": "MLI", "MT": "MLT", "MH": "MHL", "MQ": "MTQ",
            "MR": "MRT", "MU": "MUS", "YT": "MYT", "MX": "MEX", "FM": "FSM", "MD": "MDA", "MC": "MCO", "MN": "MNG", "ME": "MNE", "MS": "MSR",
            "MA": "MAR", "MZ": "MOZ", "MM": "MMR", "NA": "NAM", "NR": "NRU", "NP": "NPL", "NL": "NLD", "NC": "NCL", "NZ": "NZL", "NI": "NIC",
            "NE": "NER", "NG": "NGA", "NU": "NIU", "NF": "NFK", "MP": "MNP", "NO": "NOR", "OM": "OMN", "PK": "PAK", "PW": "PLW", "PS": "PSE",
            "PA": "PAN", "PG": "PNG", "PY": "PRY", "PE": "PER", "PH": "PHL", "PN": "PCN", "PL": "POL", "PT": "PRT", "PR": "PRI", "QA": "QAT",
            "MK": "MKD", "RO": "ROU", "RU": "RUS", "RW": "RWA", "RE": "REU", "BL": "BLM", "SH": "SHN", "KN": "KNA", "LC": "LCA", "MF": "MAF",
            "PM": "SPM", "VC": "VCT", "WS": "WSM", "SM": "SMR", "ST": "STP", "SA": "SAU", "SN": "SEN", "RS": "SRB", "SC": "SYC", "SL": "SLE",
            "SG": "SGP", "SX": "SXM", "SK": "SVK", "SI": "SVN", "SB": "SLB", "SO": "SOM", "ZA": "ZAF", "GS": "SGS", "SS": "SSD", "ES": "ESP",
            "LK": "LKA", "SD": "SDN", "SR": "SUR", "SJ": "SJM", "SE": "SWE", "CH": "CHE", "SY": "SYR", "TW": "TWN", "TJ": "TJK", "TZ": "TZA",
            "TH": "THA", "TL": "TLS", "TG": "TGO", "TK": "TKL", "TO": "TON", "TT": "TTO", "TN": "TUN", "TR": "TUR", "TM": "TKM", "TC": "TCA",
            "TV": "TUV", "UG": "UGA", "UA": "UKR", "AE": "ARE", "GB": "GBR", "UM": "UMI", "US": "USA", "UY": "URY", "UZ": "UZB", "VU": "VUT",
            "VE": "VEN", "VN": "VNM", "VG": "VGB", "VI": "VIR", "WF": "WLF", "EH": "ESH", "YE": "YEM", "ZM": "ZMB", "ZW": "ZWE"
        }
        headers = {'User-Agent': 'VisionSphere_Data_Sieve/1.0'}
        
        async def fetch_single_country_boundaries(cc: str) -> int:
            iso3 = iso2_to_iso3.get(cc.upper(), cc.upper())
            if len(iso3) != 3:
                logging.warning(f" [⚠️ PRELOAD SKIP] Country code '{cc}' cannot be mapped to a valid 3-letter ISO code.")
                return 0
                
            logging.info(f" [🗺️ THEATER PRELOAD] Initializing bulk boundary harvest for {iso3} starting at {adm_level}...")
            
            # Cascade fallbacks across administrative layers if target tier is structurally missing
            for current_level in [adm_level, "ADM2"]:
                url = f"https://www.geoboundaries.org/api/current/gbOpen/{iso3}/{current_level}/"
                try:
                    # Added follow_redirects=True to handle downstream routing on primary lookup
                    resp = await self.http_client.get(url, headers=headers, follow_redirects=True)
                    
                    if resp.status_code == 404:
                        logging.warning(f"   ↳ [{iso3}] Tier {current_level} returned 404 Not Found. Cascading to next layer...")
                        continue
                        
                    if resp.status_code == 200:
                        meta_data = resp.json()
                        geojson_url = meta_data.get("geojsonUrl") or meta_data.get("gjDownloadURL")
                        
                        if geojson_url:
                            logging.info(f"   ↳ [{iso3}] Target URL extracted. Fetching layout stream for {current_level}...")
                            
                            # CRITICAL FIX: follow_redirects=True forces the client to traverse the 302 Found
                            # redirect issued by GitHub/CDN to extract the actual raw structural GeoJSON bytes.
                            geo_resp = await self.http_client.get(geojson_url, headers=headers, follow_redirects=True)
                            
                            if geo_resp.status_code == 200:
                                layer_data = geo_resp.json()
                                features = layer_data.get("features", [])
                                count = 0
                                
                                for feature in features:
                                    props = feature.get("properties", {})
                                    shape_name = (
                                        props.get("shapeName") or 
                                        props.get("NAME_3") or 
                                        props.get("NAME_2") or 
                                        props.get("NAME_1")
                                    )
                                    if shape_name:
                                        clean_key_base = re.sub(r'[^\w\s-]', '', shape_name).strip().lower()
                                        
                                        # ─── MULTI-THEATER COLLISION AVOIDANCE ───
                                        # 1. Base Namespace Mapping
                                        self.township_boundaries[clean_key_base] = {
                                            "precise_name": shape_name,
                                            "geojson": feature.get("geometry")
                                        }
                                        
                                        # 2. Country-Qualified Namespace Mapping
                                        qualified_key = f"{clean_key_base} {iso3.lower()}"
                                        self.township_boundaries[qualified_key] = {
                                            "precise_name": f"{shape_name} ({iso3.upper()})",
                                            "geojson": feature.get("geometry")
                                        }
                                        count += 1
                                        
                                logging.info(f" [🟢 HARVEST COMPLETE] Successfully processed and cached {count} features for {iso3} using layer {current_level}.")
                                return count  # Break out of levels and return valid features parsed count
                            else:
                                logging.error(f"   ↳ [❌ DOWNLOAD FAILED] CDN final stream target returned status: {geo_resp.status_code}")
                
                except Exception as e:
                    logging.error(f" [🚨 HARVEST FAULT] Boundary download loop failed for {iso3} at tier {current_level}: {e}")
            
            logging.warning(f" [⚠️ STRUC EMPTY] Finished checking fallback layers for {iso3}. 0 items extracted.")
            return 0

        # Dispatch all country network requests concurrently via asyncio.gather
        tasks = [fetch_single_country_boundaries(cc) for cc in country_codes]
        results = await asyncio.gather(*tasks)
        
        # Cross-examine results matrix 
        for cc, total_extracted in zip(country_codes, results):
            logging.info(f" [📊 THEATER METRIC SUMMARY] Country: {cc.upper()} | Total Features Ingested: {total_extracted}")
            
        print(f"RETURNED TOTAL UNIQUE MEMORY INDEXES: {len(self.township_boundaries)}")
        return len(self.township_boundaries) > 0

    async def _execute_temporal_assessment(self, max_retries=5):
        """
        STATIC TEMPORAL MATRIX ENGINE: Replaces fragile Cerebras LLM extraction 
        with a deterministic local intelligence dataset. Completely eliminates 
        network dependencies, rate limits, and safety filter disruptions.
        """
        logging.info(f"Running static deterministic matrix assessment for {self.target_country}...")

        # Normalize the incoming target country string
        target = self.target_country.lower().strip().replace("  ", " ")

        # Master static matrix mapping strategic domains to structured temporal signatures
        STATIC_TEMPORAL_MATRIX = {
            # Eastern Europe
            "ukraine": {"status": "Direct_War", "temporal_start_date": "2022-02-24", "country_codes": ["UA"]},
            "russia": {"status": "Direct_War", "temporal_start_date": "2022-02-24", "country_codes": ["RU"]},
            
            # Middle East
            "israel": {"status": "Direct_War", "temporal_start_date": "2023-10-07", "country_codes": ["IL"]},
            "gaza": {"status": "Direct_War", "temporal_start_date": "2023-10-07", "country_codes": ["PS"]},
            "palestine": {"status": "Direct_War", "temporal_start_date": "2023-10-07", "country_codes": ["PS"]},
            "syria": {"status": "civil_war", "temporal_start_date": "2011-03-15", "country_codes": ["SY"]},
            "yemen": {"status": "civil_war", "temporal_start_date": "2014-09-16", "country_codes": ["YE"]},
            "iraq": {"status": "civil_war", "temporal_start_date": "2014-06-04", "country_codes": ["IQ"]},
            "lebanon": {"status": "stable", "temporal_start_date": "2006-08-14", "country_codes": ["LB"]},
            "iran": {"status": "stable", "temporal_start_date": "1979-04-01", "country_codes": ["IR"]},

            # Africa / Sahel
            "mali": {"status": "civil_war", "temporal_start_date": "2012-01-16", "country_codes": ["ML"]},
            "burkina faso": {"status": "civil_war", "temporal_start_date": "2015-08-04", "country_codes": ["BF"]},
            "niger": {"status": "civil_war", "temporal_start_date": "2023-07-26", "country_codes": ["NE"]},
            "chad": {"status": "civil_war", "temporal_start_date": "2021-04-20", "country_codes": ["TD"]},
            "libya": {"status": "civil_war", "temporal_start_date": "2014-05-16", "country_codes": ["LY"]},
            "sudan": {"status": "civil_war", "temporal_start_date": "2023-04-15", "country_codes": ["SD"]},
            "south sudan": {"status": "civil_war", "temporal_start_date": "2013-12-15", "country_codes": ["SS"]},
            "somalia": {"status": "civil_war", "temporal_start_date": "1991-01-26", "country_codes": ["SO"]},
            "democratic republic of the congo": {"status": "civil_war", "temporal_start_date": "1996-10-24", "country_codes": ["CD"]},
            "congo (drc)": {"status": "civil_war", "temporal_start_date": "1996-10-24", "country_codes": ["CD"]},
            "drc": {"status": "civil_war", "temporal_start_date": "1996-10-24", "country_codes": ["CD"]},
            "central african republic": {"status": "civil_war", "temporal_start_date": "2012-12-10", "country_codes": ["CF"]},
            "ethiopia": {"status": "civil_war", "temporal_start_date": "2020-11-04", "country_codes": ["ET"]},

            # Asia / Central Asia
            "afghanistan": {"status": "civil_war", "temporal_start_date": "1978-04-27", "country_codes": ["AF"]},
            "myanmar": {"status": "civil_war", "temporal_start_date": "2021-02-01", "country_codes": ["MM"]},
            "burma": {"status": "civil_war", "temporal_start_date": "2021-02-01", "country_codes": ["MM"]},
            "pakistan": {"status": "stable", "temporal_start_date": "1947-08-14", "country_codes": ["PK"]},

            # Americas / Caribbean
            "haiti": {"status": "civil_war", "temporal_start_date": "2018-07-07", "country_codes": ["HT"]},
            "venezuela": {"status": "stable", "temporal_start_date": "1999-02-02", "country_codes": ["VE"]},
            "colombia": {"status": "stable", "temporal_start_date": "2016-11-24", "country_codes": ["CO"]},
            "ecuador": {"status": "stable", "temporal_start_date": "2024-01-09", "country_codes": ["EC"]}
        }

        # Global ISO-3166 alpha-2 dictionary to safely shield standard stable domains from slicing errors
        ISO_BASELINE_OVERRIDES = {
            "united kingdom": "GB", "united states": "US", "united arab emirates": "AE",
            "south korea": "KR", "north korea": "KP", "vietnam": "VN", "philippines": "PH", 
            "chile": "CL", "china": "CN", "germany": "DE", "france": "FR", "japan": "JP", 
            "canada": "CA", "australia": "AU", "brazil": "BR", "india": "IN", "mexico": "MX"
        }

        # Yield control momentarily to maintain compatibility with the async engine loop
        await asyncio.sleep(0.01)

        if target in STATIC_TEMPORAL_MATRIX:
            profile = STATIC_TEMPORAL_MATRIX[target]
            logging.info(f"[📊 MATRIX HIT] {self.target_country} successfully validated. Footprint Status: {profile['status']}")
            return profile
        else:
            # Resolve ISO-3166 alpha-2 properties safely for non-conflict zones
            iso_code = ISO_BASELINE_OVERRIDES.get(target, target[:2].upper())
            
            # Standard structural safeguard to defend downstream geo-slicers
            if len(iso_code) != 2 or not iso_code.isalpha():
                iso_code = "XX"

            stable_profile = {
                "status": "stable",
                "temporal_start_date": "2026-01-01",
                "country_codes": [iso_code]
            }
            logging.info(f"[📊 MATRIX MISS] {self.target_country} matched standard stable baseline. Assigned ISO: {iso_code}")
            return stable_profile

    def _extract_locations_from_text(self, text: str) -> List[str]:
        """
        Uses spaCy NLP combined with Regex Capitalization Recovery to extract locations.
        Ensures obscure or foreign town names missed by the English NLP model are caught.
        """
        doc = nlp(text)
        locations = set()
        
        # Pass 1: Standard spaCy NER
        for ent in doc.ents:
            if ent.label_ in ["GPE", "LOC"]:
                clean_loc = re.sub(r'[^\w\s-]', '', ent.text).strip()
                if len(clean_loc) > 2:
                    locations.add(clean_loc)
                    
        # Pass 2: Context-Aware Regex Recovery (Catches words missed by standard NLP)
        indicators = r"\b(in|near|at|outside|of|from|towards|captured|attacked)\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)?)"
        matches = re.findall(indicators, text)
        for indicator, matched_location in matches:
            # Exclude common capitalized sentence starters or days of the week
            ignore_list = ["The", "A", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday", "This"]
            if matched_location not in ignore_list:
                locations.add(matched_location.strip())
                
        return list(locations)

    async def _fetch_precise_boundary(self, location_name: str, country_codes: List[str]) -> Optional[Dict[str, Any]]:
        """
        Two-Stage Verification Pipeline:
        Stage 1: Uses OpenCage/LocationIQ to normalize the name and grab the exact OSM ID.
        Stage 2: Uses Nominatim/Geoapify to fetch the verified GeoJSON polygon.
        """
        async with self.geocode_semaphore:
            iso_csv = ",".join(country_codes).lower()
            iso_upper = country_codes[0].upper() if country_codes else ""
            
            logging.info(f" [🛰️ GEO-PIPELINE] Initiating two-stage resolution for: '{location_name}'")

            # =========================================================================
            # STAGE 1: IDENTITY RESOLUTION (Get Exact OSM ID and Normalized Name)
            # =========================================================================
            osm_id, osm_type, canonical_name = None, None, location_name
            
            # Try OpenCage first (Superior at parsing messy text into exact OSM IDs)
            oc_meta = await self._resolve_id_opencage(location_name, iso_csv)
            if oc_meta and oc_meta[0]:
                osm_id, osm_type, canonical_name = oc_meta
            else:
                # Fallback to LocationIQ (Commercial wrapper for OSM)
                liq_meta = await self._resolve_id_locationiq(location_name, iso_csv)
                if liq_meta and liq_meta[0]:
                    osm_id, osm_type, canonical_name = liq_meta
                elif oc_meta or liq_meta:
                    # If we got a name but no ID, save the cleaned canonical name
                    canonical_name = oc_meta[2] if oc_meta else liq_meta[2]

            osm_str = f"{osm_type}{osm_id}" if osm_id else "Unresolved"
            logging.info(f"   ↳ [STAGE 1: IDENTITY] Normalized Name: '{canonical_name}' | OSM Fingerprint: {osm_str}")

            # =========================================================================
            # STAGE 2: GEOMETRY FETCHING (Extract the actual Polygon)
            # =========================================================================
            result = None

            # Route A: 100% Accurate Direct ID Lookup
            if osm_id and osm_type:
                logging.info(f"   ↳ [STAGE 2: GEOMETRY] Performing exact boundary lookup for ID {osm_str}...")
                result = await self._fetch_polygon_by_id_nominatim(osm_id, osm_type)
            
            # Route B: Text Search Fallback Cascades (If no ID was found, use the cleaned name)
            if not result:
                engines = [
                    ("Nominatim Search", lambda: self._try_nominatim_search(canonical_name, iso_csv)),
                    ("Geoapify", lambda: self._try_geoapify(canonical_name, iso_upper)),
                    ("geoBoundaries", lambda: self._try_geoboundaries(canonical_name, iso_upper))
                ]
                
                for engine_name, engine_callable in engines:
                    try:
                        logging.info(f"   ↳ [STAGE 2: FALLBACK] Cascading to {engine_name}...")
                        result = await engine_callable()
                        if result and result.get("geojson", {}).get("type") in ["Polygon", "MultiPolygon"]:
                            break
                        else:
                            result = None # Clear bad point data
                    except Exception as e:
                        logging.warning(f"   ↳ [⚠️ ENGINE FAULT] {engine_name} failed: {e}")

            # =========================================================================
            # FINAL VALIDATION & OUTPUT
            # =========================================================================
            if result and result.get("geojson", {}).get("type") in ["Polygon", "MultiPolygon"]:
                logging.info(f" [🟢 BOUNDARY LOCKED] '{canonical_name}' successfully mapped as a boundary asset.")
                result["precise_name"] = canonical_name
                return result
            
            logging.error(f" [❌ GEOMETRY EXHAUSTED] Could not extract valid MultiPolygon for '{location_name}'. Dropping event.")
            return None

    # -------------------------------------------------------------------------
    # STAGE 1 HELPER ENGINES (Identity Resolution)
    # -------------------------------------------------------------------------
    async def _resolve_id_opencage(self, location_name: str, iso_csv: str) -> Optional[tuple]:
        api_key = os.getenv("OPENCAGE_KEY")
        if not api_key: return None
        url = "https://api.opencagedata.com/geocode/v1/json"
        params = {'q': location_name, 'key': api_key, 'countrycode': iso_csv, 'limit': 1, 'no_annotations': 0}
        resp = await self.http_client.get(url, params=params)
        
        if resp.status_code == 200 and resp.json().get("results"):
            res = resp.json()["results"][0]
            # Extract cleanest city/county name
            name = res.get("components", {}).get("city") or res.get("components", {}).get("county") or location_name
            # Parse OSM URL for the exact Relation or Way ID
            osm_url = res.get("annotations", {}).get("OSM", {}).get("edit_url", "")
            match = re.search(r'(relation|way)/(\d+)', osm_url)
            if match:
                return (match.group(2), "R" if match.group(1) == "relation" else "W", name)
            return (None, None, name)
        return None

    async def _resolve_id_locationiq(self, location_name: str, iso_csv: str) -> Optional[tuple]:
        api_key = os.getenv("LOCATIONIQ_KEY")
        if not api_key: return None
        url = "https://us1.locationiq.com/v1/search.php"
        params = {'key': api_key, 'q': location_name, 'format': 'json', 'countrycodes': iso_csv, 'limit': 1}
        resp = await self.http_client.get(url, params=params)
        
        if resp.status_code == 200 and resp.json():
            res = resp.json()[0]
            name = res.get("display_name", "").split(",")[0]
            o_type = res.get("osm_type")
            osm_type_char = "R" if o_type == "relation" else ("W" if o_type == "way" else None)
            if osm_type_char:
                return (res.get("osm_id"), osm_type_char, name)
            return (None, None, name)
        return None

    # -------------------------------------------------------------------------
    # STAGE 2 HELPER ENGINES (Geometry Fetching)
    # -------------------------------------------------------------------------
    async def _fetch_polygon_by_id_nominatim(self, osm_id: str, osm_type: str) -> Optional[Dict[str, Any]]:
        await asyncio.sleep(1.0) # Obey TOS
        url = "https://nominatim.openstreetmap.org/lookup"
        params = {'osm_ids': f"{osm_type}{osm_id}", 'format': 'jsonv2', 'polygon_geojson': 1}
        resp = await self.http_client.get(url, params=params)
        if resp.status_code == 200 and resp.json():
            candidate = resp.json()[0]
            return {"geojson": candidate.get("geojson", {})}
        return None

    async def _try_nominatim_search(self, location_name: str, iso_csv: str) -> Optional[Dict[str, Any]]:
        await asyncio.sleep(1.0) # Obey TOS
        url = "https://nominatim.openstreetmap.org/search"
        params = {'q': location_name, 'format': 'jsonv2', 'polygon_geojson': 1, 'countrycodes': iso_csv, 'limit': 1}
        resp = await self.http_client.get(url, params=params)
        if resp.status_code == 200 and resp.json():
            return {"geojson": resp.json()[0].get("geojson", {})}
        return None

    async def _try_geoapify(self, location_name: str, iso_upper: str) -> Optional[Dict[str, Any]]:
        api_key = os.getenv("GEOAPIFY_GEOCODE_KEY")
        if not api_key: return None
        url = "https://api.geoapify.com/v1/geocode/search"
        params = {'text': location_name, 'filter': f"countrycode:{iso_upper.lower()}", 'lang': 'en', 'limit': 1, 'apiKey': api_key}
        resp = await self.http_client.get(url, params=params)
        if resp.status_code == 200 and resp.json().get("features"):
            return {"geojson": resp.json()["features"][0].get("geometry", {})}
        return None

    async def _try_geoboundaries(self, location_name: str, iso_upper: str) -> Optional[Dict[str, Any]]:
        """
        Hits the open-source global administrative database api.
        Resolves ISO2 to ISO3 codes, dynamically queries layer metadata URLs,
        and caches vector assets in memory to optimize search sweeps.
        """
        # 1. Translate standard ISO-2 codes to geoBoundaries required ISO-3 format
        iso2_to_iso3 = {
            "MM": "MMR", "UA": "UKR", "RU": "RUS", "PS": "PSE", "IL": "ISR",
            "SY": "SYR", "YE": "YEM", "SD": "SDN", "SO": "SOM", "CD": "COD",
            "TW": "TWN", "SD": "SDN", "SS": "SSD", "LY": "LBY", "IQ": "IRQ"
        }
        
        iso3 = iso2_to_iso3.get(iso_upper.upper(), iso_upper.upper())
        if len(iso3) != 3:
            logging.warning(f"   ↳ [geoBoundaries] Skipping. Could not map '{iso_upper}' to a valid 3-letter ISO code.")
            return None

        # Initialize an in-memory layer cache on the instance if it doesn't exist
        if not hasattr(self, "_geoboundaries_cache"):
            self._geoboundaries_cache = {}

        # 2. Check standard administrative levels 1 and 2
        for adm_level in ["ADM1", "ADM2"]:
            cache_key = f"{iso3}_{adm_level}"
            features = []

            # Pull from runtime memory if already fetched during this engine execution loop
            if cache_key in self._geoboundaries_cache:
                features = self._geoboundaries_cache[cache_key]
            else:
                # Retrieve the structural metadata index object
                url = f"https://www.geoboundaries.org/api/current/GBHUM/{iso3}/{adm_level}/"
                try:
                    resp = await self.http_client.get(url)
                    if resp.status_code == 200:
                        meta_data = resp.json()
                        geojson_url = meta_data.get("gjDownloadURL")
                        
                        if geojson_url:
                            logging.info(f"   ↳ [geoBoundaries] Fetching full {adm_level} country vector asset from mirror...")
                            geo_resp = await self.http_client.get(geojson_url)
                            if geo_resp.status_code == 200:
                                features = geo_resp.json().get("features", [])
                                # Cache the layer dataset to bypass network loops on the next town check
                                self._geoboundaries_cache[cache_key] = features
                except Exception as cache_err:
                    logging.warning(f"   ↳ [geoBoundaries] Layer download error for {cache_key}: {cache_err}")
                    continue

            # 3. Perform string-matching checks across the resolved layer features
            for feat in features:
                properties = feat.get("properties", {})
                # geoBoundaries standard property targets for nomenclature strings
                feat_name = properties.get("shapeName") or properties.get("NAME_1") or properties.get("NAME_2") or ""
                
                if feat_name and (location_name.lower() in feat_name.lower() or feat_name.lower() in location_name.lower()):
                    logging.info(f"   ↳ [🎯 geoBoundaries HIT] Matched location text to shape boundary entity: '{feat_name}'")
                    return {"geojson": feat.get("geometry", {})}
                    
        return None

    async def get_crmm_data(self):
        """
        The Main Orchestrator (Fully Concurrent with Real-Time Short-Circuit Snapshots).
        1. Evaluates Temporal Context via Cerebras
        2. Preloads national spatial geometry cache
        3. Loops through cache targets concurrently using a bounded worker pool
        4. Instantly short-circuits internal article arrays upon validating live tactical text
        5. Returns a unified dataset for downstream AI consumption with performance tracking.
        """
        import time

        # Start master execution timer
        pipeline_start = time.perf_counter()

        logging.info("==================================================================")
        logging.info(f"🚀 INITIATING TACTICAL DATA SIEVE PIPELINE FOR: {self.target_country}")
        logging.info("==================================================================")
        
        # 1. Temporal Assessment Strategy
        temporal_data = await self._execute_temporal_assessment()
        status = temporal_data.get("status", "active_conflict")
        country_codes = temporal_data.get("country_codes", [])
        
        logging.info(f" [⚙️ SYSTEM CONTEXT] Status: {status.upper()} | Mode: Concurrent Fuzzy Snapshot Short-Circuiting")
        
        # 2. Preload Cache Layer
        await self.preload_all_township_boundaries(country_codes=country_codes)
        
        # Deduplicate the global memory map by base location name to avoid redundant API hits
        unique_townships = {}
        for k, v in self.township_boundaries.items():
            base_name = v["precise_name"]
            if base_name.lower() not in unique_townships:
                unique_townships[base_name.lower()] = v

        logging.info(f" [📡 SNAPSHOT MONITORING] Commencing concurrent verification sweeps for {len(unique_townships)} unique regions...")

        # Define an isolated, bounded concurrent worker for individual township sweeps
        # This keeps the short-circuit logic fast per region while running regions in parallel
        sem = asyncio.Semaphore(15)  # Controls max simultaneous regional API queries

        async def sweep_individual_township(base_lower, asset):
            async with sem:
                admin_name = asset["precise_name"]
                try:
                    # Call date-free, multi-indexer real-time engine
                    snapshot_items = await self.dorker.fetch_latest_snapshot(
                        administrative_name=admin_name, 
                        country=self.target_country, 
                        max_results=2
                    )
                    
                    # Short-circuit logic: Scan the ranked fuzzy feed for this specific region
                    for item in snapshot_items:
                        news_body = item.get("body") or item.get("title", "")
                        fuzzy_score = item.get("fuzzy_match_score", 0)
                        
                        # Verify confidence tier match from the SequenceMatcher output
                        if fuzzy_score >= 0.65:
                            logging.info(f"   ↳ [🔥 SHORT-CIRCUIT LOCK] Live hit found for '{admin_name}' (Fuzzy Score: {fuzzy_score:.2f}). Breaking early.")
                            return {
                                "status": "matched",
                                "base_lower": base_lower,
                                "package": {
                                    "country": self.target_country,
                                    "status": status,
                                    "location": admin_name,
                                    "geojson_boundaries": asset["geojson"],
                                    "news_content": news_body,
                                    "fuzzy_score": fuzzy_score
                                }
                            }
                except Exception as e:
                    logging.error(f" [❌ WORKER FAULT] Snapshot extraction failed for {admin_name}: {e}")
                
                # Fallback / Default return for unmatched regions to maintain layout compatibility
                return {
                    "status": "missed",
                    "base_lower": base_lower,
                    "package": {
                        "missed_country": self.target_country,
                        "missed_location": admin_name,
                        "missed_geojson_boundaries": asset["geojson"],
                    }
                }

        # 3. Fire High-Velocity Parallel Execution Mesh
        tasks = [sweep_individual_township(base_lower, asset) for base_lower, asset in unique_townships.items()]
        completed_sweeps = await asyncio.gather(*tasks)

        # 4. Compile and separate results arrays safely
        unified_dataset = []
        matched_packages = []

        for result in completed_sweeps:
            unified_dataset.append(result["package"])
            if result["status"] == "matched":
                matched_packages.append(result["package"])

        # Calculate final execution duration metrics
        pipeline_duration = time.perf_counter() - pipeline_start

        # ─── DEDICATED HUMAN-READABLE INTELLIGENCE FEED ───
        print("\n" + "="*80)
        print("📡 LIVE CONFLICT INTELLIGENCE FEED (FIRST 10 REAL-TIME MATCHED TARGETS)")
        print("="*80)
        if matched_packages:
            for i, item in enumerate(matched_packages[:10], start=1):
                print(f"\nGEO BOUNDARIES: {item['geojson_boundaries']}\n")
                print(f"\n[🔥 TARGET IMPACT MATCH {i}/{len(matched_packages)}]")
                print(f" 📍 Location:      {item['location']} ({item['country']})")
                print(f" ⚠️ Threat Status:  {item['status'].upper()}")
                print(f" 🎯 Fuzzy Score:    {item['fuzzy_score']:.2f}")
                print(f" 📝 News Content:\n{item['news_content']}\n")
                print("-" * 60)
        else:
            print(" [] No valid real-time conflict snapshot matches were found during short-circuit checks.")
        
        # ─── TELEMETRY DASHBOARD ───
        print("\n" + "="*80)
        print("⏱️  TACTICAL PIPELINE PERFORMANCE METRICS")
        print("="*80)
        print(f" 📊 Unique Targets Monitored: {len(unique_townships)}")
        print(f" 🎯 Live Confirmed Hits:      {len(matched_packages)}")
        print(f" ⚡ Total Pipeline Velocity:  {pipeline_duration:.4f} seconds")
        print("="*80 + "\n")
        
        return unified_dataset

if __name__ == "__main__":
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
        
    sieve = CRMMGeoCoderNewsBoundary(target_country="Myanmar")
    asyncio.run(sieve.get_crmm_data())