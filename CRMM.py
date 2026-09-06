import os
import asyncio
import random
import json
import itertools
from pydantic import BaseModel, Field, ValidationError

# Cerebras SDK
from cerebras.cloud.sdk import Cerebras

# OpenRouter / OpenAI SDK Compatibility
from openai import AsyncOpenAI

# Self-contained master functions from your files
from DINEI_left_hand_crmm import start_crmm_dossier
from DINEI_right_hand_CRMM import run_intelligence_pipeline

class DeliveryRoutingEvaluation(BaseModel):
    requires_dynamic_routing: bool = Field(
        description="TRUE if the country has high administrative friction, unpredictable road networks, or complex customs requiring dynamic daily routing."
    )
    route_unpredictability_score: float = Field(
        description="Float from 0.0 to 1.0 indicating how often civilian postal routes must be recalculated."
    )
    
WORLD_COUNTRIES = [
    "afghanistan", "albania", "algeria", "andorra", "angola", "antigua and barbuda", 
    "argentina", "armenia", "australia", "austria", "azerbaijan", "bahamas", "bahrain", 
    "bangladesh", "barbados", "belarus", "belgium", "belize", "benin", "bhutan", 
    "bolivia", "bosnia and herzegovina", "botswana", "brazil", "brunei", "bulgaria", 
    "burkina faso", "burundi", "cabo verde", "cambodia", "cameroon", "canada", 
    "central african republic", "chad", "chile", "china", "colombia", "comoros", 
    "congo", "costa rica", "croatia", "cuba", "cyprus", "czech republic", "denmark", 
    "djibouti", "dominica", "dominican republic", "ecuador", "egypt", "el salvador", 
    "equatorial guinea", "eritrea", "estonia", "eswatini", "ethiopia", "fiji", 
    "finland", "france", "gabon", "gambia", "georgia", "germany", "ghana", "greece", 
    "grenada", "guatemala", "guinea", "guinea-bissau", "guyana", "haiti", "honduras", 
    "hungary", "iceland", "india", "indonesia", "iran", "iraq", "ireland", "israel", 
    "italy", "jamaica", "japan", "jordan", "kazakhstan", "kenya", "kiribati", 
    "kuwait", "kyrgyzstan", "laos", "latvia", "lebanon", "lesotho", "liberia", 
    "libya", "liechtenstein", "lithuania", "luxembourg", "madagascar", "malawi", 
    "malaysia", "maldives", "mali", "malta", "marshall islands", "mauritania", 
    "mauritius", "mexico", "micronesia", "moldova", "monaco", "mongolia", "montenegro", 
    "morocco", "mozambique", "myanmar", "namibia", "nauru", "nepal", "netherlands", 
    "new zealand", "nicaragua", "niger", "nigeria", "north korea", "north macedonia", 
    "norway", "oman", "pakistan", "palau", "panama", "papua new guinea", "paraguay", 
    "peru", "philippines", "poland", "portugal", "qatar", "romania", "russia", 
    "rwanda", "saint kitts and nevis", "saint lucia", "saint vincent", "samoa", 
    "san marino", "sao tome and principe", "saudi arabia", "senegal", "serbia", 
    "seychelles", "sierra leone", "singapore", "slovakia", "slovenia", "solomon islands", 
    "somalia", "south africa", "south korea", "south sudan", "spain", "sri lanka", 
    "sudan", "suriname", "sweden", "switzerland", "syria", "taiwan", "tajikistan", 
    "tanzania", "thailand", "timor-leste", "togo", "tonga", "trinidad and tobago", 
    "tunisia", "turkey", "turkmenistan", "tuvalu", "uganda", "ukraine", "united arab emirates", 
    "united kingdom", "united states", "uruguay", "uzbekistan", "vanuatu", "venezuela", 
    "vietnam", "yemen", "zambia", "zimbabwe"
]

async def upload_json_to_idrive_async(payload_dict: dict, bucket_key: str):
    """
    IN-MEMORY CLOUD INJECTION: Bypasses local disk I/O completely.
    Offloads synchronous boto3 S3 put_object to an isolated threadpool.
    Enforces absolute block-and-wait sequencing for structural map integrity.
    """
    if not payload_dict:
        print(f" [⚠️ UPLINK ABORTED] Target payload for '{bucket_key}' is empty! Mapping extraction failed.")
        return

    endpoint_url = os.getenv("IDRIVE_ENDPOINT")
    bucket_name = os.getenv("IDRIVE_BUCKET")
    access_key = os.getenv("IDRIVE_ACCESS_KEY")
    secret_key = os.getenv("IDRIVE_SECRET_KEY")

    if not all([endpoint_url, bucket_name, access_key, secret_key]):
        print(f" [⚠️ CLOUD] Upload bypassed for {bucket_key}: Missing target infrastructure variables.")
        return

    print(f" [📡 SYNC INITIALIZED] Pushing RAM payload -> idrive://{bucket_name}/{bucket_key} ...")

    def _execute_upload():
        import boto3
        from botocore.config import Config
        
        s3_client = boto3.client(
            "s3",
            endpoint_url=endpoint_url,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name="us-east-1",  
            config=Config(signature_version='s3v4')  
        )
        
        json_string = json.dumps(payload_dict, indent=4, ensure_ascii=False)
        
        s3_client.put_object(
            Bucket=bucket_name, 
            Key=bucket_key, 
            Body=json_string.encode('utf-8'),
            ContentType='application/json'
        )

    try:
        await asyncio.to_thread(_execute_upload)
        print(f" [☁️ CLOUD SUCCESS] Synchronized profile securely -> idrive://{bucket_name}/{bucket_key}")
    except Exception as e:
        print(f" [❌ CLOUD ERROR] Critical ingestion fault during upload execution for {bucket_key}: {e}")
        raise e

def clean_and_parse_json(raw_content: str) -> DeliveryRoutingEvaluation:
    """Helper function to strip markdown formatting and parse against the schema."""
    if not raw_content:
        raise ValueError("Empty content received.")
        
    raw_json = raw_content.strip()
    if raw_json.startswith("```json"):
        raw_json = raw_json[7:]
    elif raw_json.startswith("```"):
        raw_json = raw_json[3:]
    if raw_json.endswith("```"):
        raw_json = raw_json[:-3]
        
    raw_json = raw_json.strip()
    return DeliveryRoutingEvaluation.model_validate_json(raw_json)

async def evaluate_conflict_need(country: str, cerebras_client: Cerebras = None, openrouter_client: AsyncOpenAI = None, max_retries: int = 4) -> bool:
    """
    STATIC MATRIX ENGINE: Replaces all fragile cloud LLM routing with a 
    deterministic local dataset lookup covering global active conflict zones 
    and highly volatile logistics sectors. Completely immune to API errors.
    """
    # Clean and normalize the incoming string
    target = country.lower().strip().replace("  ", " ")

    # Comprehensive global mapping of active high-friction/conflict sectors
    STATIC_CONFLICT_DATASET = {
        # Eastern Europe
        "ukraine": True,
        "russia": True,
        
        # Middle East
        "israel": True,
        "gaza": True,
        "palestine": True,
        "syria": True,
        "yemen": True,
        "iraq": True,
        "lebanon": True,
        "iran": True,
        
        # North / West / Sahel Africa
        "mali": True,
        "burkina faso": True,
        "niger": True,
        "chad": True,
        "libya": True,
        "nigeria": True,
        "cameroon": True,
        
        # Central / East Africa
        "sudan": True,
        "south sudan": True,
        "somalia": True,
        "democratic republic of the congo": True,
        "congo (drc)": True,
        "drc": True,
        "central african republic": True,
        "ethiopia": True,
        "eritrea": True,
        "mozambique": True,
        
        # Asia / Central Asia
        "afghanistan": True,
        "myanmar": True,
        "burma": True,
        "pakistan": True,
        
        # Latin America / Caribbean
        "haiti": True,
        "venezuela": True,
        "ecuador": True,
        "colombia": True
    }

    # Yield control momentarily to prevent blocking the async loop execution thread
    await asyncio.sleep(0.01)

    # Evaluate the territory matrix
    requires_dynamic_routing = STATIC_CONFLICT_DATASET.get(target, False)
    
    # Mocked score format to preserve telemetry layout compatibility
    mock_score = 0.99 if requires_dynamic_routing else 0.01

    if requires_dynamic_routing:
        print(f" [📊 STATIC MAP MATCH] {country} -> Highly volatile infrastructure detected. (Dynamic Routing: True, Score: {mock_score})")
        return True
    else:
        print(f" [📊 STATIC MAP MATCH] {country} -> Baseline stable routing infrastructure. (Dynamic Routing: False, Score: {mock_score})")
        return False

async def orchestrate_node(country: str, cerebras_client: Cerebras, openrouter_client: AsyncOpenAI):
    """
    STRICT SEQUENTIAL PROCESSING.
    The engine will halt here and wait for all tasks to complete before returning.
    """
    print(f"\n{'='*60}\n[🌍 PROCESSING TARGET] {country.upper()}\n{'='*60}")
    
    # 1. Fire the Left Hand Dossier
    print(f"[📡 LH] Executing baseline intelligence gathering...")
    lh_data = await start_crmm_dossier(country)
    
    if lh_data:
        lh_filename = f"{country.lower().replace(' ', '_')}_crmm_left_hand.json"
        await upload_json_to_idrive_async(lh_data, f"dossiers/{lh_filename}")
    else:
        print(f"[⚠️ LH NULL] Base intelligence extraction failed for {country}.")

    # 2. Check infrastructure variance parameters 
    needs_mapping = await evaluate_conflict_need(country, cerebras_client, openrouter_client)
    
    # 3. Geometric Expansion Processing Block
    if needs_mapping:
        print(f"[⚔️ RH] Conflict verified. Triggering master geospatial pipeline. (AWAITING COMPLETION...)")
        
        rh_data = await run_intelligence_pipeline(country)
        
        if rh_data:
            rh_filename = f"admin3_conflict_{country.lower().replace(' ', '_')}.geojson"
            await upload_json_to_idrive_async(rh_data, f"geospatial/{rh_filename}")
            print(f"[✅ RH COMPLETE] Heavy geometry for {country} finished and securely uploaded.")
        else:
            print(f"[⚠️ RH NULL] Geometric polygon slicing failed for {country}.")
    else:
        print(f"[🕊️ RH] Theater stable. Bypassing deep boundary slicing layer.")

if __name__ == "__main__":
    cerebras_api_key = os.getenv("CERABRAS_KEY_2")
    if not cerebras_api_key:
        print("[🚨 CRITICAL] No Cerebras API key found in env (CERABRAS_KEY_2)!")
        exit(1)
        
    openrouter_api_key = os.getenv("OPENROUTER_KEY_1")
    openrouter_client = None
    
    if openrouter_api_key:
        openrouter_client = AsyncOpenAI(
            base_url="https://openrouter.ai/api/v1",
            api_key=openrouter_api_key,
            max_retries=3,
        )
            
    cerebras_client = Cerebras(api_key=cerebras_api_key)
    
    # ─── CLOUD-NATIVE STATE MANAGEMENT (IDRIVE S3) ───
    def get_s3_state_client():
        import boto3
        from botocore.config import Config
        return boto3.client(
            "s3",
            endpoint_url=os.getenv("IDRIVE_ENDPOINT"),
            aws_access_key_id=os.getenv("IDRIVE_ACCESS_KEY"),
            aws_secret_access_key=os.getenv("IDRIVE_SECRET_KEY"),
            region_name="us-east-1",  
            config=Config(signature_version='s3v4')  
        )

    STATE_BUCKET = os.getenv("IDRIVE_BUCKET")
    STATE_KEY = "system_state/crmm_pointer.json"
    
    def load_matrix_pointer() -> int:
        """Pulls the current A-to-Z index from your IDrive cloud bucket."""
        if not STATE_BUCKET: return 0
        try:
            s3 = get_s3_state_client()
            resp = s3.get_object(Bucket=STATE_BUCKET, Key=STATE_KEY)
            state_data = json.loads(resp['Body'].read().decode('utf-8'))
            return int(state_data.get("current_index", 0))
        except Exception:
            # Defaults to 0 (Afghanistan) if the file doesn't exist yet on the first run
            return 0 

    def save_matrix_pointer(idx: int):
        """Saves the advanced pointer back to IDrive to survive Hugging Face reboots."""
        if not STATE_BUCKET: return
        try:
            s3 = get_s3_state_client()
            payload = json.dumps({"current_index": idx})
            s3.put_object(
                Bucket=STATE_BUCKET, 
                Key=STATE_KEY, 
                Body=payload.encode('utf-8'),
                ContentType='application/json'
            )
        except Exception as e:
            print(f"[⚠️ CLOUD STATE ERROR] Failed to save pointer to IDrive: {e}")

    async def run_single_shot_node():
        # Ensure your target country matrix list is sorted strictly from A to Z
        WORLD_COUNTRIES.sort()
        
        current_idx = load_matrix_pointer()
        if current_idx >= len(WORLD_COUNTRIES):
            current_idx = 0  # Re-loop pointer back to Afghanistan if bounds exceeded
            
        target_country = WORLD_COUNTRIES[current_idx]
        
        print(f"\n[☁️ CLOUD STATE LOADED]")
        print(f"Targeting country index [{current_idx}/{len(WORLD_COUNTRIES)-1}] -> {target_country.upper()}")
        
        try:
            # Absolute processing envelope safely tracking long runs
            await orchestrate_node(target_country, cerebras_client, openrouter_client)
        except Exception as e:
            print(f"[❌ PIPELINE ERROR] Failed processing node '{target_country}': {e}")
            
        # Shift tracking pointer ahead for the next global loop and sync to cloud
        next_idx = (current_idx + 1) % len(WORLD_COUNTRIES)
        save_matrix_pointer(next_idx)
        print(f"[☁️ POINTER ADVANCED] Next cycle pointer assigned to index {next_idx}: {WORLD_COUNTRIES[next_idx].upper()}\n")

    try:
        asyncio.run(run_single_shot_node())
    except KeyboardInterrupt:
        print("\n[🚨 ENGINE SHUTDOWN] Loop manually stopped.")