import os
import asyncio
import itertools
from dotenv import load_dotenv
from cerebras.cloud.sdk import Cerebras

load_dotenv()

carrier_profile = {
            "name": "Naval Strike Group & Carrier Deployment Analyst",
            "goal": (
                "Identify and extract locations of active aircraft carrier deployments, "
                "carrier strike group (CSG) transits, maritime strike group operations, "
                "amphibious assault ship positions, and strategic sovereign naval deployments worldwide."
            ),
            "tags": "aircraft carrier deployment, carrier strike group, CVN deployment, transit, naval exercise, strike group",
            "vibe": "Strategic, Sovereign-Level, Maritime-Command"
        }

class IntelBrain:
    """
    The Intelligence Synthesis Core.
    Focused exclusively on the generation of high-density OSINT vectors.
    """
    def __init__(self, profile_config=None):
        """
        Initializes the intelligence engine with a polymorphic personality structure
        and a production-grade multi-key client rotation vault.
        """
        import os
        import itertools

        # 1. Internal Profile Registry (Diversified Baseline Profiles)
        self.profiles = {
            "universal_horizon": {
                "name": "Universal Horizon Monitor",
                "goal": "global shifts including economic volatility, geopolitical conflict, social uprisings, and cultural trends",
                "tags": "market crash, airstrike, protest, riot, championship, tech launch, breakthrough",
                "vibe": "all-encompassing, high-velocity, and diverse"
            },
            "kinetic_tactical": {
                "name": "Kinetic Tactical Threat Specialist",
                "goal": "physical security operations, asset locations, movements, and immediate localized threats",
                "tags": "convoys, deployments, logistics, tracking, perimeters, telemetry, intercepts",
                "vibe": "laser-focused, direct, physical, and tactical"
            },
            "cyber_intelligence": {
                "name": "Cyber Intelligence Synthesizer",
                "goal": "digital perimeter anomalies, zero-day discovery, infrastructure exploits, and network intrusions",
                "tags": "exploits, breaches, malware, critical infrastructure, ransomware, firmware",
                "vibe": "highly technical, analytical, covert, and precise"
            }
        }
        
        # 2. Dynamic Configuration Injection Architecture
        if isinstance(profile_config, dict):
            # Defensive validation: Ensure the custom configuration dictionary matches the required schema
            required_fields = ["name", "goal", "tags", "vibe"]
            missing_fields = [field for field in required_fields if field not in profile_config]
            if missing_fields:
                raise ValueError(f"[!] CRITICAL: Custom profile configuration is missing required structural keys: {missing_fields}")
            
            self.config = profile_config
            print(f"[🧠] IntelBrain Persona Locked: Direct injection of custom operational profile '{self.config['name']}'.")
            
        elif isinstance(profile_config, str):
            if profile_config in self.profiles:
                self.config = self.profiles[profile_config]
                print(f"[🧠] IntelBrain Persona Locked: Registry profile '{self.config['name']}' activated.")
            else:
                raise ValueError(f"[!] CRITICAL: Profile name '{profile_config}' not found in internal registry.")
                
        else:
            # Safe operational fallback if instantiated without arguments, while logging an explicit notice
            self.config = self.profiles["universal_horizon"]
            print(f"[⚠️] IntelBrain Persona Notice: No explicit profile specified. Initializing default framework layer.")

        # 3. Multi-Key Vault Rotation (Cerebras)
        # Supports keys CERABRAS_KEY_1 through CERABRAS_KEY_10
        keys = [os.getenv(f"CERABRAS_KEY_{i}") for i in range(1, 11)]
        self.valid_keys = [k for k in keys if k]
        
        if not self.valid_keys:
            # Fallback to standard env var if numbered ones aren't found
            standard_key = os.getenv("CEREBRAS_API_KEY")
            if standard_key:
                self.valid_keys = [standard_key]
            else:
                raise ValueError("[!] CRITICAL: No CEREBRAS API keys found in environment.")
            
        # Initialize Cerebras clients across valid keys
        self.clients = [Cerebras(api_key=k) for k in self.valid_keys]
        self.client_cycle = itertools.cycle(self.clients)
        print(f"[✅] IntelBrain Pipeline Online: Synchronized with {len(self.valid_keys)} active neural paths.")

    async def generate_vectors(self, count=15):
        """
        Generates elite intelligence search vectors heavily saturated with persona directives.
        Mentions operational personality repeatedly to force absolute behavioral compliance.
        """
        import re

        # THE HYPER-DENSE PERSONA-SATURATED PROMPT MESH (ZERO EXAMPLES)
        prompt = (
            f"CRITICAL IDENTITY: You are the {self.config['name']}.\n"
            f"OPERATIONAL PERSONALITY VIBE: {self.config['vibe']}.\n"
            f"CORE PERSONALITY OBJECTIVE: {self.config['goal']}.\n"
            f"LITERAL PERSONALITY ANCHOR TAGS: {self.config['tags']}.\n\n"
            f"MISSION MANDATE:\n"
            f"Channel your defined operational personality. You must speak, think, filter, and extract "
            f"information entirely through the lens of this specific operational personality. Synthesize exactly "
            f"{count} distinct search vectors where every single line is an unadulterated expression of your "
            f"unique analytical personality, your tactical style, and your core cognitive vibe.\n\n"
            "STRICT CONSTRUCTION RULES DRIVEN BY PERSONALITY:\n"
            "1. NO ADVANCED PREFIXES: Your personality must execute cleanly. Absolutely NO field search prefixes or domain limits. These break API integrations.\n"
            "2. NO TEMPORAL MODIFIERS: Your personality focuses on raw data, not dates. Do NOT include relative time frames, calendar years, or explicit date boundaries. These are handled programmatically downstream.\n"
            "3. PERSONALITY ANCHOR LOCK: Ground your unique intelligence personality into raw, literal terms. Wrap the physical assets, factions, or targets from your personality objective in exact double quotes. Do not use metaphors, analogies, or abstract interpretations.\n"
            "4. LOGICAL COUPLING: Pair your quoted personality anchors with sharp, event-driven action terms or observational indicators using precise space separation.\n"
            "5. NOISE FILTERING: Let your operational personality strip away the fluff. Append strict exclusion operators to filter out commentary, opinions, editorials, and mainstream essays, leaving purely high-signal data.\n\n"
            "OUTPUT FORMAT MANDATE:\n"
            "Output ONLY the raw search strings, one per line. Do NOT include numbers, bullet points, markdown blocks, introductory phrasing, or conversational text. Your operational personality must be completely visible through the tactical composition of the search lines themselves."
        )

        current_client = next(self.client_cycle)
        
        try:
            # Lowered temperature to 0.15 to ensure the heavy personality bias doesn't cause formatting drift
            resp = await asyncio.to_thread(
                current_client.chat.completions.create,
                messages=[
                    {"role": "system", "content": f"You are {self.config['name']}. Your operational personality, worldview, and vibe define everything you output. You cannot break character."},
                    {"role": "user", "content": prompt}
                ],
                model="gpt-oss-120b",
                temperature=0.15,
                max_tokens=1000
            )
            
            if not resp or not hasattr(resp, 'choices') or not resp.choices:
                print("[!] Brain Synthesis Warning: API returned an empty choice payload.")
                return []
                
            choice = resp.choices[0]
            message = choice.message
            
            if hasattr(message, 'refusal') and message.refusal:
                print(f"[⚠️] Brain Synthesis Refused Objective: {message.refusal}")
                return []
                
            if not message or message.content is None:
                finish_reason = getattr(choice, 'finish_reason', 'unknown')
                print(f"[⚠️] Brain Synthesis Warning: content is None. Finish Reason: '{finish_reason}'")
                return []
                
            content = message.content.strip()
            if not content:
                return []
            
            vectors = []
            for line in content.split('\n'):
                line = line.strip()
                if not line:
                    continue
                
                # Defensive Extraction: Strip out any hallucinated numbering or bullet formatting
                line = re.sub(r'^\s*(?:\d+[\.\)]|[\-\*\•])\s*', '', line).strip()
                line = line.replace('`', '')
                
                if line.lower().startswith("vector") or line.startswith("#") or not line:
                    continue
                    
                vectors.append(line)
            
            print(f"[🧠] IntelBrain ({self.config['name']}): Synthesized {len(vectors)} clean, personality-locked vectors.")
            return vectors[:count]
            
        except Exception as e:
            print(f"[!] Brain Synthesis Error: {e}")
            return []

# --- STANDALONE TEST BLOCK ---
async def test_brain():
    brain = IntelBrain(profile_config=carrier_profile)
    print(f"\n[🧠] {brain.config['name']} ONLINE")
    print(f"[*] Keys Loaded: {len(brain.valid_keys)}")
    print("[*] Synthesizing Vectors...\n")
    
    vectors = await brain.generate_vectors(count=10)
    
    for i, vector in enumerate(vectors, 1):
        print(f"Vector {i:02}: {vector}")

if __name__ == "__main__":
    asyncio.run(test_brain())