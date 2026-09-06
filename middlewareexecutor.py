import subprocess
import time
import sys
import logging
import os

logging.basicConfig(level=logging.INFO, format='%(asctime)s - [ORCHESTRATOR] - %(message)s')

def run_timeboxed_worker(script_name: str, duration_seconds: int):
    """Lets an infinite-loop script like the news engine harvest for a set period."""
    logging.info(f"🚀 INITIATING HARVEST PHASE: {script_name} for {duration_seconds}s")
    try:
        subprocess.run([sys.executable, script_name], timeout=duration_seconds, check=False)
    except subprocess.TimeoutExpired:
        logging.info(f"⏳ HARVEST WINDOW MET: {script_name} buffer filled. Shifting matrix.")
    except Exception as e:
        logging.error(f"⚠️ ERROR running {script_name}: {e}")

def run_single_shot_worker(script_name: str, task_label: str):
    """Runs a script to absolute completion without early time constraints."""
    logging.info(f"🚀 EXECUTING SINGLE-SHOT PHASE: {script_name} ({task_label})")
    start_time = time.time()
    try:
        # No time-out limit here; it runs until the script code naturally finishes its workload
        subprocess.run([sys.executable, script_name], check=True)
        logging.info(f"✅ PHASE COMPLETE: {script_name} finished smoothly in {time.time() - start_time:.2f}s.")
    except subprocess.CalledProcessError as e:
        logging.error(f"❌ CRASH: {script_name} returned exit code {e.returncode}.")
    except Exception as e:
        logging.error(f"⚠️ SYSTEM FAULT on {script_name}: {e}")

def master_pipeline_loop():
    cycle = 1
    while True:
        logging.info(f"\n==================================================")
        logging.info(f"🔄 EXECUTING GLOBAL PIPELINE CYCLE #{cycle}")
        logging.info(f"==================================================")
        
        # 1. Scraping Phase: Run for 15 minutes to pull the freshest operational feeds
        run_timeboxed_worker("news_engine_vs.py", 20)
        time.sleep(5) 
        
        # 2. Geospatial Phase: Run until ALL 36 layers finish updating exactly once
        run_single_shot_worker("GeographicalIntellgeneLayerUpdater.py", "All Geospatial Layers")
        time.sleep(5)
        
        # 3. Intelligence Phase: Process exactly ONE country via A-to-Z state memory
        run_single_shot_worker("CRMM.py", "Single-Target Country Dossier")
        time.sleep(5)
        
        logging.info(f"🏁 PIPELINE PASS #{cycle} SECURED. Revolving matrix...\n")
        cycle += 1

if __name__ == "__main__":
    # Maintain Hugging Face web container check requirement in isolated background fork
    subprocess.Popen([sys.executable, "-m", "http.server", "7860"])
    master_pipeline_loop()