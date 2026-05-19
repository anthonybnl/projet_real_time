import asyncio
import time
import sys
import logging
from fonction_producer import producer_stream

# Configurer la journalisation globale pour le test
log_format = '%(asctime)s [%(levelname)s] %(message)s'
logging.basicConfig(
    level=logging.INFO,
    format=log_format,
    handlers=[
        logging.FileHandler("ingestion.log", encoding="utf-8"),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger("ingestion")

async def simulate_outage(shared_state):
    # 1. Attendre 8 secondes en fonctionnement normal
    await asyncio.sleep(8)
    logger.info("="*60)
    logger.warning("[TEST-SIMULATION] >>> SIMULATION D'UNE PANNE COINBASE (arrêt des messages)... <<<")
    logger.info("="*60)
    shared_state["simulate_coinbase_error"] = True
    
    # 2. Laisser la panne pendant 10 secondes (le moniteur va détecter le silence après 2.0s et basculer sur Binance)
    await asyncio.sleep(10)
    logger.info("="*60)
    logger.info("[TEST-SIMULATION] >>> RETABLISSEMENT DU FLUX COINBASE ! <<<")
    logger.info("="*60)
    shared_state["simulate_coinbase_error"] = False

async def main():
    shared_state = {
        "last_coinbase_time": time.time(),
        "active_source": "coinbase",
        "simulate_coinbase_error": False
    }
    
    # Executer le flux du producer et la simulation de panne en parallele
    try:
        await asyncio.gather(
            producer_stream(
                topic_name=['coinbase.btc.usd.trades', 'binance.btc.usd.trades'],
                bootstrap_servers="localhost:9092",
                shared_state=shared_state
            ),
            simulate_outage(shared_state)
        )
    except asyncio.CancelledError:
        logger.info("[TEST] Annulation du script de test.")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("[TEST] Arret demande par l'utilisateur.")
