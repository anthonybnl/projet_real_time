import asyncio
import time
from fonction_producer import producer_stream

async def simulate_outage(shared_state):
    # 1. Attendre 8 secondes en fonctionnement normal
    await asyncio.sleep(8)
    print("\n" + "="*60)
    print("[TEST-SIMULATION] >>> SIMULATION D'UNE PANNE COINBASE (arrêt des messages)... <<<")
    print("="*60 + "\n")
    shared_state["simulate_coinbase_error"] = True
    
    # 2. Laisser la panne pendant 10 secondes (le moniteur va détecter le silence après 3.0s et basculer sur Binance)
    await asyncio.sleep(10)
    print("\n" + "="*60)
    print("[TEST-SIMULATION] >>> RETABLISSEMENT DU FLUX COINBASE ! <<<")
    print("="*60 + "\n")
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
                topic_name="test",
                bootstrap_servers="localhost:9092",
                shared_state=shared_state
            ),
            simulate_outage(shared_state)
        )
    except asyncio.CancelledError:
        print("[TEST] Annulation du script de test.")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n[TEST] Arret demande par l'utilisateur.")
