import asyncio
import sys
import argparse
import logging
from fonction_producer import producer_stream

# Configurer la journalisation globale
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

def parse_arguments():
    parser = argparse.ArgumentParser(
        description="Orchestrateur Haute Disponibilite (Failover) pour Ingestion de Tickers BTC-USD"
    )
    parser.add_argument(
        "--topics",
        nargs="+",
        default=["coinbase.btc.usd.trades", "binance.btc.usd.trades"],
        help="Noms des topics Kafka a utiliser (Coinbase en 1er, Binance en 2eme)"
    )
    parser.add_argument(
        "--bootstrap-servers",
        default="localhost:9092",
        help="Adresse(s) du broker Kafka (defaut: localhost:9092)"
    )
    parser.add_argument(
        "--coinbase-url",
        default="wss://ws-feed.exchange.coinbase.com",
        help="URL WebSocket pour Coinbase (defaut: wss://ws-feed.exchange.coinbase.com)"
    )
    parser.add_argument(
        "--binance-url",
        default="wss://stream.binance.com:9443/ws/btcusdt@trade",
        help="URL WebSocket pour Binance (defaut: wss://stream.binance.com:9443/ws/btcusdt@trade)"
    )
    return parser.parse_args()

async def main():
    args = parse_arguments()
    
    logger.info("=" * 60)
    logger.info("  DEMARRAGE DE L'ORCHESTRATEUR DE FLUX HAUTE DISPONIBILITE")
    logger.info("=" * 60)
    logger.info(f"Kafka Broker   : {args.bootstrap_servers}")
    logger.info(f"Topics Kafka    : {args.topics}")
    logger.info(f"Flux Coinbase  : {args.coinbase_url}")
    logger.info(f"Flux Binance   : {args.binance_url}")
    logger.info("-" * 60)
    logger.info("Lancement des consommateurs en parallele...")
    
    try:
        await producer_stream(
            topic_name=args.topics,
            bootstrap_servers=args.bootstrap_servers,
            coinbase_url=args.coinbase_url,
            binance_url=args.binance_url
        )
    except asyncio.CancelledError:
        logger.info("Taches annulees par le systeme.")
    except Exception as e:
        logger.error(f"Erreur inattendue dans l'orchestrateur : {e}", exc_info=True)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Arret de l'orchestrateur demande par l'utilisateur. Fermeture propre...")
        sys.exit(0)
