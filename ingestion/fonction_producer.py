import asyncio
import websockets
import json
import time
import logging
from datetime import datetime
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import KafkaError, TopicAlreadyExistsError

logger = logging.getLogger("ingestion")


def create_topic_if_not_exists(
    num_partitions: int = 3,
    replication_factor: int = 1,
    bootstrap_servers: str = "localhost:9092",
    topic_name: list[str] = ["coinbase.btc.usd.trades", "binance.btc.usd.trades"],
):
    """
    Crée des topics Kafka seulement s'ils n'existent pas déjà

    Args:
        num_partitions: Nombre de partitions (défaut: 3)
        replication_factor: Facteur de réplication (défaut: 1)
        bootstrap_servers: Adresse du broker Kafka
        topic_name: Liste des noms des topics à créer
    """
    admin_client = KafkaAdminClient(
        bootstrap_servers=bootstrap_servers, client_id="topic-creator"
    )

    try:
        # Récupérer la liste des topics existants
        existing_topics = admin_client.list_topics()

        new_topics_to_create = []
        for name in topic_name:
            if name in existing_topics:
                logger.info(
                    f"Le topic '{name}' existe déjà (Partitions: {num_partitions})"
                )
            else:
                new_topics_to_create.append(
                    NewTopic(
                        name=name,
                        num_partitions=num_partitions,
                        replication_factor=replication_factor,
                    )
                )

        if new_topics_to_create:
            admin_client.create_topics(
                new_topics=new_topics_to_create, validate_only=False
            )
            for t in new_topics_to_create:
                logger.info(
                    f"Topic '{t.name}' créé avec succès (Partitions: {num_partitions}, Replication factor: {replication_factor})"
                )
            return True
        return False

    except TopicAlreadyExistsError:
        logger.info("Un des topics existe déjà (détecté lors de la création)")
        return False

    except Exception as e:
        logger.error(f"Erreur lors de la création du topic: {e}", exc_info=True)
        raise

    finally:
        admin_client.close()


async def coinbase_consumer(
    shared_state, producer, url, topic_name="coinbase.btc.usd.trades"
):
    subscribe_msg = {
        "type": "subscribe",
        "product_ids": ["BTC-USD"],
        "channels": [
            "level2",
            "heartbeat",
            {"name": "ticker", "product_ids": ["BTC-USD"]},
        ],
    }

    while True:
        try:
            logger.info(f"[Coinbase] Connexion au WebSocket: {url}")
            async with websockets.connect(url) as websocket:
                await websocket.send(json.dumps(subscribe_msg))
                logger.info("[Coinbase] Abonnement envoyé avec succès")

                # Reset last_coinbase_time to current time upon connection
                shared_state["last_coinbase_time"] = time.time()

                async for message in websocket:
                    if shared_state.get("simulate_coinbase_error", False):
                        # Simuler un silence radio complet
                        continue

                    data = json.loads(message)

                    # Mettre à jour l'horodatage pour prouver que Coinbase est en vie
                    shared_state["last_coinbase_time"] = time.time()

                    if shared_state["active_source"] == "coinbase":
                        key_val = data.get("trade_id")
                        price_val = data.get("price")

                        if key_val is not None and price_val is not None:
                            producer.send(
                                topic=topic_name,
                                key=str(key_val).encode("utf-8"),
                                value=data,
                            )
        except websockets.ConnectionClosed:
            logger.warning(
                "[Coinbase] Connexion fermée, tentative de reconnexion dans 5 secondes..."
            )
        except Exception as e:
            logger.error(f"[Coinbase] Erreur: {e}", exc_info=True)

        await asyncio.sleep(5)


async def binance_consumer(
    shared_state, producer, url, topic_name="binance.btc.usd.trades"
):
    while True:
        try:
            logger.info(f"[Binance] Connexion au WebSocket: {url}")
            async with websockets.connect(url) as websocket:
                logger.info("[Binance] Connecté avec succès")

                async for message in websocket:
                    data = json.loads(message)

                    if shared_state["active_source"] == "binance":
                        key_val = data.get("E")  # Event time
                        price_val = data.get("p")  # Price

                        if key_val is not None and price_val is not None:
                            producer.send(
                                topic=topic_name,
                                key=str(key_val).encode("utf-8"),
                                value=data,
                            )
        except websockets.ConnectionClosed:
            logger.warning(
                "[Binance] Connexion fermée, tentative de reconnexion dans 5 secondes..."
            )
        except Exception as e:
            logger.error(f"[Binance] Erreur: {e}", exc_info=True)

        await asyncio.sleep(5)


async def monitor_health(shared_state, timeout=3.0):
    logger.info(f"[Monitor] Démarrage de la surveillance (timeout={timeout}s)")
    while True:
        now = time.time()
        elapsed = now - shared_state["last_coinbase_time"]

        if elapsed > timeout:
            if shared_state["active_source"] != "binance":
                logger.warning(
                    f"[Monitor] !!! Coinbase inactif depuis {elapsed:.2f}s. Bascule sur Binance !!!"
                )
                shared_state["active_source"] = "binance"
        else:
            if shared_state["active_source"] != "coinbase":
                logger.info(
                    f"[Monitor] *** Coinbase a repris après {elapsed:.2f}s. Retour sur Coinbase ***"
                )
                shared_state["active_source"] = "coinbase"

        await asyncio.sleep(0.5)


async def producer_stream(
    topic_name: list[str] = ["coinbase.btc.usd.trades", "binance.btc.usd.trades"],
    url: str = None,
    bootstrap_servers: str = "localhost:9092",
    coinbase_url: str = "wss://ws-feed.exchange.coinbase.com",
    binance_url: str = "wss://stream.binance.com:9443/ws/btcusdt@trade",
    shared_state: dict = None,
):
    create_topic_if_not_exists(
        bootstrap_servers=bootstrap_servers, topic_name=topic_name
    )

    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    # Si un url custom est passé en paramètre de compatibilité
    if url is not None:
        if "coinbase" in url:
            coinbase_url = url
        elif "binance" in url:
            binance_url = url

    if shared_state is None:
        shared_state = {
            "last_coinbase_time": time.time(),
            "active_source": "coinbase",
            "simulate_coinbase_error": False,
        }

    try:
        # Lancement simultané des trois tâches concourantes
        await asyncio.gather(
            coinbase_consumer(shared_state, producer, coinbase_url),
            binance_consumer(shared_state, producer, binance_url),
            monitor_health(shared_state, timeout=2.0),
        )
    except asyncio.CancelledError:
        logger.info("Tâches annulées par l'utilisateur")
    except Exception as e:
        logger.error(f"Erreur générale dans producer_stream: {e}", exc_info=True)
    finally:
        producer.close()
        logger.info("Kafka Producer fermé proprement")
