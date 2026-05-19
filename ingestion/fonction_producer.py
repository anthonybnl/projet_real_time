import asyncio
import websockets
import json
import time
from datetime import datetime
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import KafkaError, TopicAlreadyExistsError

def create_topic_if_not_exists(
    num_partitions: int = 3,
    replication_factor: int = 1,
    bootstrap_servers: str = "localhost:9092",
    topic_name: list[str] = ['coinbase.btc.usd.trades', 'binance.btc.usd.trades'],
):
    """
    Crée un topic Kafka seulement s'il n'existe pas déjà
    
    Args:
        topic_name: Nom du topic à créer
        num_partitions: Nombre de partitions (défaut: 3)
        replication_factor: Facteur de réplication (défaut: 1)
        bootstrap_servers: Adresse du broker Kafka
    """
    admin_client = KafkaAdminClient(
        bootstrap_servers=bootstrap_servers,
        client_id='topic-creator'
    )
    
    try:
        # Récupérer la liste des topics existants
        existing_topics = admin_client.list_topics()
        
        for topic_name in topic_name:
            if topic_name in existing_topics:
                print(f"[OK] Le topic '{topic_name}' existe déjà")
                print(f"   Partitions: {num_partitions} (configuration demandée)")
                return False
            
            # Créer le topic
        topic = NewTopic(
            name=topic_name,
            num_partitions=num_partitions,
            replication_factor=replication_factor
        )
        
        admin_client.create_topics(new_topics=[topic], validate_only=False)
        print(f"[OK] Topic '{topic_name}' créé avec succès")
        print(f"   - Partitions: {num_partitions}")
        print(f"   - Replication factor: {replication_factor}")
        return True
        
    except TopicAlreadyExistsError:
        print(f"[OK] Le topic '{topic_name}' existe déjà (détecté lors de la création)")
        return False
        
    except Exception as e:
        print(f"[ERROR] Erreur lors de la création du topic: {e}")
        raise
        
    finally:
        admin_client.close()


async def coinbase_consumer(shared_state, producer, url, topic_name="coinbase.btc.usd.trades"):
    subscribe_msg = {
        "type": "subscribe",
        "product_ids": ["BTC-USD"],
        "channels": [
            "level2",
            "heartbeat",
            {
                "name": "ticker",
                "product_ids": ["BTC-USD"]
            }
        ]
    }
    
    while True:
        try:
            print(f"[Coinbase] Connexion au WebSocket: {url}")
            async with websockets.connect(url) as websocket:
                await websocket.send(json.dumps(subscribe_msg))
                print("[Coinbase] Abonnement envoye avec succes")
                
                # Reset last_coinbase_time to current time upon connection
                shared_state["last_coinbase_time"] = time.time()
                
                async for message in websocket:
                    if shared_state.get("simulate_coinbase_error", False):
                        # Simuler un silence radio complet
                        continue
                        
                    data = json.loads(message)
                    
                    # Mettre a jour l'horodatage pour prouver que Coinbase est en vie
                    shared_state["last_coinbase_time"] = time.time()
                    
                    if shared_state["active_source"] == "coinbase":
                        key_val = data.get('trade_id')
                        price_val = data.get('price')
                        message_type = data.get('type', 'unknown')
                        
                        if key_val is not None and price_val is not None:
                            producer.send(
                                topic=topic_name,
                                key=str(key_val).encode('utf-8'),
                                value=data
                            )
                            print(f"[Coinbase - ACTIVE] Message envoye au topic {topic_name} [{message_type}]: {price_val}")
        except websockets.ConnectionClosed:
            print("[Coinbase] Connexion fermee, tentative de reconnexion dans 5 secondes...")
        except Exception as e:
            print(f"[Coinbase - ERROR] {e}, tentative de reconnexion dans 5 secondes...")
        
        await asyncio.sleep(5)


async def binance_consumer(shared_state, producer, url,topic_name="binance.btc.usd.trades"):
    while True:
        try:
            print(f"[Binance] Connexion au WebSocket: {url}")
            async with websockets.connect(url) as websocket:
                print("[Binance] Connecte avec succes")
                
                async for message in websocket:
                    data = json.loads(message)
                    
                    if shared_state["active_source"] == "binance":
                        key_val = data.get('E')  # Event time
                        price_val = data.get('p')  # Price
                        message_type = data.get('e', 'unknown')  # Event type
                        
                        if key_val is not None and price_val is not None:
                            producer.send(
                                topic=topic_name,
                                key=str(key_val).encode('utf-8'),
                                value=data
                            )
                            print(f"[Binance - FALLBACK ACTIVE] Message envoye au topic {topic_name} [{message_type}]: {price_val}")
        except websockets.ConnectionClosed:
            print("[Binance] Connexion fermee, tentative de reconnexion dans 5 secondes...")
        except Exception as e:
            print(f"[Binance - ERROR] {e}, tentative de reconnexion dans 5 secondes...")
        
        await asyncio.sleep(5)


async def monitor_health(shared_state, timeout=3.0):
    print(f"[Monitor] Demarrage de la surveillance (timeout={timeout}s)")
    while True:
        now = time.time()
        elapsed = now - shared_state["last_coinbase_time"]
        
        if elapsed > timeout:
            if shared_state["active_source"] != "binance":
                print(f"[Monitor] !!! Coinbase inactif depuis {elapsed:.2f}s. Bascule sur Binance !!!")
                shared_state["active_source"] = "binance"
        else:
            if shared_state["active_source"] != "coinbase":
                print(f"[Monitor] *** Coinbase a repris apres {elapsed:.2f}s. Retour sur Coinbase ***")
                shared_state["active_source"] = "coinbase"
                
        await asyncio.sleep(0.5)


async def producer_stream(
    topic_name: list[str] = ['coinbase.btc.usd.trades', 'binance.btc.usd.trades'],
    url: str = None,
    bootstrap_servers: str = "localhost:9092",
    coinbase_url: str = "wss://ws-feed.exchange.coinbase.com",
    binance_url: str = "wss://stream.binance.com:9443/ws/btcusdt@trade",
    shared_state: dict = None
):
    create_topic_if_not_exists(bootstrap_servers=bootstrap_servers, topic_name=topic_name)

    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
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
            "active_source": "coinbase"
        }

    try:
        # Lancement simultané des trois tâches concourantes
        await asyncio.gather(
            coinbase_consumer(shared_state, producer, coinbase_url),
            binance_consumer(shared_state, producer, binance_url),
            monitor_health(shared_state, timeout=3.0)
        )
    except asyncio.CancelledError:
        print("Taches annulees")
    except Exception as e:
        print(f"Error: {e}")
    finally:
        producer.close()
        print("Kafka Producer closed")
