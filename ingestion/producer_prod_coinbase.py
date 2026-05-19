import asyncio
import websockets
import json
import time
from datetime import datetime
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import KafkaError, TopicAlreadyExistsError

def create_topic_if_not_exists(
    topic_name: str,
    num_partitions: int = 3,
    replication_factor: int = 1,
    bootstrap_servers: str = "localhost:9092"
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
        
        if topic_name in existing_topics:
            print(f"✅ Le topic '{topic_name}' existe déjà")
            print(f"   Partitions: {num_partitions} (configuration demandée)")
            return False
        
        # Créer le topic
        topic = NewTopic(
            name=topic_name,
            num_partitions=num_partitions,
            replication_factor=replication_factor
        )
        
        admin_client.create_topics(new_topics=[topic], validate_only=False)
        print(f"✅ Topic '{topic_name}' créé avec succès")
        print(f"   - Partitions: {num_partitions}")
        print(f"   - Replication factor: {replication_factor}")
        return True
        
    except TopicAlreadyExistsError:
        print(f"✅ Le topic '{topic_name}' existe déjà (détecté lors de la création)")
        return False
        
    except Exception as e:
        print(f"❌ Erreur lors de la création du topic: {e}")
        raise
        
    finally:
        admin_client.close()

async def coinbase_stream(topic_name: str, bootstrap_servers: str = "localhost:9092"):
    url = "wss://ws-feed.exchange.coinbase.com"
    
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    
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
    
    async with websockets.connect(url) as websocket:
        await websocket.send(json.dumps(subscribe_msg))
        print("Subscribed to Coinbase stream for BTC-USD")
        
        try:
            while True:
                message = await websocket.recv()
                data = json.loads(message)
                print(data.get('sequence'))
                if data.get('sequence') is not None and data.get('price') is not None:
                    # Send to Kafka
                    producer.send(
                        topic=topic_name,
                        key=str(data.get('sequence')).encode('utf-8'),
                        value=data
                        )
                
                # Print a small log to know it works
                message_type = data.get('type', 'unknown')
                print(f"Message envoyé au topic {topic_name}: {message_type}")
                
        except websockets.ConnectionClosed:
            print("Connection closed")
        except Exception as e:
            print(f"Error: {e}")
        finally:
            producer.close()

if __name__ == "__main__":
    import sys
    
    # Paramètres par défaut
    topic_name = "coinbase.btc.usd.trades"
    num_partitions = 3
    replication_factor = 1
    bootstrap_servers = "localhost:9092"
    
    # Possibilité de passer le nom du topic en argument
    if len(sys.argv) > 1:
        topic_name = sys.argv[1]
    if len(sys.argv) > 2:
        num_partitions = int(sys.argv[2])
    
    print(f"🔧 Création du topic Kafka...")
    print(f"   Topic: {topic_name}")
    print(f"   Partitions: {num_partitions}")
    print(f"   Replication: {replication_factor}")
    print()
    
    create_topic_if_not_exists(
        topic_name=topic_name,
        num_partitions=num_partitions,
        replication_factor=replication_factor,
        bootstrap_servers=bootstrap_servers
    )

    asyncio.run(coinbase_stream(topic_name=topic_name, bootstrap_servers=bootstrap_servers))
