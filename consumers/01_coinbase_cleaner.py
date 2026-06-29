"""
Consumer 1: Nettoyage et extraction des données BTC Coinbase
Lit depuis coinbase.btc.usd et produit vers btc.cleaned
"""

import json
from datetime import datetime
from kafka import KafkaConsumer, KafkaProducer, KafkaAdminClient
from kafka.admin import NewTopic
from kafka.errors import KafkaError, TopicAlreadyExistsError

BOOTSTRAP_SERVERS = "localhost:9092"
INPUT_TOPIC = "coinbase.btc.usd.trades"
OUTPUT_TOPIC = "btc.cleaned"
GROUP_ID = "btc-cleaner-group"


def create_topic_if_not_exists(
    topic_name: str,
    num_partitions: int = 3,
    replication_factor: int = 1,
    bootstrap_servers: str = BOOTSTRAP_SERVERS,
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
        bootstrap_servers=bootstrap_servers, client_id="topic-creator"
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
            replication_factor=replication_factor,
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


def handle_message(data, producer: KafkaProducer):
    """Extrait et nettoie les données du ticker Coinbase"""

    cleaned = None

    # on extrait les informations qui nous intéressent
    try:
        price = float(data.get("price", 0))
        trade_size = float(data.get("last_size", 0))

        cleaned = {
            "timestamp": data.get("time"),
            "price": price,
            "trade_size": trade_size,
            "side": data.get("side"),
            "id": data.get("trade_id"),
            "product_id": data.get("product_id", "BTC-USD"),
        }

    except (ValueError, TypeError, KeyError) as e:
        print(f"Erreur parsing message: {e}")
        cleaned = None

    if cleaned and cleaned["id"] is not None and cleaned["price"] > 0:
        producer.send(OUTPUT_TOPIC, key=f"btc-{cleaned['id']}", value=cleaned)


def create_consumer():
    return KafkaConsumer(
        INPUT_TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id=GROUP_ID,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        auto_commit_interval_ms=1000,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        key_deserializer=lambda k: k.decode("utf-8") if k else None,
    )


def create_producer():
    return KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        acks="all",
        retries=3,
    )


def main():
    create_topic_if_not_exists(
        OUTPUT_TOPIC,
        num_partitions=3,
        replication_factor=1,
        bootstrap_servers=BOOTSTRAP_SERVERS,
    )

    consumer = None
    producer = None

    message_count = 0
    try:
        consumer = create_consumer()
        producer = create_producer()

        print(f"✅ Consumer connecté: {INPUT_TOPIC}")
        print(f"✅ Producer connecté: {OUTPUT_TOPIC}")
        print("🔄 Traitement en cours...\n")

        first = True

        for message in consumer:

            if first:
                # on affiche les partitions assignées pour debug
                assigned_partitions = consumer.assignment()
                print(
                    f"Partitions assignées: {[t.partition for t in assigned_partitions]}\n"
                )
                first = False

            print(
                f"Message reçu | Offset: {message.offset} | Timestamp: {message.timestamp} | Partition: {message.partition}"
            )
            handle_message(message.value, producer)
            message_count += 1

    except KeyboardInterrupt:
        print(f"\n⚠️ Arrêt demandé. {message_count} messages traités.")

    except KafkaError as e:
        print(f"❌ Erreur Kafka: {e}")

    except Exception as e:
        print(f"❌ Erreur: {e}")

    finally:
        if producer:
            producer.flush()
            producer.close()
        if consumer:
            consumer.close()
        print("🔒 Connexions fermées")


if __name__ == "__main__":
    main()
