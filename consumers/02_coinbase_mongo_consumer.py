"""
Consumer 2: Ingestion des donnees BTC nettoyees dans MongoDB
Lit depuis btc.cleaned et insere en lot dans MongoDB toutes les 1s
"""

import json
import time
from kafka import KafkaConsumer
from kafka.errors import KafkaError
from pymongo import MongoClient, ASCENDING
from pymongo.errors import PyMongoError

# Configuration Kafka
BOOTSTRAP_SERVERS = "localhost:9092"
INPUT_TOPIC = "btc.cleaned"
GROUP_ID = "btc-mongo-consumer-group"

# Configuration MongoDB
MONGO_URI = "mongodb://root:example@localhost:27017/?directConnection=true"
MONGO_DB = "crypto_realtime"
MONGO_COLLECTION = "btc_trades"

# Configuration du buffer
FLUSH_INTERVAL_SECONDS = 1.0
MAX_BUFFER_SIZE = 500  # Garde-fou si un burst arrive


def create_mongo_client():
    """Connexion a MongoDB et retour de la collection cible"""
    client = MongoClient(MONGO_URI)
    # Force la connexion pour detecter une erreur tout de suite
    client.admin.command("ping")
    db = client[MONGO_DB]
    collection = db[MONGO_COLLECTION]

    # Index sur timestamp pour les requetes par fenetre temporelle
    # Index sur id en unique pour eviter les doublons si Kafka rejoue des messages
    collection.create_index([("timestamp", ASCENDING)])
    collection.create_index([("id", ASCENDING)], unique=True)

    print(f"Mongo connecte: db={MONGO_DB}, collection={MONGO_COLLECTION}")
    return client, collection


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
        # Le poll a un timeout pour qu'on puisse flusher meme sans nouveau message
        consumer_timeout_ms=-1,
    )


def flush_buffer(buffer, collection):
    """Insere le buffer dans Mongo et le vide. Retourne le nombre insere."""
    if not buffer:
        return 0

    try:
        # ordered=False : si un doc echoue (ex: doublon), les autres passent quand meme
        result = collection.insert_many(buffer, ordered=False)
        inserted = len(result.inserted_ids)
        print(f"Insert Mongo: {inserted} documents")
        return inserted

    except PyMongoError as e:
        # Erreur typique: doublon sur l'index unique. On log et on continue.
        # Le write_errors contient le detail mais les autres docs sont inseres.
        print(f"Avertissement Mongo (certains docs non inseres): {e}")
        return 0

    finally:
        buffer.clear()


def main():
    consumer = None
    mongo_client = None
    buffer = []
    total_inserted = 0
    last_flush = time.time()

    try:
        mongo_client, collection = create_mongo_client()
        consumer = create_consumer()

        print(f"Consumer connecte: {INPUT_TOPIC}")
        print(f"Flush toutes les {FLUSH_INTERVAL_SECONDS}s ou {MAX_BUFFER_SIZE} messages")
        print("Traitement en cours...\n")

        for message in consumer:
            buffer.append(message.value)

            # Conditions de flush: temps ecoule OU buffer plein
            elapsed = time.time() - last_flush
            if elapsed >= FLUSH_INTERVAL_SECONDS or len(buffer) >= MAX_BUFFER_SIZE:
                inserted = flush_buffer(buffer, collection)
                total_inserted += inserted
                last_flush = time.time()

    except KeyboardInterrupt:
        print(f"\nArret demande. Flush final du buffer ({len(buffer)} messages)...")
        if buffer and mongo_client:
            inserted = flush_buffer(buffer, collection)
            total_inserted += inserted
        print(f"Total insere: {total_inserted} documents")

    except KafkaError as e:
        print(f"Erreur Kafka: {e}")

    except Exception as e:
        print(f"Erreur: {e}")

    finally:
        if consumer:
            consumer.close()
        if mongo_client:
            mongo_client.close()
        print("Connexions fermees")


if __name__ == "__main__":
    main()
