import json
import os
import time
from datetime import datetime
from kafka import KafkaConsumer
from kafka.errors import KafkaError
from pymongo import MongoClient, ASCENDING
from pymongo.errors import PyMongoError
from dotenv import load_dotenv

load_dotenv()

# Configuration MongoDB
MONGO_URI = os.environ["MONGODB_URI"]
MONGO_DB = os.environ["MONGODB_DBNAME"]
MONGO_COLLECTION = "btc_trades"


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

PIPELINE = [
    # 1) Pre-filtre sur la fenetre la plus large (1h) : on evite de scanner toute la collection
    {
        "$match": {
            "$expr": {
                "$gte": [
                    "$timestamp",
                    {"$dateSubtract": {"startDate": "$$NOW", "unit": "hour", "amount": 1}},
                ]
            }
        }
    },
    # 2) Deux sous-agregations en parallele sur le meme jeu de docs
    {
        "$facet": {
            "window_5min": [
                {
                    "$match": {
                        "$expr": {
                            "$gte": [
                                "$timestamp",
                                {"$dateSubtract": {"startDate": "$$NOW", "unit": "minute", "amount": 5}},
                            ]
                        }
                    }
                },
                {
                    "$group": {
                        "_id": None,
                        "volume": {"$sum": "$trade_size"},
                        "avg_price": {"$avg": "$price"},
                        "trades_count": {"$sum": 1},
                    }
                },
                {
                    "$project": {
                        "_id": 0,
                        "volume": {"$round": ["$volume", 8]},
                        "avg_price": {"$round": ["$avg_price", 2]},
                        "trades_count": 1,
                    }
                },
            ],
            "window_1hour": [
                {
                    "$group": {
                        "_id": None,
                        "volume": {"$sum": "$trade_size"},
                        "avg_price": {"$avg": "$price"},
                        "trades_count": {"$sum": 1},
                    }
                },
                {
                    "$project": {
                        "_id": 0,
                        "volume": {"$round": ["$volume", 8]},
                        "avg_price": {"$round": ["$avg_price", 2]},
                        "trades_count": 1,
                    }
                },
            ],
        }
    },
    # 3) Mise en forme : timestamp d'execution + extraction des objets des tableaux $facet
    {
        "$project": {
            "timestamp": "$$NOW",
            "window_5min": {
                "$ifNull": [
                    {"$arrayElemAt": ["$window_5min", 0]},
                    {"volume": 0, "avg_price": None, "trades_count": 0},
                ]
            },
            "window_1hour": {
                "$ifNull": [
                    {"$arrayElemAt": ["$window_1hour", 0]},
                    {"volume": 0, "avg_price": None, "trades_count": 0},
                ]
            },
        }
    },
]


def main():
    mongo_client, collection = create_mongo_client()

    try:
        result = next(collection.aggregate(PIPELINE), None)

        if result is None:
            print("Aucun resultat (collection vide ?)")
            return

        print("\n=== Resultat agregation ===")
        print(f"timestamp     : {result['timestamp']}")
        print(f"window_5min   : {result['window_5min']}")
        print(f"window_1hour  : {result['window_1hour']}")

    except PyMongoError as e:
        print(f"Erreur agregation: {e}")

    finally:
        mongo_client.close()
        print("Connexion fermee")


if __name__ == "__main__":
    main()