"""
Test de la "solution B" : abonnement (change stream) async sur btc_trades.

Mecanique reactive (sans horloge) :
  - On s'abonne au flux d'inserts de btc_trades.
  - A CHAQUE message recu, on regarde depuis combien de temps remonte la derniere
    agregation lancee. Si c'est > AGG_MIN_INTERVAL, on relance l'agregation, sinon
    on ignore (throttle "leading edge"). Pas de message -> pas d'agregation.
  - L'agregation tourne dans une task asyncio separee : elle ne bloque pas la
    consommation du flux, et un verrou "in-flight" evite de les empiler.

Dans FastAPI, run_aggregation() se contentera de remplacer le print par un
manager.broadcast(...) vers les clients WebSocket.

Le change stream necessite un replica set Mongo (lecture de l'oplog).
"""

import asyncio
import os

from pymongo import AsyncMongoClient
from pymongo.errors import PyMongoError
from dotenv import load_dotenv

load_dotenv()

# Configuration MongoDB
MONGO_URI = os.environ["MONGODB_URI"]
MONGO_DB = os.environ["MONGODB_DBNAME"]
MONGO_COLLECTION = "btc_trades"

# Intervalle minimal entre deux lancements d'agregation (secondes), parametrable
AGG_MIN_INTERVAL = float(os.getenv("AGG_MIN_INTERVAL", "1.0"))

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


async def watch_trades(collection, state: dict):
    """
    S'abonne au flux d'inserts de btc_trades. A chaque nouveau document, incremente
    un compteur partage ; le ticker decidera quand relancer l'agregation.
    """
    print(f"Abonnement au change stream de {MONGO_COLLECTION}...")

    loop = asyncio.get_running_loop()
    # last_launch : horodatage monotone du dernier lancement d'agregation.
    # -inf => la premiere insertion declenche immediatement une agregation.
    state["last_launch"] = float("-inf")

    async with await collection.watch(
        [{"$match": {"operationType": "insert"}}]
    ) as stream:
        print("Abonnement actif. En attente de nouveaux trades...\n")
        async for _ in stream:
            state["pending"] += 1

            now = loop.time()
            elapsed = now - state["last_launch"]

            # Throttle reactif : on (re)lance seulement si X s se sont ecoulees
            # depuis le dernier lancement, et si aucune agregation n'est en cours.
            if elapsed >= AGG_MIN_INTERVAL and not state["running"]:
                state["last_launch"] = now
                # Task separee : la consommation du flux n'est pas bloquee par la requete.
                asyncio.create_task(run_aggregation(collection, state))


async def run_aggregation(collection, state: dict):
    """
    Lance la pipeline d'agregation et affiche le resultat.
    Dans FastAPI : remplacer le print par manager.broadcast(result).
    """
    new_docs = state["pending"]
    state["pending"] = 0
    state["running"] = True
    try:
        result = None
        async for doc in await collection.aggregate(PIPELINE):
            result = doc
            break

        if result is None:
            return

        print("\n=== Agregation (declenchee par "
              f"{new_docs} nouveau(x) trade(s)) ===")
        print(f"timestamp     : {result['timestamp']}")
        print(f"window_5min   : {result['window_5min']}")
        print(f"window_1hour  : {result['window_1hour']}\n")

    except PyMongoError as e:
        print(f"Erreur agregation: {e}")
    finally:
        state["running"] = False


async def main():
    client = AsyncMongoClient(MONGO_URI)
    await client.admin.command("ping")
    collection = client[MONGO_DB][MONGO_COLLECTION]
    print(f"Mongo connecte: db={MONGO_DB}, collection={MONGO_COLLECTION}")

    # Etat partage : compteur de docs depuis la derniere agregation + verrou in-flight.
    state = {"pending": 0, "running": False}

    try:
        await watch_trades(collection, state)
    finally:
        await client.close()
        print("Connexion fermee")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nArret demande.")