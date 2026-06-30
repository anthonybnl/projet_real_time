"""
Consumer 3: calcul analytics sur les données nettoyées de btc.cleaned
"""

import json
import os
import time
from collections import deque
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv
from kafka import KafkaConsumer
from kafka.errors import KafkaError
from pymongo import MongoClient

load_dotenv()

BOOTSTRAP_SERVERS = "localhost:9092"
INPUT_TOPIC = "btc.cleaned"
GROUP_ID = "btc-analytics-group"

MONGO_URI = os.environ["MONGODB_URI"]
MONGO_DB = os.environ["MONGODB_DBNAME"]
MONGO_COLLECTION = "btc_analytics"


class AnalyticsEngine:
    def __init__(self):
        self.window_5min = deque()

        self.trades_this_second = []
        self.last_emission = time.time()
        self.emission_interval = 1.0

        self.total_price_sum = 0.0
        self.total_trades_count = 0
        self.start_time = time.time()

    def add_trade(self, trade):
        """Ajoute un trade et nettoie la fenêtre 5min"""
        try:
            timestamp = datetime.fromisoformat(
                trade["timestamp"].replace("Z", "+00:00")
            )
        except:
            timestamp = datetime.now(timezone.utc)

        trade_data = (timestamp, trade)
        self.window_5min.append(trade_data)

        cutoff = timestamp - timedelta(minutes=5)
        while self.window_5min and self.window_5min[0][0] < cutoff:
            self.window_5min.popleft()

        self.trades_this_second.append(trade)

        self.total_price_sum += trade["price"]
        self.total_trades_count += 1

    def should_emit(self):
        """Vérifie si on doit émettre les métriques"""
        current_time = time.time()
        if current_time - self.last_emission >= self.emission_interval:
            if self.trades_this_second:
                return True
            self.last_emission = current_time
        return False

    def calculate_and_reset(self):
        """Calcule les métriques et reset les compteurs 1sec"""
        if not self.trades_this_second:
            return None

        trades_5min = [t for _, t in self.window_5min]

        volume_5min = sum(t["trade_size"] for t in trades_5min)
        avg_price_5min = (
            sum(t["price"] for t in trades_5min) / len(trades_5min)
            if trades_5min
            else 0
        )

        trades_per_second = len(self.trades_this_second)
        avg_price_1sec = sum(t["price"] for t in self.trades_this_second) / len(
            self.trades_this_second
        )

        avg_price_global = (
            self.total_price_sum / self.total_trades_count
            if self.total_trades_count > 0
            else 0
        )

        metrics = {
            "timestamp": datetime.now(timezone.utc),
            "window_5min": {
                "volume": round(volume_5min, 8),
                "avg_price": round(avg_price_5min, 2),
                "trades_count": len(trades_5min),
            },
            "window_1sec": {
                "trades_per_second": trades_per_second,
                "avg_price": round(avg_price_1sec, 2),
            },
            "global": {
                "avg_price_since_start": round(avg_price_global, 2),
                "total_trades": self.total_trades_count,
                "uptime_seconds": round(time.time() - self.start_time, 1),
            },
        }

        self.trades_this_second = []
        self.last_emission = time.time()

        return metrics


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


def create_mongo_client():
    client = MongoClient(MONGO_URI)
    db = client[MONGO_DB]
    collection = db[MONGO_COLLECTION]
    return collection


def main():
    consumer = None
    mongo_collection = None

    metrics_count = 0

    try:
        consumer = create_consumer()
        mongo_collection = create_mongo_client()
        engine = AnalyticsEngine()

        print(f"Consumer connecté: {INPUT_TOPIC}")
        print(f"MongoDB connecté: {MONGO_DB}.{MONGO_COLLECTION}")
        print("🔄 Calcul analytics en cours...\n")

        first = True

        for message in consumer:
            if first:
                assigned_partitions = consumer.assignment()
                print(
                    f"Partitions assignées: {[t.partition for t in assigned_partitions]}\n"
                )
                first = False

            engine.add_trade(message.value)

            if engine.should_emit():
                metrics = engine.calculate_and_reset()

                if metrics:
                    mongo_collection.insert_one(metrics)
                    metrics_count += 1

                    # if metrics_count % 10 == 0:
                    print(
                        f"{metrics_count}\t| "
                        f"Prix 1sec: ${metrics['window_1sec']['avg_price']:.2f} \t| "
                        f"Trades/sec: {metrics['window_1sec']['trades_per_second']} \t| "
                        f"Prix global: ${metrics['global']['avg_price_since_start']:.2f}"
                    )

    except KeyboardInterrupt:
        print(f"\n⚠️ Arrêt demandé. {metrics_count} métriques calculées.")

    except KafkaError as e:
        print(f"❌ Erreur Kafka: {e}")

    except Exception as e:
        print(f"❌ Erreur: {e}")
        import traceback

        traceback.print_exc()

    finally:
        if consumer:
            consumer.close()
        print("🔒 Connexions fermées")


if __name__ == "__main__":
    main()
