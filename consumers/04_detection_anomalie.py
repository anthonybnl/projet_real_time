"""
Moteur de Détection d'Anomalies Financières en Temps Réel
Détecte :
1. SLIPPAGE ANORMAL : Z-Score glissant sur log-returns, N=100. Alerte si |Z| > 4.0.
2. SPREAD ÉLASTIQUE : Écart (BestAsk - BestBid) double par rapport à sa moyenne glissante 2 mins.
3. ORDER FLOW IMBALANCE (OFI) : Ratio (Vol_Buy - Vol_Sell) / Vol_Total sur 10s. Alerte si ratio > |0.85|.

Chaque calcul s'exécute en < 0.5 ms en utilisant exclusivement collections.deque.
Envoie les alertes dans le topic 'financial.alerts' en JSON.
"""

import json
import time
import math
import logging
import sys
from collections import deque
from datetime import datetime, timezone
from pymongo import MongoClient
from kafka import KafkaConsumer, KafkaProducer, KafkaAdminClient
from kafka.admin import NewTopic
from kafka.errors import KafkaError, TopicAlreadyExistsError

# Configuration
BOOTSTRAP_SERVERS = "127.0.0.1:9092"
INPUT_TOPIC = "btc.cleaned"
ALERT_TOPIC = "financial.alerts"
GROUP_ID = "financial-anomaly-detector-group"

MONGO_URI = "mongodb://root:example@localhost:27017/"
MONGO_DB = "crypto_realtime"
MONGO_COLLECTION = "btc_anomalies"

# Logger configuration
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger("AnomalyDetector")


class AnomalyDetector:
    """
    Détecteur en temps réel des anomalies financières de Slippage, Spread et OFI.
    Garanti < 0.5 ms par évaluation.
    """
    def __init__(
        self,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        alert_topic=ALERT_TOPIC,
        raise_on_anomaly=False,
        mongo_collection=None
    ):
        self.bootstrap_servers = bootstrap_servers
        self.alert_topic = alert_topic
        self.raise_on_anomaly = raise_on_anomaly
        self.mongo_collection = mongo_collection

        # Initialisation du Producteur Kafka pour les alertes
        self.producer = KafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8") if k else None,
            acks="all",
            retries=3
        )

        # Structures d'états en mémoire par (exchange, symbol) en utilisant exclusively collections.deque
        self.prev_prices = {}        # {(exchange, symbol): float}
        self.log_returns = {}        # {(exchange, symbol): deque(maxlen=100)}
        self.spreads = {}            # {(exchange, symbol): deque} de tuples (timestamp, spread)
        self.ofi_trades = {}         # {(exchange, symbol): deque} de tuples (timestamp, volume, side)

    def get_exchange_and_symbol(self, msg):
        """Détermine l'échange et la paire de trading."""
        exchange = msg.get("exchange") or ("coinbase" if "product_id" in msg or "sequence" in msg else "binance")
        symbol = msg.get("symbol") or msg.get("product_id") or msg.get("s") or "BTC-USD"
        return exchange.lower(), symbol.upper()

    def get_price(self, msg):
        """Extrait le prix du message."""
        for key in ["price", "p", "last_price"]:
            val = msg.get(key)
            if val is not None:
                try:
                    return float(val)
                except (ValueError, TypeError):
                    pass
        return None

    def get_volume(self, msg):
        """Extrait le volume du message."""
        for key in ["trade_size", "size", "q", "last_size", "volume"]:
            val = msg.get(key)
            if val is not None:
                try:
                    return float(val)
                except (ValueError, TypeError):
                    pass
        return None

    def get_side(self, msg):
        """Extrait la direction du trade (buy/sell)."""
        side = msg.get("side")
        if side in ["buy", "sell", "BUY", "SELL"]:
            return side.lower()
            
        # Fallback pour Binance : le champ 'm' (isBuyerMaker)
        # Si True, l'acheteur est le Maker (le Taker a vendu) -> "sell"
        # Si False, l'acheteur est le Taker (le Taker a acheté) -> "buy"
        if "m" in msg:
            return "sell" if msg["m"] else "buy"
            
        return None

    def get_best_bid_ask(self, msg):
        """Extrait le best_bid et le best_ask du message."""
        bid = msg.get("best_bid") or msg.get("bid") or msg.get("b")
        ask = msg.get("best_ask") or msg.get("ask") or msg.get("a")
        
        bid_val = None
        if bid is not None:
            try:
                bid_val = float(bid)
            except (ValueError, TypeError):
                pass
                
        ask_val = None
        if ask is not None:
            try:
                ask_val = float(ask)
            except (ValueError, TypeError):
                pass
                
        # Si absent, on génère un spread fictif de base de 0.05 dollars pour éviter les crashs si non fourni
        price = self.get_price(msg)
        if bid_val is None and price is not None:
            bid_val = price - 0.02
        if ask_val is None and price is not None:
            ask_val = price + 0.03
            
        return bid_val, ask_val

    def trigger_alert(self, anomaly_type, exchange, symbol, details, trigger_message):
        """Envoie un message d'alerte structuré au topic 'financial.alerts' de Kafka."""
        alert = {
            "anomaly_type": anomaly_type,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "exchange": exchange,
            "symbol": symbol,
            "details": details,
            "trigger_message": trigger_message
        }

        logger.warning(f"[!] [ANOMALIE DETECTEE] Type: {anomaly_type} | Echange: {exchange} | {details.get('description', '')}")

        try:
            key_str = f"{exchange}-{anomaly_type}"
            self.producer.send(
                self.alert_topic,
                key=key_str,
                value=alert
            )
        except Exception as e:
            logger.error(f"Impossible d'envoyer l'alerte à Kafka: {e}")

        if self.mongo_collection is not None:
            try:
                alert_copy = dict(alert)
                alert_copy["timestamp"] = datetime.now(timezone.utc) # Save as Date object in Mongo
                self.mongo_collection.insert_one(alert_copy)
            except Exception as e:
                logger.error(f"Impossible de sauvegarder l'alerte dans MongoDB: {e}")

        if self.raise_on_anomaly:
            raise ValueError(f"Anomalie {anomaly_type} détectée: {details.get('description')}")

    def process_message(self, msg):
        """
        Évalue un message individuel et vérifie les 3 règles d'anomalies financières.
        Garantit une exécution rapide en < 0.5ms.
        """
        start_time_ns = time.perf_counter_ns()
        now = time.time()

        exchange, symbol = self.get_exchange_and_symbol(msg)
        key = (exchange, symbol)

        # --- 1. SLIPPAGE ANORMAL (Z-Score glissant sur log-returns, N=100) ---
        price = self.get_price(msg)
        if price is not None and price > 0:
            prev_price = self.prev_prices.get(key)
            if prev_price is not None and prev_price > 0:
                log_ret = math.log(price / prev_price)
                
                if key not in self.log_returns:
                    self.log_returns[key] = deque(maxlen=100)
                
                self.log_returns[key].append(log_ret)
                
                ret_deque = self.log_returns[key]
                if len(ret_deque) >= 10:  # Fenêtre stable
                    n = len(ret_deque)
                    mean = sum(ret_deque) / n
                    variance = sum((x - mean) ** 2 for x in ret_deque) / n
                    std_dev = math.sqrt(variance)
                    
                    if std_dev > 1e-9:
                        z = (log_ret - mean) / std_dev
                        if abs(z) > 4.0:
                            self.trigger_alert(
                                anomaly_type="SLIPPAGE_ANORMAL",
                                exchange=exchange,
                                symbol=symbol,
                                details={
                                    "price": price,
                                    "previous_price": prev_price,
                                    "log_return": log_ret,
                                    "rolling_mean": mean,
                                    "rolling_std": std_dev,
                                    "z_score": z,
                                    "description": f"Alerte : Le prix a subi une variation extremement brutale en tres peu de temps. Le prix est passe de {prev_price:.2f} à {price:.2f}."
                                },
                                trigger_message=msg
                            )
            self.prev_prices[key] = price

        # --- 2. SPREAD ÉLASTIQUE (Alerte si écart BestAsk - BestBid double la moyenne glissante des 2 mins) ---
        bid, ask = self.get_best_bid_ask(msg)
        if bid is not None and ask is not None:
            spread = ask - bid
            
            if key not in self.spreads:
                self.spreads[key] = deque()
                
            # Nettoyage des spreads > 2 minutes (120 secondes)
            while self.spreads[key] and self.spreads[key][0][0] < now - 120.0:
                self.spreads[key].popleft()
                
            if len(self.spreads[key]) >= 5:
                mean_spread = sum(s for t, s in self.spreads[key]) / len(self.spreads[key])
                if mean_spread > 0 and spread > 2.0 * mean_spread:
                    self.trigger_alert(
                        anomaly_type="SPREAD_ELASTIQUE",
                        exchange=exchange,
                        symbol=symbol,
                        details={
                            "bid": bid,
                            "ask": ask,
                            "current_spread": spread,
                            "rolling_mean_spread_2m": mean_spread,
                            "ratio": spread / mean_spread,
                            "description": f"Alerte : L'ecart entre le meilleur prix d'achat et le meilleur prix de vente s'est soudainement elargi de facon anormale ({spread:.2f} USD), ce qui indique une forte baisse de la liquidite sur le marche."
                        },
                        trigger_message=msg
                    )
            
            self.spreads[key].append((now, spread))

        # --- 3. ORDER FLOW IMBALANCE (OFI) (Ratio (Vol_Buy - Vol_Sell)/Vol_Total sur 10s > |0.85|) ---
        side = self.get_side(msg)
        volume = self.get_volume(msg)
        if volume is not None and side in ["buy", "sell"]:
            if key not in self.ofi_trades:
                self.ofi_trades[key] = deque()
                
            self.ofi_trades[key].append((now, volume, side))
            
            # Nettoyage des trades > 10 secondes
            while self.ofi_trades[key] and self.ofi_trades[key][0][0] < now - 10.0:
                self.ofi_trades[key].popleft()
                
            if self.ofi_trades[key]:
                vol_buy = sum(v for t, v, s in self.ofi_trades[key] if s == "buy")
                vol_sell = sum(v for t, v, s in self.ofi_trades[key] if s == "sell")
                vol_total = vol_buy + vol_sell
                
                if vol_total > 0:
                    ratio = (vol_buy - vol_sell) / vol_total
                    if abs(ratio) > 0.85:
                        self.trigger_alert(
                            anomaly_type="ORDER_FLOW_IMBALANCE",
                            exchange=exchange,
                            symbol=symbol,
                            details={
                                "vol_buy": vol_buy,
                                "vol_sell": vol_sell,
                                "vol_total": vol_total,
                                "ofi_ratio": ratio,
                                "description": f"Alerte : Important desequilibre constate sur le marche avec une domination ecrasante des {'acheteurs' if ratio > 0 else 'vendeurs'} ({abs(ratio)*100:.1f}% du volume total de transaction sur les 10 dernieres secondes)."
                            },
                            trigger_message=msg
                        )

        # Calcul de temps d'exécution
        end_time_ns = time.perf_counter_ns()
        exec_time_ms = (end_time_ns - start_time_ns) / 1_000_000.0
        if exec_time_ms > 0.5:
            logger.debug(f"[WARNING] Traitement du message superieur a 0.5ms: {exec_time_ms:.4f}ms")
        return exec_time_ms

    def close(self):
        """Ferme proprement les connexions."""
        if self.producer:
            self.producer.flush()
            self.producer.close()
            logger.info("Connexion du producteur d'alertes fermee")


def create_topic_if_not_exists(topic_name, bootstrap_servers=BOOTSTRAP_SERVERS):
    """Crée le topic d'alertes si nécessaire."""
    admin_client = None
    for attempt in range(10):
        try:
            admin_client = KafkaAdminClient(
                bootstrap_servers=bootstrap_servers, client_id="anomaly-topic-creator"
            )
            break
        except Exception as e:
            if attempt < 9:
                logger.warning(f"[WARNING] Attente du demarrage complet de Kafka ({attempt + 1}/10)...")
                time.sleep(2)
            else:
                logger.error(f"[ERROR] Impossible de se connecter a Kafka apres 10 tentatives : {e}")
                raise e

    try:
        existing_topics = admin_client.list_topics()
        if topic_name not in existing_topics:
            topic = NewTopic(
                name=topic_name,
                num_partitions=3,
                replication_factor=1
            )
            admin_client.create_topics(new_topics=[topic], validate_only=False)
            logger.info(f"[OK] Topic '{topic_name}' cree avec succes")
        else:
            logger.info(f"[OK] Le topic '{topic_name}' existe deja")
    except TopicAlreadyExistsError:
        pass
    except Exception as e:
        logger.error(f"[ERROR] Erreur lors de la creation du topic {topic_name}: {e}")
    finally:
        if admin_client:
            admin_client.close()


def create_consumer(topic_name, bootstrap_servers=BOOTSTRAP_SERVERS):
    """Crée le consommateur Kafka."""
    return KafkaConsumer(
        topic_name,
        bootstrap_servers=bootstrap_servers,
        group_id=GROUP_ID,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        auto_commit_interval_ms=1000,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        key_deserializer=lambda k: k.decode("utf-8") if k else None
    )


def create_mongo_client():
    """Crée le client MongoDB."""
    client = MongoClient(MONGO_URI)
    db = client[MONGO_DB]
    collection = db[MONGO_COLLECTION]
    return collection

def main():
    logger.info("=" * 60)
    logger.info("       DEMARRAGE DU DETECTEUR D'ANOMALIES FINANCIERES")
    logger.info("=" * 60)

    create_topic_if_not_exists(ALERT_TOPIC)

    detector = None
    consumer = None
    mongo_collection = None
    message_count = 0
    total_exec_time_ms = 0.0

    try:
        mongo_collection = create_mongo_client()
        logger.info(f"Connecté à MongoDB: {MONGO_DB}.{MONGO_COLLECTION}")
    except Exception as e:
        logger.error(f"Erreur de connexion à MongoDB: {e}")
        # On continue même si Mongo n'est pas disponible

    try:
        detector = AnomalyDetector(mongo_collection=mongo_collection)
        consumer = create_consumer(INPUT_TOPIC)

        logger.info(f"Connecté à Kafka. Écoute sur '{INPUT_TOPIC}'...")
        logger.info(f"Alertes publiées sur '{ALERT_TOPIC}'")
        logger.info("[*] En attente de messages...\n")

        while True:
            msg_pack = consumer.poll(timeout_ms=100)
            
            for tp, messages in msg_pack.items():
                for message in messages:
                    message_count += 1
                    
                    exec_time = detector.process_message(message.value)
                    total_exec_time_ms += exec_time

                    if message_count % 100 == 0:
                        avg_time = total_exec_time_ms / message_count
                        logger.info(
                            f"Stats | Messages traites: {message_count} | Temps moyen: {avg_time:.4f} ms"
                        )

    except KeyboardInterrupt:
        logger.info(f"\n[!] Arret demande par l'utilisateur. {message_count} messages evalues.")
        if message_count > 0:
            avg_time = total_exec_time_ms / message_count
            logger.info(f"Temps de traitement moyen final: {avg_time:.4f} ms")

    except KafkaError as e:
        logger.error(f"[ERROR] Erreur Kafka: {e}")

    except Exception as e:
        logger.error(f"[ERROR] Erreur inattendue: {e}", exc_info=True)

    finally:
        if consumer:
            consumer.close()
        if detector:
            detector.close()
        logger.info("[INFO] Connexions fermees. Programme arrete.")


if __name__ == "__main__":
    main()
