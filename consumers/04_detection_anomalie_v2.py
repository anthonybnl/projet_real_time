"""
Detecteur d'anomalies financieres V2.

Consomme le topic Kafka 'btc.cleaned' et detecte trois types d'anomalies
sur les trades BTC en temps reel :
  - Slippage anormal (z-score adaptatif sur les log-returns)
  - Spread elastique (spread bid/ask vs percentile rolling)
  - Order Flow Imbalance (desequilibre acheteurs/vendeurs)

Par rapport a la V1, cette version introduit des seuils adaptatifs
(rolling percentile via numpy), un filtre volume pour eviter les faux
positifs en periode de faible liquidite, une logique de combinaison
(alerte combinee si plusieurs regles se declenchent dans une fenetre
temporelle), un cooldown pour ne pas spammer les alertes, et une
phase de warm-up au demarrage.

Les alertes sont publiees sur le topic 'financial.alerts' et
sauvegardees dans MongoDB.
"""

import json
import os
import time
import math
import logging
import sys
import argparse
from collections import deque
from datetime import datetime, timezone
from dotenv import load_dotenv
import numpy as np
from pymongo import MongoClient
from kafka import KafkaConsumer, KafkaProducer, KafkaAdminClient
from kafka.admin import NewTopic
from kafka.errors import KafkaError, TopicAlreadyExistsError

load_dotenv()
# Kafka
BOOTSTRAP_SERVERS = "127.0.0.1:9092"
INPUT_TOPIC = "btc.cleaned"
ALERT_TOPIC = "financial.alerts"
GROUP_ID = "financial-anomaly-detector-v2-group"

# MongoDB
MONGO_URI = os.environ["MONGODB_URI"]
MONGO_DB = os.environ["MONGODB_DBNAME"]
MONGO_COLLECTION = "btc_anomalies"

# Sensibilite : low = peu d'alertes mais fiables, high = plus d'alertes, plus de bruit
SENSITIVITY = os.environ["ANOMALY_SENSITIVITY"]
SENSITIVITY_MAP = {
    "low":    {"z_pct": 99.5, "spread_pct": 99, "ofi_thresh": 0.92, "combo_required": 3, "combo_window": 60},
    "medium": {"z_pct": 99,   "spread_pct": 97, "ofi_thresh": 0.85, "combo_required": 2, "combo_window": 45},
    "high":   {"z_pct": 95,   "spread_pct": 95, "ofi_thresh": 0.78, "combo_required": 2, "combo_window": 30},
}

# Tailles des fenetres glissantes
SLIPPAGE_WINDOW = 300
SPREAD_WINDOW_SEC = 300
OFI_WINDOW_SEC = 10
VOLUME_WINDOW = 500
VOLUME_FILTER_FACTOR = 0.5
VOLUME_MIN_SAMPLES = 20

# On recalcule les percentiles tous les N messages pour pas appeler numpy a chaque tick
PERCENTILE_REFRESH_INTERVAL = 50

# Delai minimum entre deux alertes du meme type pour une meme paire
ALERT_COOLDOWN_SEC = 30

# Nombre de messages a accumuler avant de commencer la detection
WARMUP_MESSAGES = 300

# On n'alerte jamais sur un slippage dont le z-score est en dessous de ce seuil,
# meme si le percentile adaptatif le laisse passer (protege contre les seuils
# instables au demarrage)
Z_SCORE_FLOOR = 2.0

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("AnomalyDetectorV2")


class AnomalyDetectorV2:

    def __init__(
        self,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        alert_topic=ALERT_TOPIC,
        sensitivity=SENSITIVITY,
        mongo_collection=None,
        dry_run=False,
    ):
        self.bootstrap_servers = bootstrap_servers
        self.alert_topic = alert_topic
        self.sensitivity = sensitivity
        self.params = SENSITIVITY_MAP[sensitivity]
        self.mongo_collection = mongo_collection
        self.dry_run = dry_run
        self.alert_log = []  # utilise en mode dry_run (backtesting)

        self.producer = None
        if not dry_run:
            self.producer = KafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8") if k else None,
                acks="all",
                retries=3
            )

        # Etat par (exchange, symbol)
        self.prev_prices = {}
        self.log_returns = {}
        self.spreads = {}
        self.ofi_trades = {}
        self.volumes = {}
        self.recent_signals = {}

        self._pct_cache = {}
        self._last_alert_time = {}
        self._msg_count = {}

    # -- Parsing des messages (compatible Coinbase et Binance) --

    def _get_exchange_and_symbol(self, msg):
        exchange = msg.get("exchange") or ("coinbase" if "product_id" in msg or "sequence" in msg else "binance")
        symbol = msg.get("symbol") or msg.get("product_id") or msg.get("s") or "BTC-USD"
        return exchange.lower(), symbol.upper()

    def _get_price(self, msg):
        for key in ["price", "p", "last_price"]:
            val = msg.get(key)
            if val is not None:
                try:
                    return float(val)
                except (ValueError, TypeError):
                    pass
        return None

    def _get_volume(self, msg):
        for key in ["trade_size", "size", "q", "last_size", "volume"]:
            val = msg.get(key)
            if val is not None:
                try:
                    return float(val)
                except (ValueError, TypeError):
                    pass
        return None

    def _get_side(self, msg):
        side = msg.get("side")
        if side in ["buy", "sell", "BUY", "SELL"]:
            return side.lower()
        # Binance : le champ 'm' indique si l'acheteur est le maker
        if "m" in msg:
            return "sell" if msg["m"] else "buy"
        return None

    def _get_msg_timestamp(self, msg):
        """Timestamp du message en epoch seconds. On prefere le timestamp
        du trade plutot que l'horloge systeme, sinon le rattrapage d'historique
        fausse les fenetres glissantes."""
        ts = msg.get("timestamp") or msg.get("time") or msg.get("T") or msg.get("E")
        if ts is not None:
            try:
                if isinstance(ts, str):
                    from datetime import datetime, timezone
                    dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
                    return dt.timestamp()
                ts_num = float(ts)
                # Binance envoie des timestamps en millisecondes
                if ts_num > 1e12:
                    return ts_num / 1000.0
                return ts_num
            except (ValueError, TypeError):
                pass
        return time.time()

    def _get_best_bid_ask(self, msg):
        bid = msg.get("best_bid") or msg.get("bid") or msg.get("b")
        ask = msg.get("best_ask") or msg.get("ask") or msg.get("a")
        bid_val, ask_val = None, None
        try:
            bid_val = float(bid) if bid is not None else None
        except (ValueError, TypeError):
            pass
        try:
            ask_val = float(ask) if ask is not None else None
        except (ValueError, TypeError):
            pass
        # Si le message ne contient pas de bid/ask, on genere un spread
        # synthetique pour ne pas bloquer la detection
        price = self._get_price(msg)
        if bid_val is None and price is not None:
            bid_val = price - 0.02
        if ask_val is None and price is not None:
            ask_val = price + 0.03
        return bid_val, ask_val

    # -- Seuils adaptatifs --

    def _refresh_thresholds(self, key):
        """Recalcule les percentiles tous les N messages. On cache le resultat
        pour eviter d'appeler numpy a chaque tick."""
        cache = self._pct_cache.get(key)
        if cache is None:
            cache = {"z_hi": None, "z_lo": None, "spread_thresh": None, "msg_count": 0}
            self._pct_cache[key] = cache

        cache["msg_count"] += 1
        if cache["msg_count"] % PERCENTILE_REFRESH_INTERVAL != 0:
            return

        ret_deque = self.log_returns.get(key)
        if ret_deque and len(ret_deque) >= 30:
            arr = np.array(ret_deque)
            pct = self.params["z_pct"]
            cache["z_hi"] = float(np.percentile(arr, pct))
            cache["z_lo"] = float(np.percentile(arr, 100 - pct))

        spread_deque = self.spreads.get(key)
        if spread_deque and len(spread_deque) >= 10:
            spreads_arr = np.array([s for _, s in spread_deque])
            cache["spread_thresh"] = float(np.percentile(spreads_arr, self.params["spread_pct"]))

    # -- Filtre volume --

    def _passes_volume_filter(self, key, volume):
        """Rejette les signaux quand le volume est trop faible. Ca evite les
        faux positifs en periode creuse ou le moindre trade fait bouger les ratios."""
        if volume is None:
            return True
        vol_deque = self.volumes.get(key)
        if vol_deque is None or len(vol_deque) < VOLUME_MIN_SAMPLES:
            return True
        vol_mean = sum(vol_deque) / len(vol_deque)
        return volume >= vol_mean * VOLUME_FILTER_FACTOR

    # -- Combinaison des signaux --

    def _record_signal(self, key, now, rule_name, intensity):
        if key not in self.recent_signals:
            self.recent_signals[key] = deque()
        self.recent_signals[key].append((now, rule_name, intensity))

    def _evaluate_combination(self, key, now, msg, exchange, symbol):
        """Verifie si suffisamment de regles ont declenche dans la fenetre
        temporelle. Si oui, on envoie une alerte combinee avec un score
        de confiance."""
        signals = self.recent_signals.get(key)
        if not signals:
            return

        combo_window = self.params["combo_window"]
        combo_required = self.params["combo_required"]

        # Virer les signaux trop vieux
        while signals and signals[0][0] < now - combo_window:
            signals.popleft()

        # On garde l'intensite max par regle
        active_rules = {}
        for _, rule, intensity in signals:
            if rule not in active_rules or intensity > active_rules[rule]:
                active_rules[rule] = intensity

        n_rules = len(active_rules)
        if n_rules < combo_required:
            return

        total_intensity = sum(active_rules.values())
        confidence = min(100, int(30 * n_rules + 10 * total_intensity))

        self._trigger_alert(
            anomaly_type="COMBINED_ANOMALY",
            exchange=exchange,
            symbol=symbol,
            details={
                "confidence": confidence,
                "active_rules": list(active_rules.keys()),
                "rule_intensities": active_rules,
                "n_rules": n_rules,
                "combo_window_sec": combo_window,
                "sensitivity": self.sensitivity,
                "description": (
                    f"Anomalie combinee : {n_rules} regles actives "
                    f"({', '.join(active_rules.keys())}), confiance {confidence}/100."
                ),
            },
            trigger_message=msg,
        )

    # -- Envoi d'alerte --

    def _trigger_alert(self, anomaly_type, exchange, symbol, details, trigger_message, msg_time=None):
        # Cooldown : on ne renvoie pas la meme alerte si la precedente date
        # de moins de ALERT_COOLDOWN_SEC secondes
        cooldown_key = (exchange, symbol, anomaly_type)
        now_real = time.time()
        last = self._last_alert_time.get(cooldown_key, 0)
        if now_real - last < ALERT_COOLDOWN_SEC:
            return
        self._last_alert_time[cooldown_key] = now_real

        alert = {
            "anomaly_type": anomaly_type,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "exchange": exchange,
            "symbol": symbol,
            "details": details,
            "trigger_message": trigger_message,
        }

        logger.warning(f"[!] [ANOMALIE] {anomaly_type} | {exchange} | {details.get('description', '')}")

        if self.dry_run:
            self.alert_log.append(alert)
            return

        if self.producer:
            try:
                self.producer.send(self.alert_topic, key=f"{exchange}-{anomaly_type}", value=alert)
            except Exception as e:
                logger.error(f"Erreur Kafka: {e}")

        if self.mongo_collection is not None:
            try:
                alert_copy = dict(alert)
                alert_copy["timestamp"] = datetime.now(timezone.utc)
                self.mongo_collection.insert_one(alert_copy)
            except Exception as e:
                logger.error(f"Erreur MongoDB: {e}")

    # -- Traitement d'un message --

    def process_message(self, msg):
        start_ns = time.perf_counter_ns()
        now = self._get_msg_timestamp(msg)

        exchange, symbol = self._get_exchange_and_symbol(msg)
        key = (exchange, symbol)

        self._msg_count[key] = self._msg_count.get(key, 0) + 1

        volume = self._get_volume(msg)

        # On accumule le volume meme pendant le warm-up
        if volume is not None:
            if key not in self.volumes:
                self.volumes[key] = deque(maxlen=VOLUME_WINDOW)
            self.volumes[key].append(volume)

        if not self._passes_volume_filter(key, volume):
            return (time.perf_counter_ns() - start_ns) / 1_000_000.0

        self._refresh_thresholds(key)
        cache = self._pct_cache.get(key, {})

        warmed_up = self._msg_count[key] >= WARMUP_MESSAGES
        if self._msg_count[key] == WARMUP_MESSAGES:
            logger.info(f"[WARMUP] {exchange}/{symbol} : warm-up termine ({WARMUP_MESSAGES} messages). Detection active.")

        # Slippage anormal : on compare le log-return au percentile adaptatif.
        # Le z-score plancher empeche les faux positifs quand les seuils
        # ne sont pas encore stables.
        price = self._get_price(msg)
        if price is not None and price > 0:
            prev_price = self.prev_prices.get(key)
            if prev_price is not None and prev_price > 0:
                log_ret = math.log(price / prev_price)

                if key not in self.log_returns:
                    self.log_returns[key] = deque(maxlen=SLIPPAGE_WINDOW)
                self.log_returns[key].append(log_ret)

                if warmed_up:
                    ret_deque = self.log_returns[key]
                    if len(ret_deque) >= 30:
                        z_hi = cache.get("z_hi")
                        z_lo = cache.get("z_lo")

                        if z_hi is not None and z_lo is not None:
                            if log_ret > z_hi or log_ret < z_lo:
                                n = len(ret_deque)
                                mean = sum(ret_deque) / n
                                std = math.sqrt(sum((x - mean) ** 2 for x in ret_deque) / n)
                                z_score = (log_ret - mean) / std if std > 1e-9 else 0

                                if abs(z_score) >= Z_SCORE_FLOOR:
                                    intensity = abs(z_score) / 4.0
                                    self._record_signal(key, now, "SLIPPAGE", intensity)
                                    self._trigger_alert(
                                        anomaly_type="SLIPPAGE_ANORMAL",
                                        exchange=exchange, symbol=symbol,
                                        details={
                                            "price": price, "previous_price": prev_price,
                                            "log_return": log_ret, "z_score": z_score,
                                            "adaptive_threshold_hi": z_hi, "adaptive_threshold_lo": z_lo,
                                            "intensity": intensity,
                                            "description": f"Variation brutale du prix de {prev_price:.2f} a {price:.2f} (z={z_score:.2f}).",
                                        },
                                        trigger_message=msg,
                                    )

            self.prev_prices[key] = price

        # Spread elastique : on alerte si le spread depasse le percentile adaptatif
        bid, ask = self._get_best_bid_ask(msg)
        if bid is not None and ask is not None:
            spread = ask - bid

            if key not in self.spreads:
                self.spreads[key] = deque()

            while self.spreads[key] and self.spreads[key][0][0] < now - SPREAD_WINDOW_SEC:
                self.spreads[key].popleft()

            if warmed_up:
                spread_thresh = cache.get("spread_thresh")
                if spread_thresh is not None and len(self.spreads[key]) >= 10:
                    if spread > spread_thresh:
                        mean_spread = sum(s for _, s in self.spreads[key]) / len(self.spreads[key])
                        ratio = spread / mean_spread if mean_spread > 0 else 0
                        intensity = ratio / 2.0

                        self._record_signal(key, now, "SPREAD", intensity)
                        self._trigger_alert(
                            anomaly_type="SPREAD_ELASTIQUE",
                            exchange=exchange, symbol=symbol,
                            details={
                                "bid": bid, "ask": ask,
                                "current_spread": spread, "adaptive_threshold": spread_thresh,
                                "rolling_mean_spread": mean_spread, "ratio": ratio,
                                "intensity": intensity,
                                "description": f"Spread anormalement elargi ({spread:.4f} USD, {ratio:.2f}x la moyenne).",
                            },
                            trigger_message=msg,
                        )

            self.spreads[key].append((now, spread))

        # Order Flow Imbalance : ratio (buy - sell) / total sur la fenetre de 10s
        side = self._get_side(msg)
        if volume is not None and side in ["buy", "sell"]:
            if key not in self.ofi_trades:
                self.ofi_trades[key] = deque()

            self.ofi_trades[key].append((now, volume, side))

            while self.ofi_trades[key] and self.ofi_trades[key][0][0] < now - OFI_WINDOW_SEC:
                self.ofi_trades[key].popleft()

            if warmed_up and self.ofi_trades[key]:
                vol_buy = sum(v for _, v, s in self.ofi_trades[key] if s == "buy")
                vol_sell = sum(v for _, v, s in self.ofi_trades[key] if s == "sell")
                vol_total = vol_buy + vol_sell

                if vol_total > 0:
                    ratio = (vol_buy - vol_sell) / vol_total
                    ofi_thresh = self.params["ofi_thresh"]
                    if abs(ratio) > ofi_thresh:
                        intensity = abs(ratio) / ofi_thresh

                        self._record_signal(key, now, "OFI", intensity)
                        dominant = "acheteurs" if ratio > 0 else "vendeurs"
                        self._trigger_alert(
                            anomaly_type="ORDER_FLOW_IMBALANCE",
                            exchange=exchange, symbol=symbol,
                            details={
                                "vol_buy": vol_buy, "vol_sell": vol_sell, "vol_total": vol_total,
                                "ofi_ratio": ratio, "threshold": ofi_thresh,
                                "intensity": intensity,
                                "description": f"Desequilibre {dominant} ({abs(ratio)*100:.1f}% du volume sur {OFI_WINDOW_SEC}s).",
                            },
                            trigger_message=msg,
                        )

        # Evaluation combinee
        if warmed_up:
            self._evaluate_combination(key, now, msg, exchange, symbol)

        return (time.perf_counter_ns() - start_ns) / 1_000_000.0

    def close(self):
        if self.producer:
            self.producer.flush()
            self.producer.close()
            logger.info("Producteur d'alertes ferme.")


# -- Utilitaires Kafka / Mongo --

def create_topic_if_not_exists(topic_name, bootstrap_servers=BOOTSTRAP_SERVERS):
    admin_client = None
    for attempt in range(10):
        try:
            admin_client = KafkaAdminClient(
                bootstrap_servers=bootstrap_servers, client_id="anomaly-v2-topic-creator"
            )
            break
        except Exception as e:
            if attempt < 9:
                logger.warning(f"Attente Kafka ({attempt + 1}/10)...")
                time.sleep(2)
            else:
                raise e

    try:
        existing = admin_client.list_topics()
        if topic_name not in existing:
            admin_client.create_topics(
                new_topics=[NewTopic(name=topic_name, num_partitions=3, replication_factor=1)],
                validate_only=False,
            )
            logger.info(f"Topic '{topic_name}' cree.")
        else:
            logger.info(f"Topic '{topic_name}' existe deja.")
    except TopicAlreadyExistsError:
        pass
    except Exception as e:
        logger.error(f"Erreur creation topic {topic_name}: {e}")
    finally:
        if admin_client:
            admin_client.close()


def create_consumer(topic_name, bootstrap_servers=BOOTSTRAP_SERVERS):
    return KafkaConsumer(
        topic_name,
        bootstrap_servers=bootstrap_servers,
        group_id=GROUP_ID,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        auto_commit_interval_ms=1000,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        key_deserializer=lambda k: k.decode("utf-8") if k else None,
    )


def create_mongo_collection():
    client = MongoClient(MONGO_URI)
    return client[MONGO_DB][MONGO_COLLECTION]


def main():
    parser = argparse.ArgumentParser(description="Detecteur d'anomalies V2")
    parser.add_argument("--sensitivity", choices=["low", "medium", "high"], default=SENSITIVITY)
    args = parser.parse_args()

    sensitivity = args.sensitivity
    logger.info("=" * 60)
    logger.info("  DETECTEUR D'ANOMALIES V2")
    logger.info(f"  Sensibilite : {sensitivity}")
    logger.info("=" * 60)

    create_topic_if_not_exists(ALERT_TOPIC)

    mongo_collection = None
    try:
        mongo_collection = create_mongo_collection()
        logger.info(f"Connecte a MongoDB: {MONGO_DB}.{MONGO_COLLECTION}")
    except Exception as e:
        logger.error(f"Erreur MongoDB: {e}")

    detector = None
    consumer = None
    message_count = 0
    total_exec_time = 0.0
    window_count = 0
    window_exec_time = 0.0
    window_start = time.time()

    try:
        detector = AnomalyDetectorV2(
            sensitivity=sensitivity,
            mongo_collection=mongo_collection,
        )
        consumer = create_consumer(INPUT_TOPIC)

        logger.info(f"Ecoute sur '{INPUT_TOPIC}', alertes sur '{ALERT_TOPIC}'")
        logger.info(f"Parametres : {detector.params}")
        logger.info("En attente de messages...\n")

        while True:
            msg_pack = consumer.poll(timeout_ms=100)
            for tp, messages in msg_pack.items():
                for message in messages:
                    message_count += 1
                    window_count += 1
                    exec_time = detector.process_message(message.value)
                    total_exec_time += exec_time
                    window_exec_time += exec_time

                    elapsed = time.time() - window_start
                    if elapsed >= 10:
                        rate = window_count / elapsed
                        avg = window_exec_time / window_count if window_count else 0
                        logger.info(f"[04_anomalie_v2] {message_count} messages traites ({rate:.1f}/s) | Temps moyen: {avg:.4f} ms")
                        window_count = 0
                        window_exec_time = 0.0
                        window_start = time.time()

    except KeyboardInterrupt:
        logger.info(f"\nArret. {message_count} messages traites.")
        if message_count > 0:
            logger.info(f"Temps moyen: {total_exec_time / message_count:.4f} ms")
    except KafkaError as e:
        logger.error(f"Erreur Kafka: {e}")
    except Exception as e:
        logger.error(f"Erreur: {e}", exc_info=True)
    finally:
        if consumer:
            consumer.close()
        if detector:
            detector.close()
        logger.info("Connexions fermees.")


if __name__ == "__main__":
    main()
