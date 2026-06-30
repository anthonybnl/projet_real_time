"""
Detecteur d'anomalies financieres V3.

Herite de la V2 et ajoute trois nouvelles regles de detection :
  - Depth Erosion : chute brutale de la profondeur du carnet d'ordres
  - VPIN : probabilite de trading informe (simplifie)
  - Whale Alert : ordres anormalement gros

Le scoring combine est pondere par regle (configurable via RULE_WEIGHTS).
Un module de backtesting permet de rejouer des trades historiques et
de calculer Precision, Recall et F1 par rapport a des evenements connus.
"""

import json
import time
import math
import logging
import sys
import csv
import argparse
from collections import deque
from datetime import datetime, timezone
from itertools import product as iter_product
import numpy as np
from pymongo import MongoClient
from kafka import KafkaConsumer, KafkaProducer, KafkaAdminClient
from kafka.admin import NewTopic
from kafka.errors import KafkaError, TopicAlreadyExistsError

# Le nom de fichier commence par un chiffre, donc on ne peut pas
# faire un import classique de la V2
import importlib.util
import os

_v2_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "04_detection_anomalie_v2.py")
_spec = importlib.util.spec_from_file_location("detection_anomalie_v2", _v2_path)
_v2 = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_v2)

AnomalyDetectorV2 = _v2.AnomalyDetectorV2
BOOTSTRAP_SERVERS = _v2.BOOTSTRAP_SERVERS
INPUT_TOPIC = _v2.INPUT_TOPIC
ALERT_TOPIC = _v2.ALERT_TOPIC
MONGO_URI = _v2.MONGO_URI
MONGO_DB = _v2.MONGO_DB
MONGO_COLLECTION = _v2.MONGO_COLLECTION
SENSITIVITY = _v2.SENSITIVITY
SENSITIVITY_MAP = _v2.SENSITIVITY_MAP
create_topic_if_not_exists = _v2.create_topic_if_not_exists
create_consumer = _v2.create_consumer
create_mongo_collection = _v2.create_mongo_collection

GROUP_ID = "financial-anomaly-detector-v3-group"

# Depth Erosion : on surveille les N premiers niveaux du carnet.
# Si la profondeur totale chute de plus de 40% par rapport a la
# moyenne glissante, ca indique que la liquidite s'evapore.
DEPTH_LEVELS = 5
DEPTH_WINDOW = 60
DEPTH_EROSION_THRESHOLD = 0.4

# VPIN : on decoupe le flux de trades en buckets de volume fixe.
# Pour chaque bucket, on regarde si c'est du buy ou du sell.
# Un VPIN eleve signifie que le flux est desequilibre, donc
# qu'il y a probablement du trading informe.
VPIN_BUCKET_SIZE_BTC = 0.5
VPIN_WINDOW_BUCKETS = 50
VPIN_THRESHOLD = 0.7

# Whale Alert : on alerte quand un trade depasse le 99.5e percentile
# des tailles recentes. Ca detecte les gros ordres qui peuvent
# provoquer du slippage.
WHALE_TRADE_WINDOW = 1000
WHALE_PERCENTILE = 99.5
WHALE_MIN_SAMPLES = 100

# Poids de chaque regle dans le scoring combine.
# Le VPIN est pondere plus fort parce qu'il capte un signal
# structurel (trading informe), pas juste du bruit de marche.
RULE_WEIGHTS = {
    "SLIPPAGE": 1.0,
    "SPREAD": 0.8,
    "OFI": 1.0,
    "DEPTH_EROSION": 1.2,
    "VPIN": 1.5,
    "WHALE": 0.7,
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("AnomalyDetectorV3")


class AnomalyDetectorV3(AnomalyDetectorV2):

    def __init__(self, rule_weights=None, **kwargs):
        super().__init__(**kwargs)
        self.rule_weights = rule_weights or RULE_WEIGHTS

        self.orderbook_depths = {}
        self.vpin_accumulator = {}
        self.vpin_buckets = {}
        self.trade_sizes = {}
        self._whale_thresh_cache = {}

    def _get_orderbook_depth(self, msg):
        """Extrait la profondeur totale des N premiers niveaux bid + ask.
        Accepte soit une liste de [prix, quantite], soit un scalaire."""
        bids = msg.get("bids") or msg.get("b_depth")
        asks = msg.get("asks") or msg.get("a_depth")

        if bids is None or asks is None:
            return None, None

        try:
            if isinstance(bids, list):
                bid_depth = sum(float(b[1]) for b in bids[:DEPTH_LEVELS] if len(b) >= 2)
            else:
                bid_depth = float(bids)

            if isinstance(asks, list):
                ask_depth = sum(float(a[1]) for a in asks[:DEPTH_LEVELS] if len(a) >= 2)
            else:
                ask_depth = float(asks)

            return bid_depth, ask_depth
        except (ValueError, TypeError, IndexError):
            return None, None

    # Depth Erosion

    def _check_depth_erosion(self, key, now, msg, exchange, symbol):
        bid_depth, ask_depth = self._get_orderbook_depth(msg)
        if bid_depth is None or ask_depth is None:
            return

        total_depth = bid_depth + ask_depth

        if key not in self.orderbook_depths:
            self.orderbook_depths[key] = deque(maxlen=DEPTH_WINDOW)

        depths = self.orderbook_depths[key]
        depths.append((now, total_depth))

        if len(depths) < 5:
            return

        mean_depth = sum(d for _, d in depths) / len(depths)
        if mean_depth <= 0:
            return

        erosion_ratio = 1.0 - (total_depth / mean_depth)
        if erosion_ratio > DEPTH_EROSION_THRESHOLD:
            intensity = erosion_ratio / DEPTH_EROSION_THRESHOLD
            self._record_signal(key, now, "DEPTH_EROSION", intensity)
            self._trigger_alert(
                anomaly_type="DEPTH_EROSION", exchange=exchange, symbol=symbol,
                details={
                    "total_depth": total_depth, "mean_depth": mean_depth,
                    "erosion_ratio": erosion_ratio, "intensity": intensity,
                    "description": (
                        f"Erosion de la profondeur du carnet de {erosion_ratio*100:.1f}% "
                        f"(depth={total_depth:.4f} vs moyenne={mean_depth:.4f})."
                    ),
                },
                trigger_message=msg,
            )

    # VPIN simplifie

    def _check_vpin(self, key, now, msg, exchange, symbol):
        volume = self._get_volume(msg)
        side = self._get_side(msg)
        if volume is None or side is None:
            return

        if key not in self.vpin_accumulator:
            self.vpin_accumulator[key] = {"buy": 0.0, "sell": 0.0, "total": 0.0}
        if key not in self.vpin_buckets:
            self.vpin_buckets[key] = deque(maxlen=VPIN_WINDOW_BUCKETS)

        acc = self.vpin_accumulator[key]
        if side == "buy":
            acc["buy"] += volume
        else:
            acc["sell"] += volume
        acc["total"] += volume

        # Quand le bucket est plein, on le ferme et on en ouvre un nouveau
        while acc["total"] >= VPIN_BUCKET_SIZE_BTC:
            overflow = acc["total"] - VPIN_BUCKET_SIZE_BTC
            ratio_used = VPIN_BUCKET_SIZE_BTC / acc["total"] if acc["total"] > 0 else 1.0

            self.vpin_buckets[key].append((acc["buy"] * ratio_used, acc["sell"] * ratio_used))

            acc["buy"] = acc["buy"] * (1 - ratio_used)
            acc["sell"] = acc["sell"] * (1 - ratio_used)
            acc["total"] = overflow

        buckets = self.vpin_buckets[key]
        if len(buckets) < 10:
            return

        total_buy = sum(b for b, _ in buckets)
        total_sell = sum(s for _, s in buckets)
        total_vol = total_buy + total_sell
        if total_vol <= 0:
            return

        vpin = abs(total_buy - total_sell) / total_vol
        if vpin > VPIN_THRESHOLD:
            intensity = vpin / VPIN_THRESHOLD
            self._record_signal(key, now, "VPIN", intensity)
            self._trigger_alert(
                anomaly_type="VPIN", exchange=exchange, symbol=symbol,
                details={
                    "vpin": vpin, "threshold": VPIN_THRESHOLD,
                    "n_buckets": len(buckets), "intensity": intensity,
                    "description": f"VPIN eleve ({vpin:.3f} > {VPIN_THRESHOLD}), probabilite de trading informe.",
                },
                trigger_message=msg,
            )

    # Whale Alert

    def _check_whale(self, key, now, msg, exchange, symbol):
        volume = self._get_volume(msg)
        if volume is None:
            return

        if key not in self.trade_sizes:
            self.trade_sizes[key] = deque(maxlen=WHALE_TRADE_WINDOW)

        self.trade_sizes[key].append(volume)

        if len(self.trade_sizes[key]) < WHALE_MIN_SAMPLES:
            return

        # On recalcule le seuil tous les 50 trades pour pas appeler numpy a chaque tick
        cached = self._whale_thresh_cache.get(key)
        if cached is None or abs(len(self.trade_sizes[key]) - cached[1]) >= 50:
            thresh = float(np.percentile(list(self.trade_sizes[key]), WHALE_PERCENTILE))
            self._whale_thresh_cache[key] = (thresh, len(self.trade_sizes[key]))
        else:
            thresh = cached[0]

        if volume > thresh and thresh > 0:
            intensity = volume / thresh
            self._record_signal(key, now, "WHALE", intensity)
            self._trigger_alert(
                anomaly_type="WHALE_ALERT", exchange=exchange, symbol=symbol,
                details={
                    "volume": volume, "threshold": thresh, "intensity": intensity,
                    "description": f"Ordre massif detecte : {volume:.6f} BTC ({intensity:.1f}x le seuil adaptatif).",
                },
                trigger_message=msg,
            )

    # Scoring combine pondere (override de la V2)

    def _evaluate_combination(self, key, now, msg, exchange, symbol):
        signals = self.recent_signals.get(key)
        if not signals:
            return

        combo_window = self.params["combo_window"]
        combo_required = self.params["combo_required"]

        while signals and signals[0][0] < now - combo_window:
            signals.popleft()

        active_rules = {}
        for _, rule, intensity in signals:
            if rule not in active_rules or intensity > active_rules[rule]:
                active_rules[rule] = intensity

        n_rules = len(active_rules)
        if n_rules < combo_required:
            return

        weighted_score = sum(
            self.rule_weights.get(rule, 1.0) * intensity
            for rule, intensity in active_rules.items()
        )
        confidence = min(100, int(weighted_score * 15 + n_rules * 10))

        self._trigger_alert(
            anomaly_type="COMBINED_ANOMALY",
            exchange=exchange,
            symbol=symbol,
            details={
                "confidence": confidence,
                "active_rules": list(active_rules.keys()),
                "rule_intensities": active_rules,
                "rule_weights": {r: self.rule_weights.get(r, 1.0) for r in active_rules},
                "weighted_score": round(weighted_score, 2),
                "n_rules": n_rules,
                "combo_window_sec": combo_window,
                "sensitivity": self.sensitivity,
                "description": (
                    f"Anomalie combinee V3 : {n_rules} regles actives "
                    f"({', '.join(active_rules.keys())}), confiance {confidence}/100 "
                    f"(score pondere: {weighted_score:.2f})."
                ),
            },
            trigger_message=msg,
        )

    # Traitement d'un message avec les 6 regles

    def process_message(self, msg):
        start_ns = time.perf_counter_ns()
        now = time.time()

        exchange, symbol = self._get_exchange_and_symbol(msg)
        key = (exchange, symbol)

        volume = self._get_volume(msg)

        if volume is not None:
            if key not in self.volumes:
                self.volumes[key] = deque(maxlen=500)
            self.volumes[key].append(volume)

        if not self._passes_volume_filter(key, volume):
            return (time.perf_counter_ns() - start_ns) / 1_000_000.0

        self._refresh_thresholds(key)
        cache = self._pct_cache.get(key, {})

        # Slippage
        price = self._get_price(msg)
        if price is not None and price > 0:
            prev_price = self.prev_prices.get(key)
            if prev_price is not None and prev_price > 0:
                log_ret = math.log(price / prev_price)

                if key not in self.log_returns:
                    self.log_returns[key] = deque(maxlen=300)
                self.log_returns[key].append(log_ret)

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
                            intensity = abs(z_score) / 4.0
                            self._record_signal(key, now, "SLIPPAGE", intensity)
                            self._trigger_alert(
                                anomaly_type="SLIPPAGE_ANORMAL", exchange=exchange, symbol=symbol,
                                details={
                                    "price": price, "previous_price": prev_price,
                                    "log_return": log_ret, "z_score": z_score, "intensity": intensity,
                                    "description": f"Variation brutale du prix de {prev_price:.2f} a {price:.2f} (z={z_score:.2f}).",
                                },
                                trigger_message=msg,
                            )

            self.prev_prices[key] = price

        # Spread
        bid, ask = self._get_best_bid_ask(msg)
        if bid is not None and ask is not None:
            spread = ask - bid
            if key not in self.spreads:
                self.spreads[key] = deque()
            while self.spreads[key] and self.spreads[key][0][0] < now - 300:
                self.spreads[key].popleft()

            spread_thresh = cache.get("spread_thresh")
            if spread_thresh is not None and len(self.spreads[key]) >= 10:
                if spread > spread_thresh:
                    mean_spread = sum(s for _, s in self.spreads[key]) / len(self.spreads[key])
                    ratio = spread / mean_spread if mean_spread > 0 else 0
                    intensity = ratio / 2.0
                    self._record_signal(key, now, "SPREAD", intensity)
                    self._trigger_alert(
                        anomaly_type="SPREAD_ELASTIQUE", exchange=exchange, symbol=symbol,
                        details={
                            "bid": bid, "ask": ask, "current_spread": spread,
                            "adaptive_threshold": spread_thresh, "rolling_mean_spread": mean_spread,
                            "ratio": ratio, "intensity": intensity,
                            "description": f"Spread anormalement elargi ({spread:.4f} USD, {ratio:.2f}x la moyenne).",
                        },
                        trigger_message=msg,
                    )

            self.spreads[key].append((now, spread))

        # OFI
        side = self._get_side(msg)
        if volume is not None and side in ["buy", "sell"]:
            if key not in self.ofi_trades:
                self.ofi_trades[key] = deque()
            self.ofi_trades[key].append((now, volume, side))
            while self.ofi_trades[key] and self.ofi_trades[key][0][0] < now - 10:
                self.ofi_trades[key].popleft()

            if self.ofi_trades[key]:
                vol_buy = sum(v for _, v, s in self.ofi_trades[key] if s == "buy")
                vol_sell = sum(v for _, v, s in self.ofi_trades[key] if s == "sell")
                vol_total = vol_buy + vol_sell
                if vol_total > 0:
                    ofi_ratio = (vol_buy - vol_sell) / vol_total
                    ofi_thresh = self.params["ofi_thresh"]
                    if abs(ofi_ratio) > ofi_thresh:
                        intensity = abs(ofi_ratio) / ofi_thresh
                        dominant = "acheteurs" if ofi_ratio > 0 else "vendeurs"
                        self._record_signal(key, now, "OFI", intensity)
                        self._trigger_alert(
                            anomaly_type="ORDER_FLOW_IMBALANCE", exchange=exchange, symbol=symbol,
                            details={
                                "vol_buy": vol_buy, "vol_sell": vol_sell, "vol_total": vol_total,
                                "ofi_ratio": ofi_ratio, "threshold": ofi_thresh, "intensity": intensity,
                                "description": f"Desequilibre {dominant} ({abs(ofi_ratio)*100:.1f}% du volume sur 10s).",
                            },
                            trigger_message=msg,
                        )

        # Depth Erosion, VPIN, Whale
        self._check_depth_erosion(key, now, msg, exchange, symbol)
        self._check_vpin(key, now, msg, exchange, symbol)
        self._check_whale(key, now, msg, exchange, symbol)

        self._evaluate_combination(key, now, msg, exchange, symbol)

        return (time.perf_counter_ns() - start_ns) / 1_000_000.0


class Backtester:
    """Rejoue des trades historiques a travers le detecteur et calcule
    les metriques de performance par rapport a des evenements connus."""

    def __init__(self, detector_class=AnomalyDetectorV3, sensitivity="medium", rule_weights=None):
        self.detector_class = detector_class
        self.sensitivity = sensitivity
        self.rule_weights = rule_weights

    def load_trades_csv(self, filepath):
        """Charge un CSV de trades. Accepte plusieurs formats de colonnes
        (timestamp/time, price/p, size/qty/volume, side, symbol, exchange)."""
        trades = []
        with open(filepath, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                msg = {}
                for k, v in row.items():
                    k_lower = k.strip().lower()
                    if k_lower in ("timestamp", "time"):
                        msg["timestamp"] = v.strip()
                    elif k_lower in ("price", "p"):
                        msg["price"] = v.strip()
                    elif k_lower in ("size", "qty", "quantity", "volume", "q", "trade_size"):
                        msg["trade_size"] = v.strip()
                    elif k_lower in ("side",):
                        msg["side"] = v.strip()
                    elif k_lower in ("symbol", "pair", "product_id", "s"):
                        msg["symbol"] = v.strip()
                    elif k_lower in ("exchange",):
                        msg["exchange"] = v.strip()
                    else:
                        msg[k.strip()] = v.strip()

                if "exchange" not in msg:
                    msg["exchange"] = "binance"
                if "symbol" not in msg:
                    msg["symbol"] = "BTC-USD"

                trades.append(msg)
        return trades

    def run(self, trades, known_events=None, tolerance_sec=60):
        """Rejoue les trades et retourne les alertes generees.
        Si known_events est fourni, calcule aussi precision/recall/F1."""
        detector = self.detector_class(
            dry_run=True,
            sensitivity=self.sensitivity,
            rule_weights=self.rule_weights,
        )

        exec_times = []
        for msg in trades:
            t = detector.process_message(msg)
            exec_times.append(t)

        alerts = detector.alert_log
        result = {
            "total_messages": len(trades),
            "total_alerts": len(alerts),
            "alerts": alerts,
            "avg_exec_time_ms": sum(exec_times) / len(exec_times) if exec_times else 0,
            "max_exec_time_ms": max(exec_times) if exec_times else 0,
        }

        if known_events:
            metrics = self._compute_metrics(alerts, known_events, tolerance_sec)
            result.update(metrics)

        detector.close()
        return result

    def _compute_metrics(self, alerts, known_events, tolerance_sec):
        """Match les alertes aux evenements connus avec une tolerance temporelle.
        Un evenement est un TP si au moins une alerte tombe dans la fenetre."""
        alert_times = []
        for a in alerts:
            ts = a.get("timestamp", "")
            try:
                dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
                alert_times.append(dt.timestamp())
            except (ValueError, AttributeError):
                pass

        event_times = []
        for e in known_events:
            ts = e.get("timestamp", "")
            try:
                dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
                event_times.append(dt.timestamp())
            except (ValueError, AttributeError):
                pass

        matched_events = set()
        tp = 0
        for at in alert_times:
            for i, et in enumerate(event_times):
                if i not in matched_events and abs(at - et) <= tolerance_sec:
                    tp += 1
                    matched_events.add(i)
                    break

        fp = len(alert_times) - tp
        fn = len(event_times) - len(matched_events)

        precision = tp / (tp + fp) if (tp + fp) > 0 else 0.0
        recall = tp / (tp + fn) if (tp + fn) > 0 else 0.0
        f1 = 2 * precision * recall / (precision + recall) if (precision + recall) > 0 else 0.0

        return {
            "true_positives": tp,
            "false_positives": fp,
            "false_negatives": fn,
            "precision": round(precision, 4),
            "recall": round(recall, 4),
            "f1_score": round(f1, 4),
        }

    def grid_search(self, trades, param_grid, known_events, tolerance_sec=60):
        """Teste toutes les combinaisons de parametres et retourne celle
        qui maximise le F1-score."""
        keys = list(param_grid.keys())
        values = list(param_grid.values())

        best_f1 = -1
        best_params = {}
        all_results = []

        for combo in iter_product(*values):
            params = dict(zip(keys, combo))

            sensitivity = params.pop("sensitivity", self.sensitivity)
            rule_weights = params.pop("rule_weights", self.rule_weights)

            bt = Backtester(
                detector_class=self.detector_class,
                sensitivity=sensitivity,
                rule_weights=rule_weights,
            )
            result = bt.run(trades, known_events, tolerance_sec)
            result["params"] = {"sensitivity": sensitivity, **params}

            all_results.append(result)
            logger.info(
                f"Grid search | sensitivity={sensitivity} | "
                f"F1={result.get('f1_score', 0):.4f} | "
                f"P={result.get('precision', 0):.4f} | R={result.get('recall', 0):.4f}"
            )

            if result.get("f1_score", 0) > best_f1:
                best_f1 = result["f1_score"]
                best_params = result["params"]

        return {
            "best_params": best_params,
            "best_f1": round(best_f1, 4),
            "all_results": all_results,
        }


def main():
    parser = argparse.ArgumentParser(description="Detecteur d'anomalies V3")
    parser.add_argument("--sensitivity", choices=["low", "medium", "high"], default=SENSITIVITY)
    parser.add_argument("--backtest", type=str, help="Chemin vers un CSV de trades historiques")
    parser.add_argument("--events", type=str, help="Chemin vers un CSV d'evenements connus (timestamp, type)")
    args = parser.parse_args()

    sensitivity = args.sensitivity

    if args.backtest:
        logger.info("=" * 60)
        logger.info("  MODE BACKTESTING")
        logger.info("=" * 60)

        bt = Backtester(sensitivity=sensitivity)
        trades = bt.load_trades_csv(args.backtest)
        logger.info(f"Charge {len(trades)} trades depuis {args.backtest}")

        known_events = None
        if args.events:
            known_events = bt.load_trades_csv(args.events)
            logger.info(f"Charge {len(known_events)} evenements connus depuis {args.events}")

        result = bt.run(trades, known_events)

        logger.info(f"Messages traites : {result['total_messages']}")
        logger.info(f"Alertes generees : {result['total_alerts']}")
        logger.info(f"Temps moyen : {result['avg_exec_time_ms']:.4f} ms")

        if known_events:
            logger.info(f"Precision : {result['precision']:.4f}")
            logger.info(f"Recall    : {result['recall']:.4f}")
            logger.info(f"F1-score  : {result['f1_score']:.4f}")
            logger.info(f"TP={result['true_positives']} FP={result['false_positives']} FN={result['false_negatives']}")

        return

    logger.info("=" * 60)
    logger.info("  DETECTEUR D'ANOMALIES V3")
    logger.info(f"  Sensibilite : {sensitivity}")
    logger.info(f"  Regles : Slippage, Spread, OFI, Depth Erosion, VPIN, Whale")
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
        detector = AnomalyDetectorV3(
            sensitivity=sensitivity,
            mongo_collection=mongo_collection,
        )
        consumer = KafkaConsumer(
            INPUT_TOPIC,
            bootstrap_servers=BOOTSTRAP_SERVERS,
            group_id=GROUP_ID,
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            auto_commit_interval_ms=1000,
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            key_deserializer=lambda k: k.decode("utf-8") if k else None,
        )

        logger.info(f"Ecoute sur '{INPUT_TOPIC}', alertes sur '{ALERT_TOPIC}'")
        logger.info(f"Poids des regles : {RULE_WEIGHTS}")
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
                        logger.info(f"[04_anomalie_v3] {message_count} messages traites ({rate:.1f}/s) | Temps moyen: {avg:.4f} ms")
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
