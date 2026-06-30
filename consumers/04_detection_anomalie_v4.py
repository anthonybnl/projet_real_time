"""
Détecteur d'anomalies financières V4.
Optimisation algorithmique O(1) et O(log N). Fusion des règles V2 et V3.
Dépendances requises: pip install kafka-python pymongo numpy sortedcontainers orjson python-dotenv
"""
import os, time, math, logging, argparse, orjson
from collections import deque
from datetime import datetime, timezone
from dotenv import load_dotenv
from sortedcontainers import SortedList
from pymongo import MongoClient
from kafka import KafkaConsumer, KafkaProducer

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("AnomalyDetectorV4")

SENSITIVITY_MAP = {
    "low":    {"z_pct": 99.5, "spread_pct": 99, "ofi_thresh": 0.92, "combo_req": 3, "combo_win": 60},
    "medium": {"z_pct": 99,   "spread_pct": 97, "ofi_thresh": 0.85, "combo_req": 2, "combo_win": 45},
    "high":   {"z_pct": 95,   "spread_pct": 95, "ofi_thresh": 0.78, "combo_req": 2, "combo_win": 30},
}

RULE_WEIGHTS = {"SLIPPAGE": 1.0, "SPREAD": 0.8, "OFI": 1.0, "DEPTH_EROSION": 1.2, "VPIN": 1.5, "WHALE": 0.7}

# Le .env peut contenir l'adresse interne Docker (19092), mais cote hote c'est 9092
BOOTSTRAP_SERVERS = "127.0.0.1:9092"

class AnomalyDetectorV4:
    def __init__(self, sensitivity="medium", dry_run=False):
        self.params = SENSITIVITY_MAP[sensitivity]
        self.sensitivity = sensitivity
        self.dry_run = dry_run
        self.alert_log = []

        self.producer = None
        if not dry_run:
            self.producer = KafkaProducer(
                bootstrap_servers=BOOTSTRAP_SERVERS,
                value_serializer=lambda v: orjson.dumps(v),
                acks="all",
                retries=3
            )

        self.mongo = None
        if not dry_run:
            try:
                self.mongo = MongoClient(os.environ["MONGODB_URI"])[os.environ["MONGODB_DBNAME"]]["btc_anomalies"]
            except Exception as e:
                logger.error(f"Erreur connexion MongoDB: {e}")

        # État optimisé (O(1) pour la variance du slippage)
        self.state = {}
        
    def _init_symbol_state(self):
        return {
            "count": 0, "prev_price": None,
            "log_rets": deque(maxlen=300), "ret_sum": 0.0, "ret_sq_sum": 0.0,
            "spreads": deque(), "spread_sum": 0.0,
            "ofi_trades": deque(),
            "depths": deque(maxlen=60), "depth_sum": 0.0,
            "vpin_acc": {"buy": 0.0, "sell": 0.0, "total": 0.0}, "vpin_buckets": deque(maxlen=50),
            "trade_sizes": deque(maxlen=1000), "sorted_sizes": SortedList(),
            "signals": deque(), "last_alerts": {}
        }

    def _trigger_alert(self, key, anomaly_type, details, msg):
        now = time.time()
        if now - key["last_alerts"].get(anomaly_type, 0) < 30:
            return
        key["last_alerts"][anomaly_type] = now

        alert = {
            "anomaly_type": anomaly_type,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "exchange": details.pop("exchange"), "symbol": details.pop("symbol"),
            "details": details, "trigger_message": msg
        }
        logger.warning(f"[!] {anomaly_type} | {alert['exchange']}-{alert['symbol']} | {details.get('description')}")
        
        if self.producer is not None:
            self.producer.send("financial.alerts", value=alert)
        if self.mongo is not None:
            alert["timestamp"] = datetime.now(timezone.utc)
            self.mongo.insert_one(alert)

    def process_message(self, msg):
        start_ns = time.perf_counter_ns()
        now = time.time()
        
        exc, sym = msg.get("exchange", "binance").lower(), msg.get("symbol", "BTC-USD").upper()
        state_key = f"{exc}_{sym}"
        if state_key not in self.state: self.state[state_key] = self._init_symbol_state()
        st = self.state[state_key]
        st["count"] += 1
        warmed_up = st["count"] >= 300

        price = float(msg.get("price") or msg.get("p") or 0)
        vol = float(msg.get("trade_size") or msg.get("volume") or msg.get("q") or msg.get("size") or 0)

        raw_side = msg.get("side")
        if raw_side and raw_side.lower() in ("buy", "sell"):
            side = raw_side.lower()
        elif "m" in msg:
            side = "sell" if msg["m"] else "buy"
        else:
            side = None

        # 1. SLIPPAGE (O(1) Variance)
        if price > 0 and st["prev_price"]:
            log_ret = math.log(price / st["prev_price"])
            old_ret = st["log_rets"].popleft() if len(st["log_rets"]) == 300 else 0.0
            st["log_rets"].append(log_ret)
            
            st["ret_sum"] += log_ret - old_ret
            st["ret_sq_sum"] += (log_ret**2) - (old_ret**2)
            
            n = len(st["log_rets"])
            if warmed_up and n >= 30:
                mean = st["ret_sum"] / n
                var = max(0, (st["ret_sq_sum"] / n) - (mean**2))
                std = math.sqrt(var)
                z_score = (log_ret - mean) / std if std > 1e-9 else 0
                
                if abs(z_score) >= 2.0:
                    st["signals"].append((now, "SLIPPAGE", abs(z_score)/4.0))
                    self._trigger_alert(st, "SLIPPAGE_ANORMAL", {"exchange": exc, "symbol": sym, "z_score": z_score, "description": f"Z-score: {z_score:.2f}"}, msg)
        st["prev_price"] = price if price > 0 else st["prev_price"]

        # 2. WHALE ALERT (O(log N) Percentile via SortedList)
        if vol > 0:
            if len(st["trade_sizes"]) == 1000:
                st["sorted_sizes"].remove(st["trade_sizes"].popleft())
            st["trade_sizes"].append(vol)
            st["sorted_sizes"].add(vol)
            
            if warmed_up and len(st["sorted_sizes"]) >= 100:
                idx = int(len(st["sorted_sizes"]) * 0.995)
                thresh = st["sorted_sizes"][idx]
                if vol > thresh > 0:
                    st["signals"].append((now, "WHALE", vol/thresh))
                    self._trigger_alert(st, "WHALE_ALERT", {"exchange": exc, "symbol": sym, "description": f"Volume massif: {vol:.4f} > seuil {thresh:.4f}"}, msg)

        # 3. VPIN Simplifié
        if vol > 0 and side in ("buy", "sell"):
            st["vpin_acc"][side] += vol
            st["vpin_acc"]["total"] += vol
            while st["vpin_acc"]["total"] >= 0.5:
                ratio = 0.5 / st["vpin_acc"]["total"]
                st["vpin_buckets"].append((st["vpin_acc"]["buy"] * ratio, st["vpin_acc"]["sell"] * ratio))
                st["vpin_acc"]["buy"] *= (1 - ratio)
                st["vpin_acc"]["sell"] *= (1 - ratio)
                st["vpin_acc"]["total"] -= 0.5
            
            if warmed_up and len(st["vpin_buckets"]) >= 10:
                tb, ts = sum(b for b,s in st["vpin_buckets"]), sum(s for b,s in st["vpin_buckets"])
                vpin = abs(tb - ts) / (tb + ts) if (tb + ts) > 0 else 0
                if vpin > 0.7:
                    st["signals"].append((now, "VPIN", vpin/0.7))
                    self._trigger_alert(st, "VPIN", {"exchange": exc, "symbol": sym, "description": f"VPIN élevé: {vpin:.2f}"}, msg)

        # Combo Cleanup & Évaluation
        while st["signals"] and st["signals"][0][0] < now - self.params["combo_win"]:
            st["signals"].popleft()
            
        if warmed_up and len(st["signals"]) >= self.params["combo_req"]:
            active_rules = {}
            for _, r, inten in st["signals"]:
                active_rules[r] = max(active_rules.get(r, 0), inten)
            
            if len(active_rules) >= self.params["combo_req"]:
                score = sum(RULE_WEIGHTS.get(r, 1.0) * i for r, i in active_rules.items())
                self._trigger_alert(st, "COMBINED_ANOMALY", {"exchange": exc, "symbol": sym, "score": score, "rules": list(active_rules.keys()), "description": f"Combo! Score: {score:.2f}"}, msg)

        return (time.perf_counter_ns() - start_ns) / 1_000_000.0

    def close(self):
        if self.producer is not None:
            self.producer.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Detecteur d'anomalies V4")
    parser.add_argument("--sensitivity", choices=["low", "medium", "high"],
                        default=os.getenv("ANOMALY_SENSITIVITY", "medium"))
    args = parser.parse_args()

    logger.info("=" * 60)
    logger.info("  DETECTEUR D'ANOMALIES V4")
    logger.info(f"  Sensibilite : {args.sensitivity}")
    logger.info("=" * 60)

    detector = AnomalyDetectorV4(sensitivity=args.sensitivity)
    consumer = KafkaConsumer(
        "btc.cleaned",
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id="financial-anomaly-detector-v4-group",
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda m: orjson.loads(m),
    )

    logger.info(f"Ecoute sur 'btc.cleaned', alertes sur 'financial.alerts'")
    logger.info("En attente de messages...\n")

    message_count = 0
    total_exec_time = 0.0
    window_count = 0
    window_exec_time = 0.0
    window_start = time.time()
    try:
        for msg in consumer:
            message_count += 1
            window_count += 1
            exec_time = detector.process_message(msg.value)
            total_exec_time += exec_time
            window_exec_time += exec_time

            elapsed = time.time() - window_start
            if elapsed >= 10:
                rate = window_count / elapsed
                avg = window_exec_time / window_count if window_count else 0
                logger.info(f"[04_anomalie_v4] {message_count} messages traites ({rate:.1f}/s) | Temps moyen: {avg:.4f} ms")
                window_count = 0
                window_exec_time = 0.0
                window_start = time.time()
    except KeyboardInterrupt:
        logger.info(f"\nArret. {message_count} messages traites.")
    finally:
        consumer.close()
        detector.close()
        logger.info("Connexions fermees.")