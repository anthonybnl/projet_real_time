"""
Tests unitaires et d'intégration pour le moteur de détection d'anomalies financières
"""

import sys
import os
import unittest
import time
import math
from unittest.mock import MagicMock

# Ajouter le chemin du projet au PYTHONPATH
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from consumers.04_detection_anomalie import AnomalyDetector


class TestAnomalyDetector(unittest.TestCase):
    def setUp(self):
        # Désactiver les logs pendant les tests pour éviter d'inonder la console
        import logging
        logging.disable(logging.CRITICAL)

        # Initialiser le détecteur avec un mock du KafkaProducer
        self.patcher = unittest.mock.patch('consumers.detection_anomalie.KafkaProducer')
        self.mock_producer_class = self.patcher.start()
        self.mock_producer = MagicMock()
        self.mock_producer_class.return_value = self.mock_producer

        self.detector = AnomalyDetector(
            bootstrap_servers="mock:9092",
            alert_topic="mock.anomalies",
            raise_on_anomaly=False
        )
        self.detector.producer = self.mock_producer

    def tearDown(self):
        import logging
        logging.disable(logging.NOTSET)
        self.patcher.stop()

    def test_slippage_anormal(self):
        """Teste l'anomalie de Slippage Anormal (Z-Score glissant > 4.0 sur log-returns)"""
        # Alimenter avec un prix stable pour remplir la fenêtre
        base_price = 50000.0
        for i in range(30):
            msg = {
                "product_id": "BTC-USD",
                "price": base_price,
                "trade_size": 1.0,
                "sequence": 1000 + i,
                "time": time.time()
            }
            self.detector.process_message(msg)

        # Vérifier qu'aucune alerte n'est encore envoyée
        self.mock_producer.send.assert_not_called()

        # Produire un slippage majeur (Z-score élevé)
        slippage_msg = {
            "product_id": "BTC-USD",
            "price": 30000.0,  # Chute brutale
            "trade_size": 1.0,
            "sequence": 1015,
            "time": time.time()
        }
        self.detector.process_message(slippage_msg)

        # Vérifier l'alerte
        alert_calls = [
            call for call in self.mock_producer.send.call_args_list
            if call[1]['value']['anomaly_type'] == "SLIPPAGE_ANORMAL"
        ]
        self.assertTrue(len(alert_calls) > 0, "L'anomalie de Slippage Anormal n'a pas été détectée")
        
        alert = alert_calls[0][1]['value']
        self.assertEqual(alert['symbol'], "BTC-USD")
        self.assertTrue(abs(alert['details']['z_score']) > 4.0)

    def test_spread_elastique(self):
        """Teste l'anomalie de Spread Élastique (écart Ask-Bid qui double sa moyenne)"""
        # Alimenter avec un spread stable de 0.05
        for i in range(10):
            msg = {
                "product_id": "BTC-USD",
                "price": 50000.0,
                "best_bid": 49999.97,
                "best_ask": 50000.02,  # spread = 0.05
                "trade_size": 1.0,
                "sequence": 2000 + i,
                "time": time.time()
            }
            self.detector.process_message(msg)

        self.mock_producer.send.reset_mock()

        # Envoyer un écart doublé (ex: 0.20)
        double_spread_msg = {
            "product_id": "BTC-USD",
            "price": 50000.0,
            "best_bid": 49999.85,
            "best_ask": 50000.05,  # spread = 0.20 (> 2 * 0.05)
            "trade_size": 1.0,
            "sequence": 2010,
            "time": time.time()
        }
        self.detector.process_message(double_spread_msg)

        alert_calls = [
            call for call in self.mock_producer.send.call_args_list
            if call[1]['value']['anomaly_type'] == "SPREAD_ELASTIQUE"
        ]
        self.assertTrue(len(alert_calls) > 0, "L'anomalie de Spread Élastique n'a pas été détectée")
        
        alert = alert_calls[0][1]['value']
        self.assertTrue(alert['details']['ratio'] > 2.0)

    def test_order_flow_imbalance(self):
        """Teste l'anomalie de déséquilibre des flux d'ordres (OFI)"""
        # Alimenter avec des flux équilibrés
        for i in range(5):
            msg_buy = {
                "product_id": "BTC-USD",
                "price": 50000.0,
                "trade_size": 1.0,
                "side": "buy",
                "sequence": 3000 + i,
                "time": time.time()
            }
            msg_sell = {
                "product_id": "BTC-USD",
                "price": 50000.0,
                "trade_size": 1.0,
                "side": "sell",
                "sequence": 3100 + i,
                "time": time.time()
            }
            self.detector.process_message(msg_buy)
            self.detector.process_message(msg_sell)

        self.mock_producer.send.reset_mock()

        # Envoyer un gros trade acheteur provoquant un déséquilibre fort (> 0.85)
        imbalance_msg = {
            "product_id": "BTC-USD",
            "price": 50000.0,
            "trade_size": 100.0,  # volume d'achat massif
            "side": "buy",
            "sequence": 3200,
            "time": time.time()
        }
        self.detector.process_message(imbalance_msg)

        alert_calls = [
            call for call in self.mock_producer.send.call_args_list
            if call[1]['value']['anomaly_type'] == "ORDER_FLOW_IMBALANCE"
        ]
        self.assertTrue(len(alert_calls) > 0, "L'anomalie d'Order Flow Imbalance n'a pas été détectée")
        
        alert = alert_calls[0][1]['value']
        self.assertTrue(abs(alert['details']['ofi_ratio']) > 0.85)

    def test_execution_time(self):
        """Vérifie que le temps d'exécution par message est inférieur à 0.5ms (500 microsecondes)"""
        msg = {
            "product_id": "BTC-USD",
            "price": 50000.0,
            "best_bid": 49999.97,
            "best_ask": 50000.02,
            "trade_size": 1.0,
            "side": "buy",
            "sequence": 7000,
            "time": time.time()
        }
        
        # Chauffer
        for i in range(100):
            msg["sequence"] = 7000 + i
            self.detector.process_message(msg)
            
        # Mesurer
        start_time = time.perf_counter()
        for i in range(1000):
            msg["sequence"] = 8000 + i
            msg["price"] = 50000.0 + (i % 10)
            msg["best_bid"] = msg["price"] - 0.02
            msg["best_ask"] = msg["price"] + 0.03
            msg["side"] = "buy" if i % 2 == 0 else "sell"
            self.detector.process_message(msg)
        end_time = time.perf_counter()
        
        duration_per_msg_ms = ((end_time - start_time) / 1000.0) * 1000.0
        print(f"\n[PERFORMANCE] Temps de traitement moyen mesuré : {duration_per_msg_ms:.4f} ms par message.")
        self.assertTrue(duration_per_msg_ms < 0.5, f"Le temps d'exécution moyen est de {duration_per_msg_ms:.4f} ms, ce qui dépasse le seuil de 0.5ms")


def run_simulation(bootstrap_servers="127.0.0.1:9092", topic="btc.cleaned"):
    from datetime import datetime, timezone
    import json
    from kafka import KafkaProducer
    print("=" * 70)
    print("   DEMARRAGE DE LA SIMULATION DES NOUVELLES ANOMALIES FINANCIERES")
    print("   Envoi de flux vers le topic Kafka : 'btc.cleaned'")
    print("   Le detecteur en arriere-plan interceptera ces anomalies !")
    print("   (Pressez Ctrl+C pour arreter la simulation)")
    print("=" * 70)
    
    try:
        producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8")
        )
    except Exception as e:
        print(f"[ERREUR] Impossible de se connecter a Kafka a l'adresse {bootstrap_servers} : {e}")
        print("Vérifiez que Docker Desktop et le service Kafka sont lancés.")
        return

    base_price = 50000.0
    seq = 1000000
    message_count = 0
    
    try:
        while True:
            message_count += 1
            seq += 1
            now = time.time()
            
            # Message de base stable
            price = base_price + (message_count % 5)
            bid = price - 0.02
            ask = price + 0.03
            volume = 1.0
            side = "buy" if message_count % 2 == 0 else "sell"
            
            msg = {
                "product_id": "BTC-USD",
                "price": price,
                "best_bid": bid,
                "best_ask": ask,
                "trade_size": volume,
                "side": side,
                "sequence": seq,
                "time": datetime.now(timezone.utc).isoformat()
            }
            
            cycle = message_count % 15
            
            if cycle == 3:
                # 1. SLIPPAGE ANORMAL (Crash de prix Z-Score > 4.0)
                msg["price"] = base_price * 0.50
                msg["best_bid"] = msg["price"] - 0.02
                msg["best_ask"] = msg["price"] + 0.03
                print(f"[SIMULATEUR] >>> Envoi SLIPPAGE ANORMAL (Prix: {price} -> {msg['price']:.2f})")
                
            elif cycle == 6:
                # 2. SPREAD ELASTIQUE (écart BestAsk - BestBid qui double, ex: 10.0 au lieu de 0.05)
                msg["best_bid"] = price - 5.0
                msg["best_ask"] = price + 5.0
                print(f"[SIMULATEUR] >>> Envoi SPREAD ELASTIQUE (Spread: 0.05 -> 10.00)")
                
            elif cycle == 9:
                # 3. ORDER FLOW IMBALANCE (Envoi de trades uniquement acheteurs massifs en 10s)
                msg["trade_size"] = 150.0
                msg["side"] = "buy"
                print(f"[SIMULATEUR] >>> Envoi ORDER FLOW IMBALANCE (Achat de volume: {msg['trade_size']})")
                
            producer.send(topic, value=msg)
            
            if cycle not in [3, 6, 9]:
                print(f"[SIMULATEUR] Message normal #{message_count} envoye (Prix: {price:.2f}, Spread: {ask-bid:.2f}, Side: {side})")
                
            time.sleep(1.0)
            
    except KeyboardInterrupt:
        print("\n[SIMULATEUR] Simulation arretée par l'utilisateur.")
    finally:
        producer.close()


if __name__ == '__main__':
    if len(sys.argv) > 1 and sys.argv[1] in ["--simulate", "--sim"]:
        run_simulation()
    else:
        unittest.main()
