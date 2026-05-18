"""
Consumer Kafka de test
Consomme les messages du topic test.ingestion
"""
import json
from kafka import KafkaConsumer
from kafka.errors import KafkaError


def create_consumer(topic, group_id='test-consumer-group'):
    """Crée un consumer Kafka"""
    return KafkaConsumer(
        topic,
        bootstrap_servers='localhost:9092',
        group_id=group_id,
        auto_offset_reset='earliest',  # Lit depuis le début du topic
        enable_auto_commit=True,
        auto_commit_interval_ms=1000,
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        key_deserializer=lambda k: k.decode('utf-8') if k else None
    )


def consume_messages(consumer, max_messages=None):
    """Consomme les messages"""
    print(f"👂 En écoute sur le topic '{consumer.subscription()}'...")
    print("Appuyez sur Ctrl+C pour arrêter\n")
    
    message_count = 0
    
    try:
        for message in consumer:
            message_count += 1
            
            print(f"📨 Message {message_count} reçu:")
            print(f"   Topic: {message.topic}")
            print(f"   Partition: {message.partition}")
            print(f"   Offset: {message.offset}")
            print(f"   Key: {message.key}")
            print(f"   Timestamp: {message.timestamp}")
            print(f"   Value: {json.dumps(message.value, indent=2)}")
            print("-" * 60)
            
            # Arrêter après max_messages si spécifié
            if max_messages and message_count >= max_messages:
                print(f"\n✅ {message_count} messages consommés. Arrêt.")
                break
                
    except KeyboardInterrupt:
        print(f"\n⚠️ Interruption utilisateur. {message_count} messages consommés.")
    except KafkaError as e:
        print(f"\n❌ Erreur Kafka: {e}")


def main():
    topic = 'test.ingestion'
    
    try:
        # Créer le consumer
        consumer = create_consumer(topic)
        print(f"✅ Consumer connecté à Kafka (localhost:9092)")
        print(f"✅ Group ID: {consumer.config['group_id']}\n")
        
        # Consommer les messages
        consume_messages(consumer, max_messages=None)  # Mettre un nombre pour limiter
        
    except Exception as e:
        print(f"❌ Erreur: {e}")
    finally:
        # Fermer proprement le consumer
        if 'consumer' in locals():
            consumer.close()
            print("\n🔒 Consumer fermé")


if __name__ == '__main__':
    main()
