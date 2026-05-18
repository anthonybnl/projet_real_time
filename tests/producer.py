"""
Producer Kafka de test
Envoie des messages sur le topic test.ingestion
"""
import json
import time
from datetime import datetime
from kafka import KafkaProducer
from kafka.errors import KafkaError


def create_producer():
    """Crée un producer Kafka"""
    return KafkaProducer(
        bootstrap_servers='localhost:9092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        key_serializer=lambda k: k.encode('utf-8') if k else None,
        acks='all',  # Attend la confirmation de tous les replicas
        retries=3,
        max_in_flight_requests_per_connection=1
    )


def send_messages(producer, topic, num_messages=10):
    """Envoie des messages de test"""
    print(f"🚀 Envoi de {num_messages} messages sur le topic '{topic}'...")
    
    for i in range(num_messages):
        message = {
            'id': i + 1,
            'timestamp': datetime.utcnow().isoformat(),
            'message': f'Test message #{i + 1}',
            'data': {
                'value': i * 10,
                'status': 'active'
            }
        }
        
        key = f'key-{i + 1}'
        
        try:
            # Envoi asynchrone avec callback
            future = producer.send(topic, key=key, value=message)
            
            # Bloque jusqu'à l'envoi (pour le test)
            record_metadata = future.get(timeout=10)
            
            print(f"✅ Message {i + 1} envoyé - "
                  f"Partition: {record_metadata.partition}, "
                  f"Offset: {record_metadata.offset}")
            
            time.sleep(0.5)  # Pause entre les messages
            
        except KafkaError as e:
            print(f"❌ Erreur lors de l'envoi du message {i + 1}: {e}")
    
    # Flush pour s'assurer que tous les messages sont envoyés
    producer.flush()
    print("\n✅ Tous les messages ont été envoyés avec succès!")


def main():
    topic = 'test.ingestion'
    
    try:
        # Créer le producer
        producer = create_producer()
        print(f"✅ Producer connecté à Kafka (localhost:9092)")
        
        # Envoyer des messages
        send_messages(producer, topic, num_messages=10)
        
    except Exception as e:
        print(f"❌ Erreur: {e}")
    finally:
        # Fermer proprement le producer
        if 'producer' in locals():
            producer.close()
            print("\n🔒 Producer fermé")


if __name__ == '__main__':
    main()
