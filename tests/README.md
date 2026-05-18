# Scripts de Test Kafka

## Installation

```bash
pip install -r ../requirements.txt
```

## Créer le topic (optionnel mais recommandé)

Kafka peut créer automatiquement les topics, mais c'est mieux de les créer manuellement avec les bons paramètres :

```bash
# Créer le topic avec 3 partitions
docker exec -it kafka kafka-topics --create \
  --bootstrap-server localhost:9092 \
  --topic test.ingestion \
  --partitions 3 \
  --replication-factor 1

# Vérifier que le topic existe
docker exec -it kafka kafka-topics --list \
  --bootstrap-server localhost:9092

# Voir les détails du topic
docker exec -it kafka kafka-topics --describe \
  --bootstrap-server localhost:9092 \
  --topic test.ingestion
```

## Utilisation

### 1. Lancer le Consumer (terminal 1)

```bash
cd tests
python consumer.py
```

### 2. Lancer le Producer (terminal 2)

```bash
cd tests
python producer.py
```

## Résultat attendu

Le producer envoie 10 messages et le consumer les affiche en temps réel.

## Commandes utiles

```bash
# Supprimer le topic
docker exec -it kafka kafka-topics --delete \
  --bootstrap-server localhost:9092 \
  --topic test.ingestion

# Voir les consumer groups
docker exec -it kafka kafka-consumer-groups --list \
  --bootstrap-server localhost:9092

# Voir les offsets d'un consumer group
docker exec -it kafka kafka-consumer-groups --describe \
  --bootstrap-server localhost:9092 \
  --group test-consumer-group
```
