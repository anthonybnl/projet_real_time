# Kafka Topics

Voici les commandes pour créer et supprimer le topic utilisé dans les tests :

```bash
docker exec -it kafka kafka-topics --create \
  --bootstrap-server localhost:9092 \
  --topic coinbase.btc.usd.trades \
  --partitions 3 \
  --replication-factor 1
```

```bash
docker exec -it kafka kafka-topics --delete \
  --bootstrap-server localhost:9092 \
  --topic coinbase.btc.usd.trades
```
