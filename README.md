# Projet Real-Time Data Engineering

Projet M1 Data Engineering & IA - Plateforme de traitement de données temps réel avec Kafka, MongoDB et API.

## 🏗️ Architecture

```
┌─────────────────┐
│   API externe   │ (source de données temps réel)
└────────┬────────┘
         │
    ┌────▼────────────┐
    │   Ingestion     │ (Producer Kafka)
    └────┬────────────┘
         │
    ┌────▼──────┐
    │   Kafka   │ (Message Broker)
    └─┬──┬──┬───┘
      │  │  │
   ┌──▼─ ▼─ ▼──────┐
   │  3 Consumers   │ (Traitement des données)
   └──────┬─────────┘
          │
     ┌────▼─────┐
     │ MongoDB  │ (Base de données)
     └────┬─────┘
          │
     ┌────▼─────┐
     │   API    │ (Backend)
     └────┬─────┘
          │
     ┌────▼─────┐
     │ Frontend │
     └──────────┘
```

## 📁 Structure du projet

```
projet_real_time/
├── compose.yml              # Docker Compose (Kafka + Kafka UI)
├── requirements.txt         # Dépendances Python
│
├── tests/                   # ✅ Scripts de test Kafka
│   ├── producer.py
│   ├── consumer.py
│   └── README.md
│
├── ingestion/              # TODO: Service d'ingestion depuis API
│   ├── producer.py
│   ├── config.py
│   └── requirements.txt
│
├── consumers/              # TODO: Les 3 consumers
│   ├── consumer_1.py
│   ├── consumer_2.py
│   ├── consumer_3.py
│   └── requirements.txt
│
├── api/                    # TODO: Backend API (FastAPI)
│   ├── main.py
│   ├── models.py
│   ├── routes/
│   └── requirements.txt
│
└── frontend/               # TODO: Frontend (React/Vue)
    └── ...
```

## 🚀 Démarrage

### 1. Lancer Kafka

```bash
docker compose up -d
```

- Kafka UI : http://localhost:8080
- Kafka Broker : localhost:9092

### 2. Tester Kafka

Voir [tests/README.md](tests/README.md)

```bash
# Installer les dépendances
pip install -r requirements.txt

# Créer le topic (recommandé)
docker exec -it kafka kafka-topics --create \
  --bootstrap-server localhost:9092 \
  --topic test.ingestion \
  --partitions 3 \
  --replication-factor 1

# Lancer les scripts de test
cd tests
python consumer.py  # Terminal 1
python producer.py  # Terminal 2
```

## 📝 Topics Kafka

- `test.ingestion` - Topic de test
- TODO: Définir les topics de production

## 🔧 Configuration Kafka

- 1 broker (KRaft mode, sans Zookeeper)
- 2 listeners :
  - `localhost:9092` : pour connexion depuis l'hôte
  - `kafka:19092` : pour connexion depuis conteneurs Docker
- Replication factor : 1 (dev mode)
- Log retention : 24h

## 📚 TODO

- [ ] Service d'ingestion temps réel
- [ ] Implémenter les 3 consumers
- [ ] Configurer MongoDB
- [ ] Créer l'API backend
- [ ] Développer le frontend
