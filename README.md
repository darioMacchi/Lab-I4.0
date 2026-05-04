# 🚗 Automatic Vehicle Monitoring (AVM)
Progetto universitario per la realizzazione di un sistema di telemetria dedicato al raccoglimento dati da veicoli, in particolare autobus. Lo stack prevede la simulazione dei veicoli, e la comunicazione attraverso protocollo MQTT ad un sistema che comprende: Ingestion, Processing, Storage, Visualization.

## 📌 Overview
Questo progetto implementa un sistema di vehicle monitoring che consente di:
  - 📍 Tracciare la posizione dei veicoli.
  - 📊 Analizzare dati di utilizzo e performance.
  - ⚙️ Monitorare parametri tecnici: velocità, pressione degli pneumatici, stato del motore, stato dell'impianto frenante, informazioni sui consumi, dati ambientali.
  - 🧠 Preparare i dati per applicazioni di analytics o machine learning.

Il sistema si ispira ai moderni approcci di fleet management e telemetria, comunemente utilizzati nel trasporto e nella mobilità intelligente.

## 🏗️ Architettura
Il progetto è strutturato in diversi moduli:
  - Data Collection → acquisizione dati dai sensori.
  - Processing Layer → aggregazione e analisi dei dati.
  - Storage → database.
  - Visualization → esposizione dei dati tramite dashboard.

```
Vehicle → Data Collector → Processing → Storage → Dashboard
```

## ⚙️ Tecnologie utilizzate
  - Linguaggio →  Python
  - Data Collection / Ingestion → Apache Kafka.
  - Processing Layer → Apache Flink.
  - Storage →  MongoDB.
  - Visualization → Grafana.
  - Comunicazione / Messaging → MQTT.
