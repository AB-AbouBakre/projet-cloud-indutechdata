## Architecture hybride InduTechData

```mermaid
flowchart LR
    subgraph ONPREM["Datacenter on-premise"]
        SQL["SQL Server - 40 To - ERP et CRM"]
        SAN["SAN - 10 To - fichiers et logs"]
        AD["Active Directory"]
        IOT["Applications et capteurs IoT"]
        CDC["Connecteur CDC"]
    end

    subgraph AWS["Cloud AWS"]
        RP["Redpanda - Streaming"]
        SPARK["PySpark - Traitement"]
        S3["Amazon S3 - Stockage objet"]
        RS["Amazon Redshift - Entrepôt de données"]
        IAM["IAM Identity Center - Gestion des accès"]
        KMS["AWS KMS - Chiffrement au repos"]
    end

    SQL -->|"Changements"| CDC
    CDC -->|"Kafka et TLS via VPN"| RP
    IOT -->|"Kafka et TLS via VPN"| RP
    RP -->|"Flux d'événements"| SPARK
    SPARK -->|"HTTPS et TLS"| S3
    SPARK -->|"JDBC et TLS"| RS
    SAN -->|"HTTPS et TLS via VPN"| S3
    SQL -->|"ETL via VPN"| RS
    AD -->|"Fédération des identités"| IAM
    KMS -.->|"Chiffrement"| S3
    KMS -.->|"Chiffrement"| RS
```

### Rôle des composants

| Composant | Fonction principale |
|---|---|
| SQL Server | Héberge les données critiques des applications ERP et CRM. |
| SAN | Stocke les fichiers, journaux système et données non structurées existantes. |
| Connecteur CDC | Détecte les changements dans SQL Server et les publie sous forme d'événements. |
| Redpanda | Transporte les événements en temps réel entre producteurs et consommateurs. |
| PySpark | Transforme, filtre et agrège les données. |
| Amazon S3 | Conserve durablement les données brutes ou transformées. |
| Amazon Redshift | Analyse de grandes quantités de données avec SQL. |
| IAM Identity Center | Centralise les accès AWS en lien avec Active Directory. |
| AWS KMS | Gère les clés utilisées pour chiffrer les données stockées. |

### Sécurité et protocoles

- Les communications entre le datacenter et AWS passent par un VPN sécurisé.
- TLS chiffre les données pendant leur transfert.
- AWS KMS protège les données stockées dans Amazon S3 et Amazon Redshift.
- IAM Identity Center permet une gestion homogène des identités avec Active Directory.
- Le protocole Kafka est utilisé pour les flux Redpanda.
- HTTPS est utilisé pour les transferts vers Amazon S3.
- JDBC est utilisé pour les échanges avec Amazon Redshift.

