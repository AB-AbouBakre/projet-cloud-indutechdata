# Évaluation de compatibilité de l'architecture hybride

## 1. Objectif

L'architecture proposée doit moderniser la gestion des données d'InduTechData sans interrompre les systèmes existants. Elle conserve SQL Server, le SAN, Active Directory, l'ERP et le CRM dans le datacenter, tout en ajoutant des services cloud pour le stockage, le streaming et l'analyse.

## 2. Compatibilité avec le SI existant

| Exigence | Solution proposée | Justification |
|---|---|---|
| Données non structurées | Amazon S3 | Stockage objet adapté aux fichiers, journaux et données IoT brutes. |
| Données analytiques | Amazon Redshift | Entrepôt de données permettant des analyses SQL sur de grands volumes. |
| Streaming en temps réel | Redpanda | Réception et distribution rapide des événements, avec compatibilité Kafka. |
| Traitement | PySpark | Transformation et agrégation distribuées des événements. |
| Interopérabilité SQL Server | Connecteur CDC | Détection des changements SQL Server et publication dans Redpanda. |
| Identités | AD et IAM Identity Center | Conservation des comptes de l'entreprise et attribution centralisée des accès AWS. |
| Connexion hybride | AWS Site-to-Site VPN | Tunnel sécurisé entre le datacenter et le cloud. |

## 3. Sécurité et conformité

- Le VPN sécurise la connexion réseau entre le datacenter et AWS.
- TLS chiffre les flux pendant leur transfert.
- AWS KMS gère les clés de chiffrement des données stockées dans S3 et Redshift.
- IAM applique le principe du moindre privilège : chaque utilisateur ou service reçoit uniquement les droits nécessaires.
- IAM Identity Center permet d'utiliser les identités Active Directory existantes.
- Les journaux d'accès et les alertes doivent être centralisés pour faciliter les audits.
- Les données sensibles doivent être classifiées et conservées pendant une durée conforme aux règles internes et réglementaires.

## 4. Interopérabilité et protocoles

| Source | Destination | Mécanisme ou protocole |
|---|---|---|
| SQL Server | Redpanda | CDC, API Kafka et TLS via VPN |
| Capteurs et applications | Redpanda | API Kafka et TLS via VPN |
| Redpanda | PySpark | Connecteur Kafka et TLS |
| PySpark | Amazon S3 | HTTPS et TLS |
| PySpark | Amazon Redshift | JDBC et TLS |
| SQL Server | Amazon Redshift | ETL ou réplication via VPN et TLS |
| Active Directory | IAM Identity Center | Fédération des identités |

## 5. Automatisation des flux

Le pipeline limite les interventions humaines répétitives :

```text
Modification SQL Server ou nouvel événement IoT
                        ↓
Détection automatique par le connecteur CDC ou le producteur
                        ↓
Publication dans un topic Redpanda
                        ↓
Traitement automatique par PySpark
                        ↓
Export vers Amazon S3 ou chargement dans Amazon Redshift
```

Cette automatisation réduit les risques d'oubli, d'erreur de manipulation et de retard dans la disponibilité des données.

## 6. Scalabilité

- Amazon S3 adapte sa capacité au volume d'objets stockés.
- Redpanda peut répartir les événements entre plusieurs partitions et nœuds.
- PySpark peut distribuer les traitements entre plusieurs exécutants.
- Redshift Serverless adapte la capacité de calcul à la charge analytique.
- L'architecture peut absorber l'augmentation mensuelle de 50 Go sans remplacement complet de l'infrastructure.

Le SAN reste utilisable pour les besoins locaux, mais une extension physique serait nécessaire lorsqu'il atteint sa capacité maximale.

## 7. Surveillance et optimisation des coûts

Les recommandations suivantes sont retenues :

- créer un budget mensuel dans AWS Budgets avec des alertes à 50 %, 80 % et 100 % ;
- analyser les dépenses dans AWS Cost Explorer ;
- appliquer des tags par service, environnement et responsable ;
- utiliser CloudWatch pour repérer les ressources inutilisées ou surdimensionnées ;
- limiter les heures actives de Redshift Serverless ;
- utiliser S3 Lifecycle pour archiver automatiquement les données anciennes ;
- convertir les données analytiques au format Parquet pour réduire le volume lu ;
- réviser l'estimation après un test de charge du POC.

## 8. Hypothèses de l'estimation

Cette estimation constitue un ordre de grandeur pédagogique et non un devis commercial.

- Région de référence : Europe, avec validation finale dans AWS Pricing Calculator pour Paris.
- Volume initial S3 : 10 To, soit environ 10 240 Go.
- Croissance : 50 Go par mois.
- Sous-ensemble analytique initial dans Redshift : 1 To.
- Redshift Serverless : 2 heures d'activité par jour, 30 jours par mois.
- Une connexion VPN active en permanence.
- Deux clés KMS gérées par le client.
- Petit environnement de test pour Redpanda et PySpark.
- Taxes, support premium, licences externes et transferts sortants importants exclus.

## 9. Estimation des coûts initiaux

Hypothèse de valorisation : 500 € par jour de travail.

| Activité initiale | Effort estimé | Coût estimé |
|---|---:|---:|
| Conception détaillée de l'architecture | 3 jours | 1 500 € |
| Configuration des services et du VPN | 5 jours | 2 500 € |
| Migration pilote et configuration CDC | 4 jours | 2 000 € |
| Tests fonctionnels et tests de sécurité | 2 jours | 1 000 € |
| Documentation et formation | 1 jour | 500 € |
| **Total initial estimé** | **15 jours** | **7 500 €** |

Ce total représente principalement le travail de mise en place. Le coût réel dépendra des tarifs internes, de la complexité de la migration et des éventuelles prestations externes.

## 10. Estimation des coûts récurrents

| Poste mensuel | Hypothèse | Estimation mensuelle |
|---|---|---:|
| Amazon S3 | 10 240 Go en stockage standard | 236 à 256 $ |
| Redshift Serverless - calcul | 2 h/jour × 30 jours × tarif de départ de 1,50 $/h | environ 90 $ |
| Stockage géré Redshift | Sous-ensemble analytique de 1 To | environ 24 $ |
| AWS Site-to-Site VPN | Une connexion active en continu | environ 36 $ |
| AWS KMS | Deux clés et un volume modéré de requêtes | 2 à 7 $ |
| Calcul Redpanda et PySpark | Petit environnement de test | 100 à 300 $ |
| Requêtes, journaux et supervision | Faible activité initiale | 10 à 30 $ |
| **Total mensuel indicatif** | Hors taxes et transferts sortants importants | **environ 500 à 740 $** |

La ligne S3 repose sur une hypothèse de 0,023 à 0,025 $ par Go-mois. Le coût Redpanda/PySpark est une réserve budgétaire à remplacer par un dimensionnement mesuré. Tous les tarifs doivent être vérifiés dans la région choisie avant une décision de production.

## 11. Sensibilité des coûts

- Chaque tranche supplémentaire de 50 Go dans S3 ajoute approximativement 1,15 à 1,25 $ par mois avant archivage.
- Redshift utilisé 24 heures par jour coûterait beaucoup plus cher qu'une utilisation limitée à quelques heures.
- Les transferts de données sortants peuvent devenir importants si de grands volumes quittent AWS.
- L'archivage, l'arrêt des ressources inutilisées et les formats compressés permettent de limiter la facture.

## 12. Conclusion

L'architecture est compatible avec le SI existant grâce au VPN, à la fédération des identités, au CDC et aux protocoles standards. Elle améliore la capacité de stockage, automatise les flux et permet de faire évoluer les ressources selon la charge. Les coûts restent pilotables à condition d'utiliser des budgets, des alertes, des règles d'archivage et un dimensionnement fondé sur des mesures réelles.

## Sources tarifaires

- [Tarification Amazon S3](https://aws.amazon.com/s3/pricing/)
- [Tarification Amazon Redshift](https://aws.amazon.com/redshift/pricing/)
- [Tarification AWS Site-to-Site VPN](https://aws.amazon.com/vpn/pricing/)
- [Tarification AWS KMS](https://aws.amazon.com/kms/pricing/)
- [AWS Pricing Calculator](https://calculator.aws/)

