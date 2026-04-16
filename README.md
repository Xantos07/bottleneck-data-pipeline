<div align="center">

# 🍷 Bottleneck Data Pipeline

Pipeline de données orchestré pour l'analyse des ventes de vins, couvrant l'ingestion de fichiers Excel bruts jusqu'à l'export de rapports consolidés (chiffre d'affaires, classification produits, qualité des données).

[![Python](https://img.shields.io/badge/Python-3.x-3776AB?style=for-the-badge&logo=python&logoColor=white)](https://www.python.org/)
[![Kestra](https://img.shields.io/badge/Kestra-Orchestration-6650a4?style=for-the-badge&logo=kestra&logoColor=white)](https://kestra.io)
[![DuckDB](https://img.shields.io/badge/DuckDB-Analytical_DB-FFF000?style=for-the-badge&logo=duckdb&logoColor=black)](https://duckdb.org/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16-4169E1?style=for-the-badge&logo=postgresql&logoColor=white)](https://www.postgresql.org/)
[![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?style=for-the-badge&logo=docker&logoColor=white)](https://www.docker.com/)

</div>

---

## Contexte métier

L'entreprise Bottleneck gère un catalogue de vins. Les données sont fragmentées dans trois sources hétérogènes :

| Source | Fichier | Contenu |
|---|---|---|
| ERP | `Fichier_erp.xlsx` | Produits, prix unitaires |
| Liaison | `fichier_liaison.xlsx` | Table de correspondance ERP ↔ Web |
| Web | `Fichier_web.xlsx` | Ventes en ligne, titres produits |

Le pipeline automatise la consolidation de ces sources, le nettoyage des données (doublons, valeurs nulles), et la production d'indicateurs métier.

---

## Stack technique

| Couche | Technologie | Rôle |
|---|---|---|
| **Orchestration** | [Kestra](https://kestra.io) | Scheduling, DAG de flows, triggers |
| **Traitement** | Python 3 | Scripts de transformation et analyse |
| **Stockage intermédiaire** | Apache Parquet | Format colonnaire pour transit entre flows |
| **Base de données analytique** | DuckDB | Jointures SQL en mémoire, requêtes analytiques |
| **Conteneurisation** | Docker / Docker Compose | Environnement reproductible |
| **Base de données Kestra** | PostgreSQL 16 | Métadonnées Kestra (exécutions, flows, logs) |

---

## Architecture du pipeline

Le pipeline est découpé en **flows Kestra chainés par triggers** (chaque flow se déclenche sur le succès du précédent).

```
monthly_trigger (cron: 15 du mois à 9h)
        │
        ▼
init-bottleneck-data-pipeline
  └── pip install pandas openpyxl duckdb pyarrow
        │
        ▼
import_excel_data_new
  └── Excel (ERP + Liaison + Web) → Parquet
  └── outputs: erp_table.parquet, liaison_table.parquet, web_table.parquet
        │
        ▼
analyse_data
  ├── create_db_from_parquet.py → database.duckdb
  └── [PARALLEL]
       ├── analyse_doublons.py
       └── analyse_valeurs_null.py
  └── output: database.duckdb
        │
        ▼
delete_data
  ├── delete_doublons.py
  └── delete_valeurs_null.py
  └── output: database.duckdb (nettoyé)
        │
        ▼
analyse_data_after_delete
  ├── analyse_doublons.py   (vérification post-nettoyage)
  └── analyse_valeurs_null.py
        │
        ▼
export_data
  ├── classification.py     → segments produits par Z-score
  └── chiffre.py            → chiffre d'affaires consolidé
  └── outputs: CSV
```

---

## Structure du projet

```
bottleneck-data-pipeline/
│
├── bottleneck/                     # Données sources (montées en volume dans Kestra)
│   ├── Fichier_erp.xlsx
│   ├── fichier_liaison.xlsx
│   └── Fichier_web.xlsx
│
├── flows/                          # Flows Kestra (YAML)
│   ├── init-bottleneck-data-pipeline.yml
│   ├── import_excel_data_new.yml
│   ├── analyse_data.yml
│   ├── delete_data.yml
│   ├── analyse_data_after_delete.yml
│   ├── export_data.yml
│   ├── classification-z-score.yml
│   ├── monthly_trigger.yml
│   └── unit_test.yml
│
├── scripts/                        # Scripts Python (namespace files Kestra)
│   ├── import_all_excel_to_parquet.py
│   ├── create_db_from_parquet.py
│   ├── analyse_doublons.py
│   ├── analyse_valeurs_null.py
│   ├── delete_doublons.py
│   ├── delete_valeurs_null.py
│   ├── classification.py
│   ├── chiffre.py
│   ├── export.py
│   ├── doublon_repository.py / doublon_service.py
│   ├── null_repository.py / null_service.py
│   └── test/
│       ├── unit_test_doublon.py
│       ├── unit_test_null_value.py
│       ├── unit_test_deleted_doublon.py
│       └── unit_test_deleted_null_value.py
│
├── docker-compose.yml              # Infrastructure locale
├── init-kestra.sh                  # Script de déploiement flows + namespace files
└── .env                            # Variables d'environnement (tokens, webhooks)
```

---

## Lancer le projet

### Prérequis

- Docker Desktop installé et lancé
- Git

### Démarrage

```bash
git clone https://github.com/Xantos07/bottleneck-data-pipeline.git
cd bottleneck-data-pipeline
docker compose up -d
```

Docker Compose démarre trois services :

| Service | Description |
|---|---|
| `postgres` | Base de données Kestra |
| `kestra` | Serveur Kestra (UI sur http://localhost:8080) |
| `kestra-init` | Déploie automatiquement les flows et namespace files au démarrage |

L'UI est accessible sur [http://localhost:8080](http://localhost:8080)
- Login : `your_kestra_admin_user` (défini dans `.env`)
- Mot de passe : `your_kestra_admin_password` (défini dans `.env`)

### Mise à jour des scripts ou flows

Après modification d'un fichier dans `scripts/` ou `flows/` :

```bash
docker compose restart kestra-init
```

### Tester le CI/CD en local avec `act`

[`act`](https://github.com/nektos/act) permet de simuler GitHub Actions localement sans pusher.

**Installation :**
```bash
scoop install act      # Windows (Scoop)
choco install act-cli  # Windows (Chocolatey)
brew install act       # macOS
```

**Créer le fichier de secrets locaux** (non commité) :
```bash
# .secrets.act
KESTRA_URL=http://host.docker.internal:8080
KESTRA_ADMIN_USER=your_kestra_admin_user
KESTRA_ADMIN_PASSWORD=your_kestra_admin_password
```

**Utilisation :**
```bash
act push -b main       # simule un push sur main (validate + deploy)
act push -j validate   # validation uniquement
act push -j deploy     # déploiement uniquement
```

> Le job `deploy` nécessite un Kestra accessible sur un serveur. En local, seul `validate` est fonctionnel.

---

## Flows détaillés

### `init-bottleneck-data-pipeline`
Installation des dépendances Python dans l'environnement Kestra. Point d'entrée du pipeline mensuel.

### `import_excel_data_new`
Lit les trois fichiers Excel depuis `/app/data` et les convertit en Parquet via `pandas` + `pyarrow`. Les fichiers Parquet sont passés en output du flow pour le flow suivant.

### `analyse_data`
- Reconstruit une base DuckDB à partir des Parquet
- Lance en **parallèle** l'analyse des doublons et des valeurs nulles
- Transmet la base au flow suivant via output Kestra

### `delete_data`
Supprime les doublons et valeurs nulles identifiés. La base nettoyée est passée en output.

### `analyse_data_after_delete`
Vérifie la qualité des données après nettoyage (re-run des analyses pour confirmation).

### `export_data`
- Calcule le chiffre d'affaires par produit (jointure ERP × Liaison × Web)
- Classe les produits par Z-score
- Exporte les résultats en CSV

### `monthly_trigger`
Trigger cron (`0 9 15 * *` — le 15 de chaque mois à 9h) qui lance le pipeline complet.

---

## Architecture des scripts Python

Les scripts suivent un pattern **Repository / Service** :

```
doublon_repository.py   → accès données (requêtes DuckDB)
doublon_service.py      → logique métier (détection doublons)
analyse_doublons.py     → point d'entrée (orchestration)
delete_doublons.py      → suppression
```

Même pattern pour les valeurs nulles (`null_*`).

---

## Tests

Les tests unitaires couvrent la détection et la suppression des doublons et valeurs nulles.

```bash
# Depuis le conteneur Kestra ou en local avec DuckDB installé
python scripts/test/unit_test_doublon.py
python scripts/test/unit_test_null_value.py
python scripts/test/unit_test_deleted_doublon.py
python scripts/test/unit_test_deleted_null_value.py
```

Le flow `unit_test` dans Kestra exécute ces tests dans l'environnement orchestré.

---

## env

```
# Variables d'environnement pour Docker Compose
# PostgreSQL (Kestra)
POSTGRES_DB=your_kestra_db
POSTGRES_USER=your_kestra_user
POSTGRES_PASSWORD=your_kestra_password

# Kestra Admin
KESTRA_ADMIN_USER=your_kestra_admin_user
KESTRA_ADMIN_PASSWORD=your_kestra_admin_password
```
