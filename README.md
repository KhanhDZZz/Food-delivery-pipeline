# Food delivery pipeline
<img width="719" height="427" alt="image" src="https://github.com/user-attachments/assets/4e94264f-1456-445e-9ae4-8ceb02ff6cda" />

Food Delivery Pipeline
ETL pipeline built with Apache Airflow and PostgreSQL using Medallion Architecture (Bronze → Silver → Gold) to ingest CSV/XLSX food delivery orders, clean data, and build a star schema and data marts.

- Dataset Source: https://www.kaggle.com/datasets/luthfimuthathohirin/global-food-delivery-restaurant-market-2026

# Medallion Architecture (Bronze -> Silver -> Gold)
<img width="1835" height="1068" alt="image" src="https://github.com/user-attachments/assets/aadf5693-a0de-42da-8ddc-6618f2020086" />

# DAG Architecture
<img width="1672" height="784" alt="image" src="https://github.com/user-attachments/assets/7d1b2046-ad20-4d32-9a6c-15132800418e" />

# Quick Start
- Modify the following string in the x-airflow-common function within docker-compose.yaml

```
environment:
  _PIP_ADDITIONAL_REQUIREMENTS: "pandas sqlalchemy psycopg2-binary openpyxl"
  DATA_DIR: /opt/airflow/data
  PG_HOST: host.docker.internal
  PG_PORT: "5432"
  PG_DB: postgres
  PG_USER: postgres
  PG_PASSWORD: ${PG_PASSWORD}
volumes:
  - "<csv/xlsx file address>:/opt/airflow/data"
```
- run

```
docker compose up -d
```

- Access http://localhost:8080 and trigger the data_pipeline_for_food_delivery_orders DAG
