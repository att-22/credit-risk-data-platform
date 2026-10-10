# Credit Risk Data Platform

[![CI](https://github.com/att-22/credit-risk-data-platform/actions/workflows/ci.yml/badge.svg)](https://github.com/att-22/credit-risk-data-platform/actions/workflows/ci.yml)

Plataforma de datos construida a partir de mi TFM de *scoring* crediticio
(dataset [Home Credit Default Risk](https://www.kaggle.com/competitions/home-credit-default-risk/data)).

El objetivo es llevar la ETL del TFM —hecha en notebooks con pandas— a un
proyecto de ingeniería de datos profesional: código versionado, testeado,
orquestado y desplegable.

## Estado

- [ ] Fase 0 — Bases: Git, empaquetado, tests, lint, CI con GitHub Actions, Docker
- [ ] Fase 1 — Modelado de datos con dbt + DuckDB (arquitectura medallion)
- [ ] Fase 2 — Orquestación con Airflow
- [ ] Fase 3 — Ingesta incremental y streaming
- [ ] Fase 4 — Procesamiento distribuido con Spark
- [ ] Fase 5 — Cloud + Terraform + despliegue continuo

## Datos

Los datos **no** están en este repositorio. Se leen desde la carpeta indicada
en la variable de entorno `DATA_DIR`.
