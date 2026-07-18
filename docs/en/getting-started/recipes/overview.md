---
slug: /getting-started/recipes
---

# Scenario Recipes

These recipes are best read after your first local job succeeds. Instead of reading every example in order, start with the pipeline shape that is closest to your real source and sink.

The MySQL CDC to Kafka and Elasticsearch recipes in this section are backed by Docker E2E tests. Their configs, observed results, and prerequisites are kept aligned with those executable paths.

## Choose A Recipe By Pipeline Goal

| Goal | Start here |
| --- | --- |
| CDC from MySQL into an analytics database | [MySQL CDC to Doris](./mysql-cdc-to-doris.md) |
| CDC from MySQL into Kafka with metadata headers | [MySQL CDC to Kafka](./mysql-cdc-to-kafka.md) |
| CDC from MySQL into Elasticsearch with filtering and field shaping | [MySQL CDC to Elasticsearch](./mysql-cdc-to-elasticsearch.md) |
| JDBC extraction into object storage | [JDBC to S3](./jdbc-to-s3.md) |
| Streaming from Kafka into a table format | [Kafka to Iceberg](./kafka-to-iceberg.md) |
| HTTP ingestion into a relational target | [HTTP to JDBC](./http-to-jdbc.md) |
| File-based loading into an analytical system | [File to StarRocks](./file-to-starrocks.md) |
| Multi-table CDC orchestration | [Multi-Table CDC](./multi-table-cdc.md) |

## How To Read A Recipe

1. Confirm the source and sink combination matches your target pipeline.
2. Compare the `env`, `source`, `transform`, and `sink` sections with your own job.
3. Replace only one system at a time when adapting the sample.
4. If the sample depends on CDC, drivers, or extra plugins, verify those prerequisites before running it.
