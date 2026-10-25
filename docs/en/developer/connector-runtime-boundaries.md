# Connector Runtime Boundaries

This page explains how connector runtime APIs, option rules, catalogs, and metadata providers fit
together. It is intended for developers building connectors or integrating SeaTunnel with external
metadata systems.

## Connector Source/Sink SPI is the runtime data path

`TableSourceFactory` and `TableSinkFactory` create the runtime source and sink objects that read and
write data. Runtime connector code is responsible for:

- creating the source or sink instance;
- validating and reading connector options;
- producing or consuming `SeaTunnelRow` records;
- preserving schema, primary key, table path, and row kind metadata when the connector supports
  them.

Do not put connector-specific read/write behavior into engine, core, or metadata provider code.

## OptionRule is a configuration contract

Every connector factory exposes `optionRule()`. SeaTunnel uses it to validate user configuration and
to let Web UI or plugin-discovery code understand which options are required or optional.

`OptionRule` should describe the connector's configuration surface. It is not a place to open
network connections, discover remote schemas, or perform runtime side effects.

When you add or change user-facing options:

- define them as `Option` values;
- include defaults only when the connector really has a stable default;
- keep existing option names backward compatible;
- update both English and Chinese docs.

## CatalogFactory is for catalog operations

`CatalogFactory` creates a `Catalog` for metadata operations such as listing databases, listing
tables, and reading table schemas. A connector may provide both runtime source/sink factories and a
catalog factory, but they are separate contracts.

Use catalog code when the operation is about metadata. Use source/sink runtime code when the
operation reads or writes records.

## MetadataProvider is for external datasource resolution

`MetadataProvider` is a separate SPI for external metadata services. It resolves a
`metadata_datasource_id` or external table id into connector configuration or table schema.

The provider acts before connector creation. It should map external metadata to SeaTunnel config; it
should not replace the connector source/sink implementation and should not contain connector runtime
read/write logic.

## Practical boundary checks before submitting a connector PR

- Does the connector factory expose all user-facing options through `OptionRule`?
- Are option names, defaults, and docs consistent?
- If the connector supports catalog operations, is the catalog implementation registered through
  `CatalogFactory` instead of hidden in source/sink runtime code?
- If external metadata is used, does `MetadataProvider` only resolve config/schema and leave runtime
  reading/writing to the connector?
- Do source, sink, catalog, SPI registration, plugin mapping, docs, and tests cover the same feature
  scope?
