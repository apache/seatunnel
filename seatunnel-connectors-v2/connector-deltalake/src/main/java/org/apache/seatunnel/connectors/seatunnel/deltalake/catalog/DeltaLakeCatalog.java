package org.apache.seatunnel.connectors.seatunnel.deltalake.catalog;

import io.delta.kernel.Table;
import io.delta.kernel.Snapshot;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.types.StructType;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.*;
import org.apache.seatunnel.api.table.catalog.exception.CatalogException;
import org.apache.seatunnel.api.table.catalog.exception.TableNotExistException;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.connectors.seatunnel.deltalake.kernel.MetastoreResolver;
import org.apache.seatunnel.connectors.seatunnel.deltalake.kernel.StaticPathResolver;
import org.apache.seatunnel.connectors.seatunnel.deltalake.utils.SchemaUtils;

import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.function.Function;

@Slf4j
public class DeltaLakeCatalog implements Catalog {

  private final String catalogName;
  private final ReadonlyConfig readonlyConfig;

  @Getter
  private final Engine engine;
  private final MetastoreResolver resolver;

  public DeltaLakeCatalog(String catalogName, ReadonlyConfig readonlyConfig) {
    this.catalogName = catalogName;
    this.readonlyConfig = readonlyConfig;
    Configuration hadoopConf = new Configuration();
    this.engine = DefaultEngine.create(hadoopConf);

    String basePath = readonlyConfig.get("base_path").toString();
    this.resolver = new StaticPathResolver(basePath);
  }

  @Override
  public void open() {
    // Initialization logic if needed
  }

  @Override
  public void close() {
    // Cleanup logic if needed
  }

  @Override
  public String name() {
    return catalogName;
  }

  @Override
  public String getDefaultDatabase() {
    return "default";
  }

  @Override
  public boolean databaseExists(String databaseName) {
    // Implement logic to check if the database exists
    return true; // Placeholder implementation
  }

  @Override
  public List<String> listDatabases() {
    // Implement logic to list all databases
    return List.of("default"); // Placeholder implementation
  }

  @Override
  public List<String> listTables(String databaseName) {
    // Implement logic to list all tables in the specified database
    throw new UnsupportedOperationException("listTables not implemented in static mode");
  }

  @Override
  public boolean tableExists(TablePath tablePath) {
    return resolvePath(tablePath).map(path ->
            Files.exists(Paths.get(URI.create(path + "/_delta_log")))
    ).orElse(false);
  }

  @Override
  public CatalogTable getTable(TablePath tablePath) throws CatalogException, TableNotExistException {
    Optional<String> pathOpt = resolvePath(tablePath);
    if (pathOpt.isEmpty()) {
      throw new TableNotExistException("Table not exist", tablePath);
    }

    Table table = Table.forPath(engine, pathOpt.get());
    Snapshot snapshot = table.getLatestSnapshot(engine);
    StructType schema = snapshot.getSchema(engine);
    return toCatalogTable(table, schema, tablePath);
  }

  private CatalogTable toCatalogTable(Table table, StructType schema, TablePath tablePath) {
    TableIdentifier tableId = TableIdentifier.of(catalogName, tablePath);
    TableSchema.Builder schemaBuilder = TableSchema.builder();
    schema.fields().forEach(
            nestedField -> {
              String name = nestedField.getName();
              SeaTunnelDataType<?> seaTunnelType =
                      SchemaUtils.toSeaTunnelType(name, nestedField.getDataType());
              PhysicalColumn physicalColumn =
                      PhysicalColumn.of(
                              name,
                              seaTunnelType,
                              (Long) null,
                              nestedField.isNullable(),
                              null,
                              "");
              schemaBuilder.column(physicalColumn);
            });
    Optional.of(schema.fieldNames())
            .filter(names -> !names.isEmpty())
            .map(
                    (Function<List<String>, Object>)
                            names ->
                                    schemaBuilder.primaryKey(
                                            PrimaryKey.of(
                                                    tablePath.getTableName() + "_pk",
                                                    new ArrayList<>(names))));
    TableSchema tableSchema = schemaBuilder.build();
    return CatalogTable.of(tableId, tableSchema, Map.of(), List.of(), "");
  }

  @Override
  public void createTable(TablePath tablePath, CatalogTable table, boolean ignoreIfExists) {
    log.info("Creating table at path: {}", tablePath);
    SchemaUtils.autoCreateTable(engine, tablePath, table, readonlyConfig);
  }

  @Override
  public void dropTable(TablePath tablePath, boolean ignoreIfNotExists) {
    throw new UnsupportedOperationException("Write operations are not supported");
  }

  @Override
  public void createDatabase(TablePath tablePath, boolean ignoreIfExists) {
    throw new UnsupportedOperationException("Write operations are not supported");
  }

  @Override
  public void dropDatabase(TablePath tablePath, boolean ignoreIfNotExists) {
    throw new UnsupportedOperationException("Write operations are not supported");
  }

  private Optional<String> resolvePath(TablePath path) {
    return resolver.resolvePath(path.getDatabaseName(), path.getSchemaName(), path.getTableName());
  }

  public Engine getEngine() {
    return engine;
  }
}
