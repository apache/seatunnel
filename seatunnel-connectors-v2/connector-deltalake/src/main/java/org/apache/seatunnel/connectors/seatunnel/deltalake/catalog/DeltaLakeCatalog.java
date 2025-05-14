package org.apache.seatunnel.connectors.seatunnel.deltalake.catalog;

import io.delta.kernel.Table;
import io.delta.kernel.types.StructType;
import lombok.extern.slf4j.Slf4j;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.*;
import org.apache.seatunnel.api.table.catalog.exception.CatalogException;
import org.apache.seatunnel.api.table.catalog.exception.TableNotExistException;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.connectors.seatunnel.deltalake.config.DeltaLakeCommonConfig;
import org.apache.seatunnel.connectors.seatunnel.deltalake.config.DeltaLakeSinkOptions;
import org.apache.seatunnel.connectors.seatunnel.deltalake.kernel.DeltaLakeKernel;
import org.apache.seatunnel.connectors.seatunnel.deltalake.utils.SchemaUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

@Slf4j
public class DeltaLakeCatalog implements Catalog {

  private final String catalogName;
  private final ReadonlyConfig readonlyConfig;

  private final DeltaLakeKernel deltaKernel;

  public DeltaLakeCatalog(String catalogName, ReadonlyConfig readonlyConfig) {
    this.catalogName = catalogName;
    this.readonlyConfig = readonlyConfig;
    this.deltaKernel = new DeltaLakeKernel(
            new DeltaLakeCommonConfig(readonlyConfig));
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
    return deltaKernel.tableExists(tablePath);
  }

  @Override
  public CatalogTable getTable(TablePath tablePath) throws CatalogException, TableNotExistException {
    Table table = deltaKernel.getTable(tablePath);
    StructType schema = deltaKernel.getSchema(tablePath);
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
    String engineInfo =
            String.valueOf(readonlyConfig.getOptional(DeltaLakeSinkOptions.ENGINE_INFO));
    deltaKernel.createTable(tablePath, table, engineInfo, ignoreIfExists);
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
}
