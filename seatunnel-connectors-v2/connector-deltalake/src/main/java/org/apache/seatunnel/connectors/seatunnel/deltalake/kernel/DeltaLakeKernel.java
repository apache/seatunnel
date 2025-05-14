package org.apache.seatunnel.connectors.seatunnel.deltalake.kernel;

import io.delta.kernel.*;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterable;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.catalog.exception.TableAlreadyExistException;
import org.apache.seatunnel.api.table.catalog.exception.TableNotExistException;
import org.apache.seatunnel.connectors.seatunnel.deltalake.config.DeltaLakeCommonConfig;
import org.apache.seatunnel.connectors.seatunnel.deltalake.config.DeltaLakeSinkOptions;
import org.apache.seatunnel.connectors.seatunnel.deltalake.utils.SchemaUtils;
import org.apache.seatunnel.shade.com.google.common.collect.ImmutableList;

import java.io.Serializable;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Optional;

import static org.apache.seatunnel.connectors.seatunnel.deltalake.utils.SchemaUtils.toDeltaLakeSchema;

@Slf4j
public class DeltaLakeKernel implements Serializable {
  private static final long serialVersionUID = -6003040601422350869L;
  private static final List<String> HADOOP_CONF_FILES =
          ImmutableList.of("core-site.xml", "hdfs-site.xml", "hive-site.xml");

  private final DeltaLakeCommonConfig config;
  @Getter
  private final Engine engine;
  private final StaticPathResolver resolver;

  public DeltaLakeKernel(DeltaLakeCommonConfig config) {
    this.config = config;
    this.engine = createDeltaEngine();
    String basePath = String.valueOf(config.getTable());
    this.resolver = new StaticPathResolver(basePath);
  }

  private Engine createDeltaEngine() {
    Configuration hadoopConf = new Configuration();
    return DefaultEngine.create(hadoopConf);
  }

  private Optional<String> resolvePath(TablePath path) {
    return resolver.resolvePath(path.getDatabaseName(), path.getSchemaName(), path.getTableName());
  }

  public StructType getSchema(TablePath tablePath) {
    Table table = getTable(tablePath);
    Snapshot snapshot = table.getLatestSnapshot(engine);
    return snapshot.getSchema(engine);
  }

  public StructType getSchema(Table table) {
    Snapshot snapshot = table.getLatestSnapshot(engine);
    return snapshot.getSchema(engine);
  }

  public Table getTable(TablePath tablePath) {
    Optional<String> pathOpt = resolvePath(tablePath);
    if (pathOpt.isEmpty()) {
      throw new TableNotExistException("Table not exist", tablePath);
    }
    return Table.forPath(engine, pathOpt.get());
  }


  public boolean tableExists(TablePath tablePath) {
    return resolvePath(tablePath).map(path ->
            Files.exists(Paths.get(URI.create(path + "/_delta_log")))
    ).orElse(false);
  }

  public Table createTable(
          TablePath tablePath,
          CatalogTable table,
          String engineInfo,
          boolean ignoreIfExists) {
    Table myTable = Table.forPath(engine, tablePath.toString());
    TableSchema tableSchema = table.getTableSchema();

    TransactionBuilder txnBuilder =
            myTable.createTransactionBuilder(
                    engine,
                    engineInfo,
                    Operation.WRITE /* What is the operation we are trying to perform? This is noted in the Delta Log */
            );

    // convert the table schema to the Delta table schema
    StructType deltaTableSchema = toDeltaLakeSchema(tableSchema);

    txnBuilder = txnBuilder
            .withSchema(engine, deltaTableSchema);

    // Build the transaction
    Transaction txn = txnBuilder.build(engine);

    // Commit the transaction.
    TransactionCommitResult commitResult =
            txn.commit(
                    engine,
                    CloseableIterable.emptyIterable()
            );
    if (commitResult.getVersion() == -1) {
      throw new TableAlreadyExistException(
              table.getCatalogName(), tablePath);
    } else {
      return myTable;
    }
  }
}
