package org.apache.seatunnel.connectors.seatunnel.deltalake.utils;

import io.delta.kernel.*;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.types.DataType;
import io.delta.kernel.types.FieldMetadata;
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterable;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.types.Types;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.catalog.exception.CatalogException;
import org.apache.seatunnel.api.table.catalog.exception.DatabaseNotExistException;
import org.apache.seatunnel.api.table.catalog.exception.TableAlreadyExistException;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.connectors.seatunnel.deltalake.data.DeltalakeTypeMapper;
import org.codehaus.commons.nullanalysis.NotNull;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class SchemaUtils {
  public static SeaTunnelDataType<?> toSeaTunnelType(String fieldName, DataType type) {
    return DeltalakeTypeMapper.mapping(fieldName, type);
  }

  public static Table autoCreateTable(
          Engine engine, TablePath tablePath, CatalogTable table, ReadonlyConfig readonlyConfig)
          throws TableAlreadyExistException, DatabaseNotExistException, CatalogException {
    Table myTable = Table.forPath(engine, tablePath.toString());
    TableSchema tableSchema = table.getTableSchema();

    TransactionBuilder txnBuilder =
            myTable.createTransactionBuilder(
                    engine,
                    "Examples", /* engineInfo - connector can add its own identifier which is noted in the Delta Log */
                    Operation.WRITE /* What is the operation we are trying to perform? This is noted in the Delta Log */
            );

    // convert the table schema to the Delta table schema
    StructType deltaTableSchema = toDeltaLakeSchema(tableSchema, readonlyConfig);

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

  @NotNull protected static StructType toDeltaLakeSchema(
          @NotNull TableSchema tableSchema, @NotNull ReadonlyConfig readonlyConfig
  ) {
    return toDeltaLakeType(tableSchema);
  }

  public static StructType toDeltaLakeType(TableSchema tableSchema) {
    List<StructField> fields = new ArrayList<>();
    AtomicInteger idIncrementer = new AtomicInteger(1);
    for (Column column : tableSchema.getColumns()) {
      StructField field = new StructField(
              column.getName(),
              DeltalakeTypeMapper.toDeltaLakeType(column.getDataType(), idIncrementer),
              column.isNullable(),
              FieldMetadata.builder()
                      .putString("comment", column.getComment())
                      .build()
      );
      fields.add(field);
    }
    return new StructType(fields);
  }
}
