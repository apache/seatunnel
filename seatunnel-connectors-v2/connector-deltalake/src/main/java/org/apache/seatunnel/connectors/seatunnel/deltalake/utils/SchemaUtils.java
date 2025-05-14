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
import org.apache.seatunnel.connectors.seatunnel.deltalake.config.DeltaLakeSinkOptions;
import org.apache.seatunnel.connectors.seatunnel.deltalake.data.DeltalakeTypeMapper;
import org.codehaus.commons.nullanalysis.NotNull;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class SchemaUtils {
  public static SeaTunnelDataType<?> toSeaTunnelType(String fieldName, DataType type) {
    return DeltalakeTypeMapper.mapping(fieldName, type);
  }

  @NotNull
  public static StructType toDeltaLakeSchema(
          @NotNull TableSchema tableSchema
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
