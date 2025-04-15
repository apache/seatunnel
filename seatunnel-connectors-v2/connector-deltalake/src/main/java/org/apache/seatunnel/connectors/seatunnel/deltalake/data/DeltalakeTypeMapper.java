package org.apache.seatunnel.connectors.seatunnel.deltalake.data;

import io.delta.kernel.types.BinaryType;
import io.delta.kernel.types.BooleanType;
import io.delta.kernel.types.ByteType;
import io.delta.kernel.types.DataType;
import io.delta.kernel.types.DateType;
import io.delta.kernel.types.DoubleType;
import io.delta.kernel.types.FloatType;
import io.delta.kernel.types.IntegerType;
import io.delta.kernel.types.LongType;
import io.delta.kernel.types.ShortType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;
import io.delta.kernel.types.TimestampNTZType;
import io.delta.kernel.types.TimestampType;
import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.PrimitiveByteArrayType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.exception.CommonError;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class DeltalakeTypeMapper {


  public static SeaTunnelDataType<?> mapping(String field, DataType deltalakeType) {
    if (deltalakeType instanceof BooleanType) {
      return BasicType.BOOLEAN_TYPE;
    } else if (deltalakeType instanceof ByteType) {
      return BasicType.BYTE_TYPE;
    } else if (deltalakeType instanceof ShortType) {
      return BasicType.SHORT_TYPE;
    } else if (deltalakeType instanceof IntegerType) {
      return BasicType.INT_TYPE;
    } else if (deltalakeType instanceof LongType) {
      return BasicType.LONG_TYPE;
    } else if (deltalakeType instanceof FloatType) {
      return BasicType.FLOAT_TYPE;
    } else if (deltalakeType instanceof DoubleType) {
      return BasicType.DOUBLE_TYPE;
    } else if (deltalakeType instanceof StringType) {
      return BasicType.STRING_TYPE;
    } else if (deltalakeType instanceof BinaryType) {
      return PrimitiveByteArrayType.INSTANCE;
    } else if (deltalakeType instanceof DateType) {
      return LocalTimeType.LOCAL_DATE_TYPE;
    } else if (deltalakeType instanceof TimestampType || deltalakeType instanceof TimestampNTZType) {
      return LocalTimeType.LOCAL_DATE_TIME_TYPE;
    } else if (deltalakeType instanceof io.delta.kernel.types.DecimalType) {
      io.delta.kernel.types.DecimalType decimalType = (io.delta.kernel.types.DecimalType) deltalakeType;
      return new DecimalType(decimalType.getPrecision(), decimalType.getScale());
    } else if (deltalakeType instanceof StructType) {
      return mappingStructType((StructType) deltalakeType);
    } else if (deltalakeType instanceof io.delta.kernel.types.ArrayType) {
      return mappingArrayType(field, (io.delta.kernel.types.ArrayType) deltalakeType);
    } else {
      throw CommonError.convertToSeaTunnelTypeError(
              "Deltalake", deltalakeType.toString(), field);
    }
  }

  private static SeaTunnelRowType mappingStructType(StructType structType) {
    List<StructField> fields = structType.fields();
    List<String> fieldNames = new ArrayList<>(fields.size());
    List<SeaTunnelDataType<?>> fieldTypes = new ArrayList<>(fields.size());
    for (StructField field : fields) {
      fieldNames.add(field.getName());
      fieldTypes.add(mapping(field.getName(), field.getDataType()));
    }
    return new SeaTunnelRowType(
            fieldNames.toArray(new String[0]), fieldTypes.toArray(new SeaTunnelDataType[0]));
  }

  private static ArrayType mappingArrayType(String field, io.delta.kernel.types.ArrayType arrayType) {
    if (arrayType.getElementType() instanceof BooleanType) {
      return ArrayType.BOOLEAN_ARRAY_TYPE;
    } else if (arrayType.getElementType() instanceof IntegerType) {
      return ArrayType.INT_ARRAY_TYPE;
    } else if (arrayType.getElementType() instanceof LongType) {
      return ArrayType.LONG_ARRAY_TYPE;
    } else if (arrayType.getElementType() instanceof FloatType) {
      return ArrayType.FLOAT_ARRAY_TYPE;
    } else if (arrayType.getElementType() instanceof DoubleType) {
      return ArrayType.DOUBLE_ARRAY_TYPE;
    } else if (arrayType.getElementType() instanceof StringType) {
      return ArrayType.STRING_ARRAY_TYPE;
    } else {
      throw CommonError.convertToSeaTunnelTypeError(
              "Deltalake", arrayType.toString(), field);
    }
  }

  public static DataType toDeltaLakeType(
          SeaTunnelDataType seaTunnelDataType,
          AtomicInteger nextId) {
    switch (seaTunnelDataType.getSqlType()) {
      case BOOLEAN:
        return BooleanType.BOOLEAN;
      case BYTES:
        return BinaryType.BINARY;
      case SMALLINT:
      case TINYINT:
        return ShortType.SHORT;
      case INT:
        return IntegerType.INTEGER;
      case BIGINT:
        return LongType.LONG;
      case FLOAT:
        return FloatType.FLOAT;
      case DOUBLE:
        return DoubleType.DOUBLE;
      case DECIMAL:
        DecimalType decimalType = (DecimalType) seaTunnelDataType;
        return new io.delta.kernel.types.DecimalType(
                decimalType.getPrecision(), decimalType.getScale());
      case ARRAY:
        ArrayType arrayType = (ArrayType) seaTunnelDataType;
        DataType elementType = toDeltaLakeType(arrayType.getElementType(), nextId);
        return new io.delta.kernel.types.ArrayType(elementType, false);
      case STRING:
      default:
        return StringType.STRING;
    }
  }
}
