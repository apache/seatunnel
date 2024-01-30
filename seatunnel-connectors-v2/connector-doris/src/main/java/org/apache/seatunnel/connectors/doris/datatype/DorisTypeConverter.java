package org.apache.seatunnel.connectors.doris.datatype;

import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.converter.BasicTypeDefine;
import org.apache.seatunnel.api.table.converter.TypeConverter;
import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.common.exception.CommonError;
import org.apache.seatunnel.connectors.doris.config.DorisConfig;

import com.google.auto.service.AutoService;
import com.google.common.base.Preconditions;
import lombok.extern.slf4j.Slf4j;

import java.util.Locale;

@Slf4j
@AutoService(TypeConverter.class)
public class DorisTypeConverter implements TypeConverter<BasicTypeDefine> {

    public static final String DORIS_NULL = "NULL";
    public static final String DORIS_BOOLEAN = "BOOLEAN";
    public static final String DORIS_TINYINT = "TINYINT";
    public static final String DORIS_SMALLINT = "SMALLINT";
    public static final String DORIS_INT = "INT";
    public static final String DORIS_BIGINT = "BIGINT";
    public static final String DORIS_LARGEINT = "LARGEINT";
    public static final String DORIS_FLOAT = "FLOAT";
    public static final String DORIS_DOUBLE = "DOUBLE";
    public static final String DORIS_DECIMAL = "DECIMAL";
    public static final String DORIS_DECIMALV3 = "DECIMALV3";
    public static final String DORIS_DATE = "DATE";
    public static final String DORIS_DATEV2 = "DATEV2";
    public static final String DORIS_DATETIME = "DATETIME";
    public static final String DORIS_DATETIMEV2 = "DATETIMEV2";
    public static final String DORIS_CHAR = "CHAR";
    public static final String DORIS_VARCHAR = "VARCHAR";
    public static final String DORIS_STRING = "STRING";
    public static final String DORIS_BOOLEAN_ARRAY = "ARRAY<boolean>";
    public static final String DORIS_TINYINT_ARRAY = "ARRAY<tinyint>";
    public static final String DORIS_SMALLINT_ARRAY = "ARRAY<smallint>";
    public static final String DORIS_INT_ARRAY = "ARRAY<int(11)>";
    public static final String DORIS_BIGINT_ARRAY = "ARRAY<bigint>";
    public static final String DORIS_FLOAT_ARRAY = "ARRAY<float>";
    public static final String DORIS_DOUBLE_ARRAY = "ARRAY<double>";
    public static final String DORIS_DECIMALV3_ARRAY = "ARRAY<DECIMALV3>";
    public static final String DORIS_DECIMALV3_ARRAY_COLUMN_TYPE_TMP = "ARRAY<DECIMALV3(%, %)>";
    public static final String DORIS_DATEV2_ARRAY = "ARRAY<DATEV2>";
    public static final String DORIS_DATETIMEV2_ARRAY = "ARRAY<DATETIMEV2>";
    public static final String DORIS_STRING_ARRAY = "ARRAY<STRING>";

    // Because can not get the column length from array, So the following types of arrays cannot be
    // generated properly.
    public static final String DORIS_LARGEINT_ARRAY = "ARRAY<largeint>";
    public static final String DORIS_CHAR_ARRAY = "ARRAY<CHAR>";
    public static final String DORIS_CHAR_ARRAY_COLUMN_TYPE_TMP = "ARRAY<CHAR(%s)>";
    public static final String DORIS_VARCHAR_ARRAY = "ARRAY<VARCHAR>";
    public static final String DORIS_VARCHAR_ARRAY_COLUMN_TYPE_TMP = "ARRAY<VARCHAR(%s)>";

    public static final String DORIS_JSONB = "JSONB";

    // for doris version >= 2.0
    public static final String DORIS_JSON = "JSON";

    public static final Integer DEFAULT_PRECISION = 9;
    public static final Integer MAX_PRECISION = 38;

    public static final Integer DEFAULT_SCALE = 0;

    // Min value of LARGEINT is -170141183460469231731687303715884105728, it will use 39 bytes in
    // UTF-8.
    // Add a bit to prevent overflow
    public static final long MAX_DORIS_LARGEINT_TO_VARCHAR_LENGTH = 39L;

    public static final long POWER_2_8 = (long) Math.pow(2, 8);
    public static final long POWER_2_16 = (long) Math.pow(2, 16);
    public static final long MAX_STRING_LENGTH = 2147483643;

    public static final DorisTypeConverter INSTANCE = new DorisTypeConverter();

    @Override
    public String identifier() {
        return DorisConfig.IDENTIFIER;
    }

    @Override
    public Column convert(BasicTypeDefine typeDefine) {
        PhysicalColumn.PhysicalColumnBuilder builder =
                PhysicalColumn.builder()
                        .name(typeDefine.getName())
                        .sourceType(typeDefine.getColumnType())
                        .nullable(typeDefine.isNullable())
                        .defaultValue(typeDefine.getDefaultValue())
                        .comment(typeDefine.getComment());

        String dorisColumnType = typeDefine.getColumnType().toUpperCase(Locale.ROOT);
        int idx = dorisColumnType.indexOf("(");
        int idx2 = dorisColumnType.indexOf("<");
        if (idx != -1) {
            dorisColumnType = dorisColumnType.substring(0, idx);
        }
        if (idx2 != -1) {
            dorisColumnType = dorisColumnType.substring(0, idx2);
        }

        switch (dorisColumnType) {
            case DORIS_NULL:
                builder.dataType(BasicType.VOID_TYPE);
                break;
            case DORIS_BOOLEAN:
                builder.dataType(BasicType.BOOLEAN_TYPE);
                break;
            case DORIS_TINYINT:
                if (typeDefine.getColumnType().equalsIgnoreCase("tinyint(1)")) {
                    builder.dataType(BasicType.BOOLEAN_TYPE);
                } else {
                    builder.dataType(BasicType.SHORT_TYPE);
                }
                break;
            case DORIS_SMALLINT:
                builder.dataType(BasicType.SHORT_TYPE);
                break;
            case DORIS_INT:
                builder.dataType(BasicType.INT_TYPE);
                break;
            case DORIS_BIGINT:
                builder.dataType(BasicType.LONG_TYPE);
                break;
            case DORIS_FLOAT:
                builder.dataType(BasicType.FLOAT_TYPE);
                break;
            case DORIS_DOUBLE:
                builder.dataType(BasicType.DOUBLE_TYPE);
                break;
            case DORIS_DATE:
            case DORIS_DATEV2:
                builder.dataType(LocalTimeType.LOCAL_DATE_TYPE);
                break;
            case DORIS_DATETIME:
                builder.dataType(LocalTimeType.LOCAL_DATE_TIME_TYPE);
                break;
            case DORIS_DATETIMEV2:
                builder.dataType(LocalTimeType.LOCAL_DATE_TIME_TYPE);
                builder.scale(typeDefine.getScale());
                break;
            case DORIS_DECIMAL:
            case DORIS_DECIMALV3:
                Preconditions.checkArgument(typeDefine.getPrecision() > 0);
                DecimalType decimalType;
                decimalType =
                        new DecimalType(
                                typeDefine.getPrecision().intValue(),
                                typeDefine.getScale() == null
                                        ? 0
                                        : typeDefine.getScale().intValue());
                builder.dataType(decimalType);
                builder.columnLength(Long.valueOf(decimalType.getPrecision()));
                builder.scale(decimalType.getScale());
                break;
            case DORIS_CHAR:
            case DORIS_VARCHAR:
                if (typeDefine.getLength() != null && typeDefine.getLength() > 0) {
                    builder.columnLength(typeDefine.getLength());
                }
                builder.dataType(BasicType.STRING_TYPE);
                builder.sourceType(dorisColumnType);
                break;
            case DORIS_LARGEINT:
                builder.dataType(BasicType.STRING_TYPE);
                builder.columnLength(MAX_DORIS_LARGEINT_TO_VARCHAR_LENGTH);
                break;
            case DORIS_STRING:
            case DORIS_JSONB:
            case DORIS_JSON:
                builder.dataType(BasicType.STRING_TYPE);
                builder.columnLength(MAX_STRING_LENGTH);
                break;
            default:
                throw CommonError.convertToSeaTunnelTypeError(
                        DorisConfig.IDENTIFIER, dorisColumnType, typeDefine.getName());
        }

        return builder.build();
    }

    @Override
    public BasicTypeDefine reconvert(Column column) {
        BasicTypeDefine.BasicTypeDefineBuilder builder =
                BasicTypeDefine.builder()
                        .name(column.getName())
                        .nullable(column.isNullable())
                        .comment(column.getComment())
                        .defaultValue(column.getDefaultValue());

        switch (column.getDataType().getSqlType()) {
            case NULL:
                builder.columnType(DORIS_NULL);
                builder.dataType(DORIS_NULL);
                break;
            case STRING:
                reconvertString(column, builder);
                break;
            case BYTES:
                builder.columnType(DORIS_STRING);
                builder.dataType(DORIS_STRING);
                break;
            case BOOLEAN:
                builder.columnType(DORIS_BOOLEAN);
                builder.dataType(DORIS_BOOLEAN);
                builder.length(1L);
                break;
            case TINYINT:
                builder.columnType(DORIS_TINYINT);
                builder.dataType(DORIS_TINYINT);
                break;
            case SMALLINT:
                builder.columnType(DORIS_SMALLINT);
                builder.dataType(DORIS_SMALLINT);
                break;
            case INT:
                builder.columnType(DORIS_INT);
                builder.dataType(DORIS_INT);
                break;
            case BIGINT:
                builder.columnType(DORIS_BIGINT);
                builder.dataType(DORIS_BIGINT);
                break;
            case FLOAT:
                builder.columnType(DORIS_FLOAT);
                builder.dataType(DORIS_FLOAT);
                break;
            case DOUBLE:
                builder.columnType(DORIS_DOUBLE);
                builder.dataType(DORIS_DOUBLE);
                break;
            case DECIMAL:
                DecimalType decimalType = (DecimalType) column.getDataType();
                int precision = decimalType.getPrecision();
                int scale = decimalType.getScale();
                if (precision <= 0) {
                    precision = DEFAULT_PRECISION;
                    scale = DEFAULT_SCALE;
                    log.warn(
                            "The decimal column {} type decimal({},{}) is out of range, "
                                    + "which is precision less than 0, "
                                    + "it will be converted to decimal({},{})",
                            column.getName(),
                            decimalType.getPrecision(),
                            decimalType.getScale(),
                            precision,
                            scale);
                } else if (precision > MAX_PRECISION) {
                    scale = (int) Math.max(0, scale - (precision - MAX_PRECISION));
                    precision = MAX_PRECISION;
                    log.warn(
                            "The decimal column {} type decimal({},{}) is out of range, "
                                    + "which exceeds the maximum precision of {}, "
                                    + "it will be converted to decimal({},{})",
                            column.getName(),
                            decimalType.getPrecision(),
                            decimalType.getScale(),
                            MAX_PRECISION,
                            precision,
                            scale);
                }
                if (scale < 0) {
                    scale = 0;
                    log.warn(
                            "The decimal column {} type decimal({},{}) is out of range, "
                                    + "which is scale less than 0, "
                                    + "it will be converted to decimal({},{})",
                            column.getName(),
                            decimalType.getPrecision(),
                            decimalType.getScale(),
                            precision,
                            scale);
                } else if (scale > precision) {
                    scale = precision;
                    log.warn(
                            "The decimal column {} type decimal({},{}) is out of range, "
                                    + "which exceeds the maximum scale of {}, "
                                    + "it will be converted to decimal({},{})",
                            column.getName(),
                            decimalType.getPrecision(),
                            decimalType.getScale(),
                            precision,
                            precision,
                            scale);
                }

                builder.columnType(String.format("%s(%s,%s)", DORIS_DECIMALV3, precision, scale));
                builder.dataType(DORIS_DECIMALV3);
                builder.precision((long) precision);
                builder.scale(scale);
                break;
            case TIME:
                builder.length(8L);
                builder.columnType(String.format("%s(%s)", DORIS_VARCHAR, 8));
                builder.dataType(DORIS_VARCHAR);
                break;
            case DATE:
                builder.columnType(DORIS_DATEV2);
                builder.dataType(DORIS_DATEV2);
                break;
            case TIMESTAMP:
                if (column.getScale() != null && column.getScale() > 0) {
                    builder.columnType(
                            String.format("%s(%s)", DORIS_DATETIMEV2, column.getScale()));
                    builder.scale(column.getScale());
                } else {
                    builder.columnType(DORIS_DATETIMEV2);
                }
                builder.dataType(DORIS_DATETIMEV2);
                break;
            case ARRAY:
                ArrayType arrayType = (ArrayType) column.getDataType();
                SeaTunnelDataType elementType = arrayType.getElementType();
                buildArrayInternal(elementType, builder, column.getName());
                break;
            case MAP:
            case ROW:
                builder.columnType(DORIS_JSONB);
                builder.dataType(DORIS_JSONB);
                break;
            default:
                throw CommonError.convertToConnectorTypeError(
                        DorisConfig.IDENTIFIER,
                        column.getDataType().getSqlType().name(),
                        column.getName());
        }
        return builder.build();
    }

    private void reconvertString(Column column, BasicTypeDefine.BasicTypeDefineBuilder builder) {
        // source is doris too.
        if (column.getSourceType() != null
                && column.getSourceType().equalsIgnoreCase(DORIS_LARGEINT)) {
            builder.columnType(DORIS_LARGEINT);
            builder.dataType(DORIS_LARGEINT);
            return;
        }

        if (column.getSourceType() != null && column.getSourceType().equalsIgnoreCase(DORIS_JSON)) {
            // Compatible with Doris 1.x and Doris 2.x versions
            builder.columnType(DORIS_JSONB);
            builder.dataType(DORIS_JSONB);
            return;
        }

        if (column.getSourceType() != null
                && column.getSourceType().equalsIgnoreCase(DORIS_JSONB)) {
            builder.columnType(DORIS_JSONB);
            builder.dataType(DORIS_JSONB);
            return;
        }

        if (column.getColumnLength() == null || column.getColumnLength() <= 0) {
            builder.columnType(DORIS_STRING);
            builder.dataType(DORIS_STRING);
            return;
        }

        if (column.getColumnLength() < POWER_2_8) {
            if (column.getSourceType() != null
                    && column.getSourceType().equalsIgnoreCase(DORIS_VARCHAR)) {
                builder.columnType(
                        String.format("%s(%s)", DORIS_VARCHAR, column.getColumnLength()));
                builder.dataType(DORIS_VARCHAR);
            } else {
                builder.columnType(String.format("%s(%s)", DORIS_CHAR, column.getColumnLength()));
                builder.dataType(DORIS_CHAR);
            }
            return;
        }

        if (column.getColumnLength() < POWER_2_16) {
            builder.columnType(String.format("%s(%s)", DORIS_VARCHAR, column.getColumnLength()));
            builder.dataType(DORIS_VARCHAR);
            return;
        }

        if (column.getColumnLength() <= MAX_STRING_LENGTH) {
            builder.columnType(DORIS_STRING);
            builder.dataType(DORIS_STRING);
            return;
        }

        throw CommonError.convertToConnectorTypeError(
                DorisConfig.IDENTIFIER, column.getDataType().getSqlType().name(), column.getName());
    }

    private void buildArrayInternal(
            SeaTunnelDataType elementType,
            BasicTypeDefine.BasicTypeDefineBuilder builder,
            String columnName) {
        switch (elementType.getSqlType()) {
            case BOOLEAN:
                builder.columnType(DORIS_BOOLEAN_ARRAY);
                builder.dataType(DORIS_BOOLEAN_ARRAY);
                break;
            case TINYINT:
                builder.columnType(DORIS_TINYINT_ARRAY);
                builder.dataType(DORIS_TINYINT_ARRAY);
                break;
            case SMALLINT:
                builder.columnType(DORIS_SMALLINT_ARRAY);
                builder.dataType(DORIS_SMALLINT_ARRAY);
                break;
            case INT:
                builder.columnType(DORIS_INT_ARRAY);
                builder.dataType(DORIS_INT_ARRAY);
                break;
            case BIGINT:
                builder.columnType(DORIS_BIGINT_ARRAY);
                builder.dataType(DORIS_BIGINT_ARRAY);
                break;
            case FLOAT:
                builder.columnType(DORIS_FLOAT_ARRAY);
                builder.dataType(DORIS_FLOAT_ARRAY);
                break;
            case DOUBLE:
                builder.columnType(DORIS_DOUBLE_ARRAY);
                builder.dataType(DORIS_DOUBLE_ARRAY);
                break;
            case DECIMAL:
                builder.columnType(
                        String.format(
                                DORIS_DECIMALV3_ARRAY_COLUMN_TYPE_TMP,
                                MAX_PRECISION,
                                DEFAULT_SCALE));
                builder.dataType(DORIS_DECIMALV3_ARRAY);
                break;
            case STRING:
            case TIME:
                builder.columnType(DORIS_STRING_ARRAY);
                builder.dataType(DORIS_STRING_ARRAY);
                break;
            case DATE:
                builder.columnType(DORIS_DATEV2_ARRAY);
                builder.dataType(DORIS_DATEV2_ARRAY);
                break;
            case TIMESTAMP:
                builder.columnType(DORIS_DATETIMEV2_ARRAY);
                builder.dataType(DORIS_DATETIMEV2_ARRAY);
                break;
            default:
                throw CommonError.convertToConnectorTypeError(
                        DorisConfig.IDENTIFIER, elementType.getSqlType().name(), columnName);
        }
    }
}