package org.apache.seatunnel.connectors.doris.datatype;

import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.converter.BasicTypeDefine;
import org.apache.seatunnel.api.table.converter.TypeConverter;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.common.exception.CommonError;
import org.apache.seatunnel.connectors.doris.config.DorisConfig;

import com.google.auto.service.AutoService;
import com.google.common.base.Preconditions;
import com.mysql.cj.MysqlType;
import lombok.extern.slf4j.Slf4j;

import java.util.Locale;

@Slf4j
@AutoService(TypeConverter.class)
public class DorisTypeConverter implements TypeConverter<BasicTypeDefine> {

    public static final String NULL = "NULL";
    public static final String BOOLEAN = "BOOLEAN";
    public static final String TINYINT = "TINYINT";
    public static final String SMALLINT = "SMALLINT";
    public static final String INT = "INT";
    public static final String BIGINT = "BIGINT";
    public static final String FLOAT = "FLOAT";
    public static final String DOUBLE = "DOUBLE";
    public static final String DECIMAL = "DECIMAL";
    public static final String DECIMALV3 = "DECIMALV3";
    public static final String DATE = "DATE";
    public static final String DATEV2 = "DATEV2";
    public static final String DATETIME = "DATETIME";
    public static final String DATETIMEV2 = "DATETIMEV2";
    public static final String CHAR = "CHAR";
    public static final String VARCHAR = "VARCHAR";
    public static final String STRING = "STRING";
    public static final String BINARY = "BINARY";
    public static final String VARBINARY = "VARBINARY";
    public static final String ARRAY = "ARRAY";
    public static final String MAP = "MAP";
    public static final String STRUCT = "STRUCT";
    public static final String UNION = "UNION";
    public static final String INTERVAL = "INTERVAL";
    public static final String TIMESTAMP = "TIMESTAMP";
    public static final String YEAR = "YEAR";
    public static final String GEOMETRY = "GEOMETRY";
    public static final String IP = "IP";
    public static final String JSONB = "JSONB";

    public static final String PRECISION = "precision";
    public static final String SCALE = "scale";

    public static final Integer DEFAULT_PRECISION = 10;
    public static final Integer MAX_PRECISION = 38;

    public static final Integer DEFAULT_SCALE = 0;

    public static final long POWER_2_8 = (long) Math.pow(2, 8);
    public static final long POWER_2_16 = (long) Math.pow(2, 16);
    public static final long MAX_STRING_LENGTH = 2147483643;
    public static final long MAX_VARBINARY_LENGTH = POWER_2_16 - 4;

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
            case "NULL_TYPE":
                builder.dataType(BasicType.VOID_TYPE);
                break;
            case "BOOLEAN":
                builder.dataType(BasicType.BOOLEAN_TYPE);
                break;
            case "TINYINT":
            case "SMALLINT":
                builder.dataType(BasicType.SHORT_TYPE);
                break;
            case "INT":
                builder.dataType(BasicType.INT_TYPE);
                break;
            case "BIGINT":
                builder.dataType(BasicType.LONG_TYPE);
                break;
            case "FLOAT":
                builder.dataType(BasicType.FLOAT_TYPE);
                break;
            case "DOUBLE":
                builder.dataType(BasicType.DOUBLE_TYPE);
                break;
            case "DATE":
            case "DATEV2":
                builder.dataType(LocalTimeType.LOCAL_DATE_TYPE);
                break;
            case "DATETIME":
            case "DATETIMEV2":
            case "DATETIMEV3":
                builder.dataType(LocalTimeType.LOCAL_DATE_TIME_TYPE);
                break;
            case "DECIMAL":
            case "DECIMALV2":
            case "DECIMALV3":
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
            case "TIME":
                builder.dataType(LocalTimeType.LOCAL_TIME_TYPE);
                break;
            case "CHAR":
            case "LARGEINT":
            case "VARCHAR":
            case "JSONB":
            case "STRING":
            case "ARRAY":
            case "MAP":
            case "STRUCT":
                if (typeDefine.getLength() == null || typeDefine.getLength() <= 0) {
                    builder.columnLength(1L);
                } else {
                    builder.columnLength(typeDefine.getLength());
                }
                builder.dataType(BasicType.STRING_TYPE);
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
                BasicTypeDefine.<MysqlType>builder()
                        .name(column.getName())
                        .nullable(column.isNullable())
                        .comment(column.getComment())
                        .defaultValue(column.getDefaultValue());

        switch (column.getDataType().getSqlType()) {
            case NULL:
                builder.nativeType(NULL);
                builder.columnType(NULL);
                builder.dataType(NULL);
                break;
            case STRING:
                if (column.getColumnLength() == null || column.getColumnLength() <= 0) {
                    builder.nativeType(STRING);
                    builder.columnType(STRING);
                    builder.dataType(VARCHAR);
                } else if (column.getColumnLength() < POWER_2_8) {
                    builder.nativeType(MysqlType.CHAR);
                    builder.columnType(String.format("%s(%s)", CHAR, column.getColumnLength()));
                    builder.dataType(CHAR);
                } else if (column.getColumnLength() < POWER_2_16) {
                    builder.nativeType(VARCHAR);
                    builder.columnType(VARCHAR);
                    builder.dataType(VARCHAR);
                } else if (column.getColumnLength() <= MAX_STRING_LENGTH) {
                    builder.nativeType(VARCHAR);
                    builder.columnType(STRING);
                    builder.dataType(STRING);
                }
                break;
            case BYTES:
                builder.nativeType(VARCHAR);
                builder.columnType(STRING);
                builder.dataType(STRING);
                break;
            case BOOLEAN:
                builder.nativeType(BOOLEAN);
                builder.columnType(BIGINT);
                builder.dataType(BIGINT);
                break;
            case SMALLINT:
                builder.nativeType(SMALLINT);
                builder.columnType(SMALLINT);
                builder.dataType(SMALLINT);
                break;
            case INT:
                builder.nativeType(INT);
                builder.columnType(INT);
                builder.dataType(INT);
                break;
            case BIGINT:
                builder.nativeType(BIGINT);
                builder.columnType(BIGINT);
                builder.dataType(BIGINT);
                break;
            case FLOAT:
                builder.nativeType(FLOAT);
                builder.columnType(FLOAT);
                builder.dataType(FLOAT);
                break;
            case DOUBLE:
                builder.nativeType(DOUBLE);
                builder.columnType(DOUBLE);
                builder.dataType(DOUBLE);
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

                builder.nativeType(DECIMALV3);
                builder.columnType(String.format("%s(%s,%s)", DECIMALV3, precision, scale));
                builder.dataType(DECIMALV3);
                builder.precision((long) precision);
                builder.scale(scale);
                break;
            case TIME:
                builder.nativeType(VARCHAR);
                builder.length(8L);
                builder.columnType(VARCHAR);
                builder.dataType(VARCHAR);
                break;
            case DATE:
                builder.nativeType(DATEV2);
                builder.columnType(DATEV2);
                builder.dataType(DATEV2);
                break;
            case TIMESTAMP:
                builder.nativeType(DATETIME);
                builder.columnType(DATETIME);
                builder.dataType(DATETIME);
                break;
            case ARRAY:
                builder.nativeType(ARRAY);
                builder.columnType(ARRAY);
                builder.dataType(ARRAY);
                break;
            case MAP:
            case ROW:
                builder.nativeType(JSONB);
                builder.columnType(JSONB);
                builder.dataType(JSONB);
                break;
            case TINYINT:
                builder.nativeType(TINYINT);
                builder.columnType(TINYINT);
                builder.dataType(TINYINT);
                break;
            default:
                throw CommonError.convertToConnectorTypeError(
                        DorisConfig.IDENTIFIER,
                        column.getDataType().getSqlType().name(),
                        column.getName());
        }
        return builder.build();
    }
}
