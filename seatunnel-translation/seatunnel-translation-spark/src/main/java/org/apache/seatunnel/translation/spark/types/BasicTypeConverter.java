package org.apache.seatunnel.translation.spark.types;

import org.apache.seatunnel.api.table.type.BasicType;

import org.apache.spark.sql.types.CharType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.DecimalType;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.util.Date;

public class BasicTypeConverter<T1>
    implements SparkDataTypeConverter<BasicType<T1>, DataType> {

    public static final BasicTypeConverter<String> STRING_CONVERTER =
        new BasicTypeConverter<>(
            BasicType.STRING,
            DataTypes.StringType
        );

    public static final BasicTypeConverter<Integer> INTEGER_CONVERTER =
        new BasicTypeConverter<>(
            BasicType.INTEGER,
            DataTypes.IntegerType
        );

    public static final BasicTypeConverter<Boolean> BOOLEAN_CONVERTER =
        new BasicTypeConverter<>(
            BasicType.BOOLEAN,
            DataTypes.BooleanType);

    public static final BasicTypeConverter<Date> DATE_CONVERTER =
        new BasicTypeConverter<>(
            BasicType.DATE,
            DataTypes.DateType
        );

    public static final BasicTypeConverter<Double> DOUBLE_CONVERTER =
        new BasicTypeConverter<>(
            BasicType.DOUBLE,
            DataTypes.DoubleType
        );

    public static final BasicTypeConverter<Long> LONG_CONVERTER =
        new BasicTypeConverter<>(
            BasicType.LONG,
            DataTypes.LongType
        );

    public static final BasicTypeConverter<Float> FLOAT_CONVERTER =
        new BasicTypeConverter<>(
            BasicType.FLOAT,
            DataTypes.FloatType);

    public static final BasicTypeConverter<Byte> BYTE_CONVERTER =
        new BasicTypeConverter<>(
            BasicType.BYTE,
            DataTypes.ByteType
        );

    public static final BasicTypeConverter<Short> SHORT_CONVERTER =
        new BasicTypeConverter<>(
            BasicType.SHORT,
            DataTypes.ShortType);

    // todo: need to confirm
    public static final BasicTypeConverter<Character> CHARACTER_CONVERTER =
        new BasicTypeConverter<>(
            BasicType.CHARACTER,
            new CharType(1)
        );

    public static final BasicTypeConverter<BigInteger> BIG_INTEGER_CONVERTER =
        new BasicTypeConverter<>(
            BasicType.BIG_INTEGER,
            DataTypes.LongType
        );

    public static final BasicTypeConverter<BigDecimal> BID_DECIMAL_CONVERTER =
        new BasicTypeConverter<>(
            BasicType.BIG_DECIMAL,
            new DecimalType()
        );

    public static final BasicTypeConverter<Instant> INSTANT_CONVERTER =
        new BasicTypeConverter<>(
            BasicType.INSTANT,
            DataTypes.TimestampType
        );

    public static final BasicTypeConverter<Void> NULL_CONVERTER =
        new BasicTypeConverter<>(
            BasicType.NULL,
            DataTypes.NullType
        );

    private final BasicType<T1> seatunnelDataType;
    private final DataType sparkDataType;

    public BasicTypeConverter(BasicType<T1> seatunnelDataType, DataType sparkDataType) {
        this.seatunnelDataType = seatunnelDataType;
        this.sparkDataType = sparkDataType;
    }

    @Override
    public DataType convert(BasicType<T1> seaTunnelDataType) {
        return sparkDataType;
    }
}
