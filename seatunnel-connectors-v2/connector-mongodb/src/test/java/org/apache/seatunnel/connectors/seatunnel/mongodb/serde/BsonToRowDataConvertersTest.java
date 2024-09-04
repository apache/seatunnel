package org.apache.seatunnel.connectors.seatunnel.mongodb.serde;

import org.apache.seatunnel.api.table.type.BasicType;

import org.bson.BsonDouble;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class BsonToRowDataConvertersTest {
    private final BsonToRowDataConverters converterFactory = new BsonToRowDataConverters();

    @Test
    public void testConvertAnyNumberToDouble() {
        // It covered #6997
        BsonToRowDataConverters.BsonToRowDataConverter converter =
                converterFactory.createConverter(BasicType.DOUBLE_TYPE);

        Assertions.assertEquals(1.0d, converter.convert(new BsonInt32(1)));
        Assertions.assertEquals(1.0d, converter.convert(new BsonInt64(1L)));

        Assertions.assertEquals(4.0d, converter.convert(new BsonDouble(4.0d)));
        Assertions.assertEquals(4.4d, converter.convert(new BsonDouble(4.4d)));
    }

    @Test
    public void testConvertBsonIntToBigInt() {
        // It covered #7567
        BsonToRowDataConverters.BsonToRowDataConverter converter =
                converterFactory.createConverter(BasicType.LONG_TYPE);

        Assertions.assertEquals(123456L, converter.convert(new BsonInt32(123456)));

        Assertions.assertEquals(
                (long) Integer.MAX_VALUE, converter.convert(new BsonInt64(Integer.MAX_VALUE)));
    }
}
