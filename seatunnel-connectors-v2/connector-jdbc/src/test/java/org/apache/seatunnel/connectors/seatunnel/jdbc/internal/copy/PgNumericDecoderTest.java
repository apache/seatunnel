package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.copy;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class PgNumericDecoderTest {

    @Test
    public void testDecode() {
        // 123.45
        ByteBuffer buf1 = ByteBuffer.allocate(12);
        buf1.putShort((short) 2); // ndigits = 2
        buf1.putShort((short) 0); // weight = 0
        buf1.putShort((short) 0); // sign = 0
        buf1.putShort((short) 2); // dscale = 2
        buf1.putShort((short) 123); // digits[0] = 1
        buf1.putShort((short) 4500); // digits[1] = 2345
        buf1.flip();
        assertEquals(new BigDecimal("123.45"), PgNumericDecoder.decode(buf1));

        // -123.45
        ByteBuffer buf2 = ByteBuffer.allocate(12);
        buf2.putShort((short) 2); // ndigits = 2
        buf2.putShort((short) 0); // weight = 0
        buf2.putShort((short) 0x4000); // sign = 0x4000
        buf2.putShort((short) 2); // dscale = 2
        buf2.putShort((short) 123); // digits[0] = 1
        buf2.putShort((short) 4500); // digits[1] = 2345
        buf2.flip();
        assertEquals(new BigDecimal("-123.45"), PgNumericDecoder.decode(buf2));

        // 0
        ByteBuffer buf3 = ByteBuffer.allocate(8);
        buf3.putShort((short) 0); // ndigits = 0
        buf3.putShort((short) 0); // weight = 0
        buf3.putShort((short) 0); // sign = 0
        buf3.putShort((short) 2); // dscale = 2
        buf3.flip();
        assertEquals(new BigDecimal("0.00"), PgNumericDecoder.decode(buf3));

        // 12345
        // 12345 = 1 * 10000^1 + 2345 * 10000^0
        ByteBuffer buf4 = ByteBuffer.allocate(12);
        buf4.putShort((short) 2); // ndigits = 2
        buf4.putShort((short) 1); // weight = 1
        buf4.putShort((short) 0); // sign = 0
        buf4.putShort((short) 0); // dscale = 0
        buf4.putShort((short) 1); // digits[0] = 1
        buf4.putShort((short) 2345); // digits[1] = 2345
        buf4.flip();
        assertEquals(new BigDecimal("12345"), PgNumericDecoder.decode(buf4));

        // NaN
        ByteBuffer buf5 = ByteBuffer.allocate(8);
        buf5.putShort((short) 0); // ndigits = 0
        buf5.putShort((short) 0); // weight = 0
        buf5.putShort((short) 0xC000); // sign = 0xC000 (NaN)
        buf5.putShort((short) 0); // dscale = 0
        buf5.flip();
        try {
            PgNumericDecoder.decode(buf5);
        } catch (Exception e) {
            assertEquals(
                    "ErrorCode:[COMMON-05], ErrorDescription:[Unsupported operation] - PostgreSQL NUMERIC value is NaN, not supported.",
                    e.getMessage());
        }

        // 123.4567 (dscale=4)
        ByteBuffer buf6 = ByteBuffer.allocate(12);
        buf6.putShort((short) 2); // ndigits = 2
        buf6.putShort((short) 0); // weight = 0
        buf6.putShort((short) 0); // sign = 0
        buf6.putShort((short) 4); // dscale = 4
        buf6.putShort((short) 123); // digits[0] = 123
        buf6.putShort((short) 4567); // digits[1] = 4567
        buf6.flip();
        assertEquals(new BigDecimal("123.4567"), PgNumericDecoder.decode(buf6));

        // High weight 1 × 10000^3 = 1_0000_0000_0000
        ByteBuffer buf7 = ByteBuffer.allocate(10);
        buf7.putShort((short) 1); // ndigits = 1
        buf7.putShort((short) 3); // weight = 3
        buf7.putShort((short) 0); // sign = 0
        buf7.putShort((short) 0); // dscale = 0
        buf7.putShort((short) 1); // digits[0] = 1
        buf7.flip();
        assertEquals(new BigDecimal("1000000000000"), PgNumericDecoder.decode(buf7));

        // Small value 0.000123
        // 0.000123
        ByteBuffer buf8 = ByteBuffer.allocate(12);
        buf8.putShort((short) 2); // ndigits = 2
        buf8.putShort((short) -1); // weight = -1
        buf8.putShort((short) 0); // sign = 0
        buf8.putShort((short) 6); // dscale = 6
        buf8.putShort((short) 1); // digits[0] = 1
        buf8.putShort((short) 2300); // digits[1] = 2300
        buf8.flip();
        assertEquals(new BigDecimal("0.000123"), PgNumericDecoder.decode(buf8));

        // Very large number 1 × 10000^10
        ByteBuffer buf9 = ByteBuffer.allocate(10);
        buf9.putShort((short) 1); // ndigits = 1
        buf9.putShort((short) 10); // weight = 10
        buf9.putShort((short) 0); // sign = 0
        buf9.putShort((short) 0); // dscale = 0
        buf9.putShort((short) 1); // digits[0] = 1
        buf9.flip();
        assertEquals(
                new BigDecimal("10000000000000000000000000000000000000000"),
                PgNumericDecoder.decode(buf9));

        // Trailing zeros 0.000100
        ByteBuffer buf10 = ByteBuffer.allocate(10);
        buf10.putShort((short) 1); // ndigits = 1
        buf10.putShort((short) -1); // weight = -1
        buf10.putShort((short) 0); // sign = 0
        buf10.putShort((short) 6); // dscale = 6
        buf10.putShort((short) 1); // digits[0] = 100
        buf10.flip();
        assertEquals(new BigDecimal("0.000100"), PgNumericDecoder.decode(buf10));

        ByteBuffer buf11 = ByteBuffer.allocate(12);
        buf11.putShort((short) 2); // ndigits = 2
        buf11.putShort((short) 2); // weight = 2
        buf11.putShort((short) 0); // sign = 0
        buf11.putShort((short) 0); // dscale = 0
        buf11.putShort((short) 1); // digits[0] = 1
        buf11.putShort((short) 0); // digits[1] = 0
        buf11.flip();
        assertEquals(new BigDecimal("100000000"), PgNumericDecoder.decode(buf11));

        ByteBuffer buf12 = ByteBuffer.allocate(10);
        buf12.putShort((short) 1); // ndigits = 1
        buf12.putShort((short) -1); // weight = -1
        buf12.putShort((short) 0); // sign = 0
        buf12.putShort((short) 4); // dscale = 4
        buf12.putShort((short) 1); // digits[0] = 1
        buf12.flip();
        assertEquals(new BigDecimal("0.0001"), PgNumericDecoder.decode(buf12));
    }
}
