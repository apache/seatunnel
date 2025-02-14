package org.apache.seatunnel.format.sensorsdata.utils;

import org.apache.seatunnel.format.sensorsdata.exception.SensorsDataException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Date;

/**
 * TODO
 *
 * @author chwang
 * @version 1.0.0
 * @since 2024/03/16 16:08
 */
class TypeUtilTest {

    @Test
    void testToTargetType() {
        // 1. Number
        Assertions.assertEquals(123, TypeUtil.toTargetType(123, "NUMBER"));
        Assertions.assertEquals(123L, TypeUtil.toTargetType(123L, "NUMBER"));
        Assertions.assertEquals(123.1, TypeUtil.toTargetType(123.1, "NUMBER"));
        Assertions.assertEquals(123, TypeUtil.toTargetType("123", "NUMBER"));
        Assertions.assertEquals(
                ((Double) 123.1).floatValue(), TypeUtil.toTargetType("123.1", "NUMBER"));
        // 2. Boolean
        Assertions.assertEquals(true, TypeUtil.toTargetType(1, "BOOLEAN"));
        Assertions.assertEquals(false, TypeUtil.toTargetType(0, "BOOLEAN"));
        Assertions.assertEquals(false, TypeUtil.toTargetType(0.0, "BOOLEAN"));
        Assertions.assertEquals(true, TypeUtil.toTargetType("true", "BOOLEAN"));
        Assertions.assertEquals(false, TypeUtil.toTargetType("f", "BOOLEAN"));
        // 3. Timestamp
        Assertions.assertEquals(
                1710588307000L, TypeUtil.toTargetType("2024-03-16 19:25:07", "TIMESTAMP"));
        Assertions.assertEquals(
                1710588307000L, TypeUtil.toTargetType(new Date(1710588307000L), "TIMESTAMP"));
        Assertions.assertEquals(
                1710588307000.0, TypeUtil.toTargetType(1710588307000.0, "TIMESTAMP"));
        Assertions.assertEquals(
                1710588307000L, TypeUtil.toTargetType("20240316_192507", "TIMESTAMP"));
        Assertions.assertEquals(
                1710588307123L, TypeUtil.toTargetType("2024-03-16 19:25:07.123", "TIMESTAMP"));
        Assertions.assertEquals(
                1710613507000L,
                TypeUtil.toTargetType(
                        "2024-03-16T19:25:07+0100", "TIMESTAMP yyyy-MM-dd'T'HH:mm:ssZ"));
        Assertions.assertEquals(
                "20240316 192507", TypeUtil.toTargetType("20240316 192507", "TIMESTAMP"));

        // 4. List
        Assertions.assertEquals(
                Arrays.asList("123", "456"), TypeUtil.toTargetType("123\n456", "LIST"));
        Assertions.assertEquals(
                Arrays.asList("123", "456"), TypeUtil.toTargetType("123,456", "LIST_COMMA"));
        Assertions.assertEquals(
                Collections.singletonList("456"), TypeUtil.toTargetType(";456", "LIST_SEMICOLON"));
        Assertions.assertEquals(
                Collections.singletonList("123"), TypeUtil.toTargetType("123", "LIST"));
        Assertions.assertThrowsExactly(
                SensorsDataException.class, () -> TypeUtil.toTargetType(123, "LIST"));
    }
}
