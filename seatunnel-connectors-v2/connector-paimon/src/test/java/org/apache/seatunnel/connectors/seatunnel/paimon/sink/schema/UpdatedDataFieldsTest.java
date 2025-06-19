/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.paimon.sink.schema;

import org.apache.paimon.types.BigIntType;
import org.apache.paimon.types.DecimalType;
import org.apache.paimon.types.DoubleType;
import org.apache.paimon.types.FloatType;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.SmallIntType;
import org.apache.paimon.types.TimeType;
import org.apache.paimon.types.TimestampType;
import org.apache.paimon.types.VarCharType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class UpdatedDataFieldsTest {
    @Test
    public void testCanConvertString() {
        VarCharType oldVarchar = new VarCharType(true, 10);
        VarCharType biggerLengthVarchar = new VarCharType(true, 20);
        VarCharType smallerLengthVarchar = new VarCharType(true, 5);
        IntType intType = new IntType();

        UpdatedDataFields.ConvertAction convertAction;
        convertAction = UpdatedDataFields.canConvert(oldVarchar, biggerLengthVarchar);
        Assertions.assertEquals(UpdatedDataFields.ConvertAction.CONVERT, convertAction);
        convertAction = UpdatedDataFields.canConvert(oldVarchar, smallerLengthVarchar);
        Assertions.assertEquals(UpdatedDataFields.ConvertAction.IGNORE, convertAction);
        convertAction = UpdatedDataFields.canConvert(oldVarchar, intType);

        Assertions.assertEquals(UpdatedDataFields.ConvertAction.EXCEPTION, convertAction);
    }

    @Test
    public void testCanConvertNumber() {
        IntType oldType = new IntType();
        BigIntType bigintType = new BigIntType();
        SmallIntType smallintType = new SmallIntType();

        FloatType floatType = new FloatType();

        UpdatedDataFields.ConvertAction convertAction;
        convertAction = UpdatedDataFields.canConvert(oldType, bigintType);
        Assertions.assertEquals(UpdatedDataFields.ConvertAction.CONVERT, convertAction);
        convertAction = UpdatedDataFields.canConvert(oldType, smallintType);
        Assertions.assertEquals(UpdatedDataFields.ConvertAction.IGNORE, convertAction);
        convertAction = UpdatedDataFields.canConvert(oldType, floatType);

        Assertions.assertEquals(UpdatedDataFields.ConvertAction.EXCEPTION, convertAction);
    }

    @Test
    public void testCanConvertDecimal() {
        DecimalType oldType = new DecimalType(20, 9);
        DecimalType biggerRangeType = new DecimalType(30, 10);
        DecimalType smallerRangeType = new DecimalType(10, 3);
        DoubleType doubleType = new DoubleType();

        UpdatedDataFields.ConvertAction convertAction = null;
        convertAction = UpdatedDataFields.canConvert(oldType, biggerRangeType);
        Assertions.assertEquals(UpdatedDataFields.ConvertAction.CONVERT, convertAction);
        convertAction = UpdatedDataFields.canConvert(oldType, smallerRangeType);
        Assertions.assertEquals(UpdatedDataFields.ConvertAction.IGNORE, convertAction);
        convertAction = UpdatedDataFields.canConvert(oldType, doubleType);

        Assertions.assertEquals(UpdatedDataFields.ConvertAction.EXCEPTION, convertAction);
    }

    @Test
    public void testCanConvertTimestamp() {
        TimestampType oldType = new TimestampType(true, 3);
        TimestampType biggerLengthTimestamp = new TimestampType(true, 5);
        TimestampType smallerLengthTimestamp = new TimestampType(true, 2);
        VarCharType varCharType = new VarCharType();

        UpdatedDataFields.ConvertAction convertAction;
        convertAction = UpdatedDataFields.canConvert(oldType, biggerLengthTimestamp);
        Assertions.assertEquals(UpdatedDataFields.ConvertAction.CONVERT, convertAction);
        convertAction = UpdatedDataFields.canConvert(oldType, smallerLengthTimestamp);
        Assertions.assertEquals(UpdatedDataFields.ConvertAction.IGNORE, convertAction);
        convertAction = UpdatedDataFields.canConvert(oldType, varCharType);

        Assertions.assertEquals(UpdatedDataFields.ConvertAction.EXCEPTION, convertAction);
    }

    @Test
    public void testCanConvertTime() {
        TimeType oldType = new TimeType(true, 3);
        TimeType biggerLengthTimestamp = new TimeType(true, 5);
        TimeType smallerLengthTimestamp = new TimeType(true, 2);
        VarCharType varCharType = new VarCharType();

        UpdatedDataFields.ConvertAction convertAction;
        convertAction = UpdatedDataFields.canConvert(oldType, biggerLengthTimestamp);
        Assertions.assertEquals(UpdatedDataFields.ConvertAction.CONVERT, convertAction);
        convertAction = UpdatedDataFields.canConvert(oldType, smallerLengthTimestamp);
        Assertions.assertEquals(UpdatedDataFields.ConvertAction.IGNORE, convertAction);
        convertAction = UpdatedDataFields.canConvert(oldType, varCharType);

        Assertions.assertEquals(UpdatedDataFields.ConvertAction.EXCEPTION, convertAction);
    }
}
