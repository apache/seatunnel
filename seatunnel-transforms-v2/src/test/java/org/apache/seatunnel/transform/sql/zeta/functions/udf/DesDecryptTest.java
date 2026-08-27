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

package org.apache.seatunnel.transform.sql.zeta.functions.udf;

import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

public class DesDecryptTest {

    @Test
    public void testFunctionNameAndResultType() {
        DesDecrypt udf = new DesDecrypt();
        Assertions.assertEquals("DES_DECRYPT", udf.functionName());

        List<SeaTunnelDataType<?>> argTypes =
                Arrays.asList(BasicType.STRING_TYPE, BasicType.STRING_TYPE);
        Assertions.assertEquals(BasicType.STRING_TYPE, udf.resultType(argTypes));
    }

    @Test
    public void testEvaluateDecryptsWithValidArguments() {
        DesDecrypt udf = new DesDecrypt();
        String password = "password123";
        String plain = "hello-decrypt";

        String cipher = DESUtil.encrypt(password, plain);

        List<Object> args = Arrays.asList(password, cipher);
        Object result = udf.evaluate(args);
        Assertions.assertTrue(result instanceof String);
        Assertions.assertEquals(plain, result);
    }

    @Test
    public void testEvaluateReturnsNullWhenPasswordOrDataIsNull() {
        DesDecrypt udf = new DesDecrypt();

        Assertions.assertNull(udf.evaluate(Arrays.asList(null, "data")));
        Assertions.assertNull(udf.evaluate(Arrays.asList("password123", null)));
    }
}
