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

public class DesEncryptTest {

    @Test
    public void testFunctionNameAndResultType() {
        DesEncrypt udf = new DesEncrypt();
        Assertions.assertEquals("DES_ENCRYPT", udf.functionName());

        List<SeaTunnelDataType<?>> argTypes =
                Arrays.asList(BasicType.STRING_TYPE, BasicType.STRING_TYPE);
        Assertions.assertEquals(BasicType.STRING_TYPE, udf.resultType(argTypes));
    }

    @Test
    public void testEvaluateEncryptsWithValidArguments() {
        DesEncrypt udf = new DesEncrypt();
        String password = "password123";
        String plain = "hello-udf";

        List<Object> args = Arrays.asList(password, plain);
        Object result = udf.evaluate(args);
        Assertions.assertTrue(result instanceof String);

        String decrypted = DESUtil.decrypt(password, (String) result);
        Assertions.assertEquals(plain, decrypted);
    }

    @Test
    public void testEvaluateReturnsNullWhenPasswordOrDataIsNull() {
        DesEncrypt udf = new DesEncrypt();
        Assertions.assertNull(udf.evaluate(Arrays.asList(null, "data")));
        Assertions.assertNull(udf.evaluate(Arrays.asList("password123", null)));
    }
}
