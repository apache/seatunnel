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

package org.apache.seatunnel.transform.calcite.udf;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class MaskFunctionTest {

    @Test
    void testNormalMask() {
        Assertions.assertEquals("138****5678", MaskFunction.eval("13812345678", 3, 7, "*"));
    }

    @Test
    void testNullInput() {
        Assertions.assertNull(MaskFunction.eval(null, 0, 3, "*"));
    }

    @Test
    void testInvalidRange() {
        Assertions.assertEquals("source", MaskFunction.eval("source", 6, 3, "*"));
        Assertions.assertEquals("source", MaskFunction.eval("source", -1, 3, "*"));
    }

    @Test
    void testCustomMaskChar() {
        Assertions.assertEquals("ze##!", MaskFunction.eval("zeta!", 2, 4, "#"));
    }

    @Test
    void testEmptyMaskChar() {
        Assertions.assertEquals("s**k", MaskFunction.eval("sink", 1, 3, ""));
    }

    @Test
    void testEndExceedsLength() {
        Assertions.assertEquals("source", MaskFunction.eval("source", 0, 10, "*"));
    }

    @Test
    void testNullMaskChar() {
        Assertions.assertEquals("s**k", MaskFunction.eval("sink", 1, 3, null));
    }

    @Test
    void testFunctionName() {
        MaskFunction fn = new MaskFunction();
        Assertions.assertEquals("MASK", fn.functionName());
    }
}
