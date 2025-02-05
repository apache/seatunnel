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

package org.apache.seatunnel.transform.sql.zeta;

import org.apache.seatunnel.transform.sql.zeta.functions.StringFunction;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class ConcatWsFunctionTest {

    @Test
    public void testConcatWs() {
        Assertions.assertEquals("", StringFunction.concatWs(genArgs(";", new String[] {})));
        Assertions.assertEquals("", StringFunction.concatWs(genArgs(null, new String[] {})));
        Assertions.assertEquals(
                "a;b", StringFunction.concatWs(genArgs(";", new String[] {"a", "b"})));
        Assertions.assertEquals(
                "a;b", StringFunction.concatWs(genArgs(";", new String[] {"a", null, "b"})));
        Assertions.assertEquals(
                "ab",
                StringFunction.concatWs(genArgs("", new String[] {null, "a", null, "b", null})));
        Assertions.assertEquals(
                "ab", StringFunction.concatWs(genArgs(null, new String[] {"a", "b", null})));
        Assertions.assertEquals(
                "a;b;c", StringFunction.concatWs(genArgs(";", new String[] {"a", "b"}, "c")));
        Assertions.assertEquals(
                "a;b", StringFunction.concatWs(genArgs(";", new String[] {"a", "b"}, null)));
        Assertions.assertEquals(
                "a;b;1;2",
                StringFunction.concatWs(
                        genArgs(";", new String[] {"a", "b"}, new String[] {"1", "2"})));
    }

    public List<Object> genArgs(String separator, String[] arr) {
        List<Object> list = new ArrayList<>();
        list.add(separator);
        list.add(arr);
        return list;
    }

    public List<Object> genArgs(String separator, Object... arr) {
        List<Object> list = new ArrayList<>();
        list.add(separator);
        Collections.addAll(list, arr);
        return list;
    }
}
