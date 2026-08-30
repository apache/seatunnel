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

package org.apache.seatunnel.core.starter.utils;

import org.apache.seatunnel.core.starter.command.ParameterSplitter;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class ParameterSplitterTest {

    ParameterSplitter parameterSplitter = new ParameterSplitter();

    @Test
    void testQuotedBracesAndCommas() {
        String input = "props={\"a\":\"}\",\"b\":\"x\"},other=1";
        String[] expected = {"props={\"a\":\"}\",\"b\":\"x\"}", "other=1"};
        assertArrayEquals(expected, parameterSplitter.split(input).toArray());
    }

    @Test
    void testNestedBracesAndBrackets() {
        String input = "{b={c=1}}";
        String[] expected = {"{b={c=1}}"};
        assertArrayEquals(expected, parameterSplitter.split(input).toArray());
    }

    @Test
    void testEscapedQuotes() {
        String input =
                "json={\"key\":\"value with \\\"${var}\\\" inside\",\"path\":\"D:\\data\\file\\\\\"},next=2";
        String[] expected = {
            "json={\"key\":\"value with \\\"${var}\\\" inside\",\"path\":\"D:\\data\\file\\\\\"}",
            "next=2"
        };
        assertArrayEquals(expected, parameterSplitter.split(input).toArray());
    }

    @Test
    public void testEscapedQuoteFollowedByComma() {
        String input = "{\"note\":\"he said,\\\"stop,\\\", then left\"}";

        assertEquals(input, parameterSplitter.split(input).toArray()[0]);
    }

    @Test
    public void testEscapedQuoteFollowedByBrace() {
        String input = "{\"note\":\"he said \\\"}stop{\\\"} then left\"}";

        assertEquals(input, parameterSplitter.split(input).toArray()[0]);
    }

    @Test
    public void testEscapedQuoteFollowedByBracket() {
        String input = "{\"note\":\"he said \\\"]stop]\\\"[ then left\"}";

        assertEquals(input, parameterSplitter.split(input).toArray()[0]);
    }

    @Test
    public void testEscapedQuoteFollowedByEqual() {
        String input = "{\"note\":\"he said \\\"=stop=\\\" then left\"}";

        assertEquals(input, parameterSplitter.split(input).toArray()[0]);
    }

    @Test
    public void testEscapedQuoteFollowedByColon() {
        String input = "{\"note\":\"he said: \\\":stop:\\\" then left\"}";

        assertEquals(input, parameterSplitter.split(input).toArray()[0]);
    }

    @Test
    void testArrayWithCommas() {
        String input = "arr=[1,2,3],other=4";
        String[] expected = {"arr=[1,2,3]", "other=4"};
        assertArrayEquals(expected, parameterSplitter.split(input).toArray());
    }

    @Test
    void testArrayAndObjectMixed() {
        String input =
                "json_data={\"a\":\"}\",\"b\":\"{xyz}\",\"c\":[{\"k1\":\"v1\"},{\"k2\":\"v2\",\"k3\":\"v3\"}],\"d\":{\"list\":[{\"k1\":\"v1\"},{\"k2\":\"v2\",\"k3\":\"v3\"}]}},extra=5";
        String[] expected = {
            "json_data={\"a\":\"}\",\"b\":\"{xyz}\",\"c\":[{\"k1\":\"v1\"},{\"k2\":\"v2\",\"k3\":\"v3\"}],\"d\":{\"list\":[{\"k1\":\"v1\"},{\"k2\":\"v2\",\"k3\":\"v3\"}]}}",
            "extra=5"
        };
        assertArrayEquals(expected, parameterSplitter.split(input).toArray());
    }

    @Test
    void testPlain() {
        String input = "a=1,b=2,c=3";
        String[] expected = {"a=1", "b=2", "c=3"};
        assertArrayEquals(expected, parameterSplitter.split(input).toArray());
    }
}
