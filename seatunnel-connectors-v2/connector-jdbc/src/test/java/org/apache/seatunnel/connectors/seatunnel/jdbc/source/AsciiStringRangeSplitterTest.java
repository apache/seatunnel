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

package org.apache.seatunnel.connectors.seatunnel.jdbc.source;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class AsciiStringRangeSplitterTest {

    @Test
    public void testSplitFixedLengthPrintableAsciiRange() {
        assertArrayEquals(
                new String[] {"aa", "ff", "kk"}, AsciiStringRangeSplitter.split("aa", "kk", 2));
    }

    @Test
    public void testSplitNumericSuffixKeepsGeneratedBoundariesPrintable() {
        assertDoesNotThrow(() -> AsciiStringRangeSplitter.split("key00000", "key00099", 11));
    }

    @Test
    public void testRejectVariableLengthRange() {
        assertThrows(
                IllegalArgumentException.class, () -> AsciiStringRangeSplitter.split("aa", "z", 2));
    }

    @Test
    public void testRejectNonPrintableAsciiRange() {
        assertThrows(
                IllegalArgumentException.class,
                () -> AsciiStringRangeSplitter.split("aa", "中中", 2));
    }
}
