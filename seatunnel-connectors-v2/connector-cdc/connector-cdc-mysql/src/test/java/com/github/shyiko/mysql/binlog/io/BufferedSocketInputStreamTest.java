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

package com.github.shyiko.mysql.binlog.io;

import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class BufferedSocketInputStreamTest {

    @Test
    public void testReadFromBufferedSocketInputStream() throws Exception {
        BufferedSocketInputStream in =
                new BufferedSocketInputStream(
                        new ByteArrayInputStream(
                                new byte[] {'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H'}));
        byte[] buf = new byte[3];
        assertEquals(3, in.read(buf, 0, buf.length));
        Arrays.equals(new byte[] {'A', 'B', 'C'}, buf);
        assertEquals(5, in.available());

        assertEquals(3, in.read(buf, 0, buf.length));
        Arrays.equals(new byte[] {'D', 'E', 'F'}, buf);
        assertEquals(2, in.available());

        assertEquals(2, in.read(buf, 0, buf.length));
        Arrays.equals(new byte[] {'G', 'H'}, buf);
        assertEquals(0, in.available());

        // reach the end of stream normally
        assertEquals(-1, in.read(buf, 0, buf.length));
        assertEquals(0, in.available());
    }
}
