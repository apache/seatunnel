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

package org.apache.seatunnel.connectors.doris.sink.writer;

import org.apache.seatunnel.connectors.doris.exception.DorisConnectorException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

public class DorisNodeResolverTest {

    @Test
    void testParseNodesTrimWhitespace() {
        List<String> nodes = DorisNodeResolver.parseNodes(" be1:8040 , be2:8041 ", "benodes");

        Assertions.assertEquals(Arrays.asList("be1:8040", "be2:8041"), nodes);
    }

    @Test
    void testParseNodesRejectBlankEntry() {
        DorisConnectorException exception =
                Assertions.assertThrows(
                        DorisConnectorException.class,
                        () -> DorisNodeResolver.parseNodes("be1:8040,,be2:8041", "benodes"));

        Assertions.assertTrue(exception.getMessage().contains("benodes"));
    }

    @Test
    void testParseNodesRejectInvalidPort() {
        DorisConnectorException exception =
                Assertions.assertThrows(
                        DorisConnectorException.class,
                        () -> DorisNodeResolver.parseNodes("be1:abc", "benodes"));

        Assertions.assertTrue(exception.getMessage().contains("benodes"));
        Assertions.assertTrue(exception.getMessage().contains("be1:abc"));
    }
}
