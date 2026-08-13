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

package org.apache.seatunnel.core.starter.flink.execution;

import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.sink.SupportSchemaEvolutionSink;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SchemaEvolutionRoutingTest {

    @Test
    void testSchemaCapableSinkIsNotRoutedWhenSourceProtocolIsDisabled() {
        DataStreamTableInfo stream =
                new DataStreamTableInfo(null, Collections.emptyList(), "source-output");
        SeaTunnelSink sink =
                Mockito.mock(
                        SeaTunnelSink.class,
                        Mockito.withSettings().extraInterfaces(SupportSchemaEvolutionSink.class));

        assertFalse(SchemaEvolutionRouting.isRequired(true, stream, sink));
    }

    @Test
    void testRoutingRequiresStreamingProtocolAndCapableSink() {
        DataStreamTableInfo stream =
                new DataStreamTableInfo(null, Collections.emptyList(), "source-output", true);
        SeaTunnelSink capableSink =
                Mockito.mock(
                        SeaTunnelSink.class,
                        Mockito.withSettings().extraInterfaces(SupportSchemaEvolutionSink.class));
        SeaTunnelSink plainSink = Mockito.mock(SeaTunnelSink.class);

        assertTrue(SchemaEvolutionRouting.isRequired(true, stream, capableSink));
        assertFalse(SchemaEvolutionRouting.isRequired(false, stream, capableSink));
        assertFalse(SchemaEvolutionRouting.isRequired(true, stream, plainSink));
    }
}
