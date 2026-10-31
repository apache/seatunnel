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

package org.apache.seatunnel.connectors.seatunnel.qdrant.sink;

import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.connectors.seatunnel.qdrant.exception.QdrantConnectorException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Guards the payload mapping of the Qdrant sink against silent data loss: a JSON logical value has
 * no Qdrant payload mapping yet, so it must be rejected with an explicit unsupported-type error
 * instead of falling through to the {@code null} branch that drops the column.
 */
class QdrantBatchWriterTest {

    @Test
    void jsonPayloadIsRejectedInsteadOfBeingDropped() {
        QdrantConnectorException exception =
                Assertions.assertThrows(
                        QdrantConnectorException.class,
                        () -> QdrantBatchWriter.buildPayload(BasicType.JSON_TYPE, "{\"a\":1}"));
        Assertions.assertTrue(exception.getMessage().contains("JSON"), exception.getMessage());
        // The legacy STRING path is unchanged, which is also what handle_json_as_string restores.
        Assertions.assertEquals(
                "{\"a\":1}",
                QdrantBatchWriter.buildPayload(BasicType.STRING_TYPE, "{\"a\":1}")
                        .getStringValue());
    }
}
