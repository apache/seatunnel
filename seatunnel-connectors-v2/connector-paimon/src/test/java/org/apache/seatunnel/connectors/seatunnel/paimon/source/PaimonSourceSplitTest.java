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

package org.apache.seatunnel.connectors.seatunnel.paimon.source;

import org.apache.paimon.table.source.Split;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

class PaimonSourceSplitTest {

    @Test
    void splitIdentityDoesNotDependOnUnderlyingRepresentation() {
        PaimonSourceSplit first =
                new PaimonSourceSplit("0000000001", "db.table", new TestSplit("executor-a"));
        PaimonSourceSplit second =
                new PaimonSourceSplit("0000000001", "db.table", new TestSplit("executor-b"));
        PaimonSourceSplit next =
                new PaimonSourceSplit("0000000002", "db.table", new TestSplit("executor-a"));

        assertEquals("0000000001", first.splitId());
        assertEquals(first.splitId(), second.splitId());
        assertNotEquals(first.splitId(), next.splitId());
    }

    @Test
    void splitIdentitySurvivesSerialization() throws Exception {
        PaimonSourceSplit original =
                new PaimonSourceSplit("0000000001", "db.table", new TestSplit("executor-a"));
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ObjectOutputStream output = new ObjectOutputStream(bytes)) {
            output.writeObject(original);
        }
        try (ObjectInputStream input =
                new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
            PaimonSourceSplit restored = (PaimonSourceSplit) input.readObject();
            assertEquals("0000000001", restored.splitId());
            assertEquals(original.splitId(), restored.splitId());
            assertEquals(original.getTableId(), restored.getTableId());
        }
    }

    private static class TestSplit implements Split {
        private static final long serialVersionUID = 1L;
        private final String representation;

        private TestSplit(String representation) {
            this.representation = representation;
        }

        @Override
        public long rowCount() {
            return 1;
        }

        @Override
        public String toString() {
            return representation;
        }
    }
}
