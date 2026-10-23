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

package org.apache.seatunnel.api.common.error;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Tests for {@link SupportRowLevelErrorClassifier}.
 *
 * <p>The interface must keep downstream subclasses source- and binary-compatible when they do not
 * override {@link SupportRowLevelErrorClassifier#classifyRowError(Throwable, Object)}.
 */
public class SupportRowLevelErrorClassifierTest {

    /** Subclass that does not override {@code classifyRowError}. */
    private static final class NoOverrideSubclass
            implements SupportRowLevelErrorClassifier<String> {}

    /** Subclass that overrides {@code classifyRowError}. */
    private static final class OverridingSubclass
            implements SupportRowLevelErrorClassifier<String> {
        final AtomicBoolean invoked = new AtomicBoolean();

        @Override
        public RowErrorClassification classifyRowError(Throwable t, String row) {
            invoked.set(true);
            return RowErrorClassification.ROW_ERROR;
        }
    }

    @Test
    void defaultImplementationReturnsSystemError() {
        SupportRowLevelErrorClassifier<String> classifier = new NoOverrideSubclass();
        RowErrorClassification classification =
                classifier.classifyRowError(new RuntimeException("boom"), "row");
        Assertions.assertEquals(RowErrorClassification.SYSTEM_ERROR, classification);
    }

    @Test
    void subclassCanStillOverride() {
        OverridingSubclass classifier = new OverridingSubclass();
        RowErrorClassification classification =
                classifier.classifyRowError(new RuntimeException("boom"), "row");
        Assertions.assertTrue(classifier.invoked.get());
        Assertions.assertEquals(RowErrorClassification.ROW_ERROR, classification);
    }
}
