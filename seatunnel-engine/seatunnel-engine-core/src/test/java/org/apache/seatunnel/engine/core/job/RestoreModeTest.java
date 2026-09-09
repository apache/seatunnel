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

package org.apache.seatunnel.engine.core.job;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class RestoreModeTest {

    @Test
    void restoreModeCodes_shouldRemainStable() {
        Assertions.assertEquals(0, RestoreMode.NONE.getCode());
        Assertions.assertEquals(1, RestoreMode.SAVEPOINT.getCode());
        Assertions.assertEquals(2, RestoreMode.CHECKPOINT.getCode());
    }

    @Test
    void fromCode_shouldResolveStableProtocolCodes() {
        Assertions.assertEquals(RestoreMode.NONE, RestoreMode.fromCode(0));
        Assertions.assertEquals(RestoreMode.SAVEPOINT, RestoreMode.fromCode(1));
        Assertions.assertEquals(RestoreMode.CHECKPOINT, RestoreMode.fromCode(2));
    }

    @Test
    void fromCode_shouldRejectUnknownCode() {
        IllegalArgumentException exception =
                Assertions.assertThrows(
                        IllegalArgumentException.class, () -> RestoreMode.fromCode(999));
        Assertions.assertTrue(exception.getMessage().contains("Unknown restore mode code"));
    }
}
