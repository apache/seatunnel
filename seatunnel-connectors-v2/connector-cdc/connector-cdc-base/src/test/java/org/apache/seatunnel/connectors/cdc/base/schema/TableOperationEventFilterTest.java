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

package org.apache.seatunnel.connectors.cdc.base.schema;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.operation.event.TruncateTableEvent;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class TableOperationEventFilterTest {

    private static TruncateTableEvent truncateEvent() {
        return TruncateTableEvent.of(TableIdentifier.of("", "shop", "products"));
    }

    private static TableOperationEventFilter filter(List<String> include, List<String> exclude) {
        Map<String, Object> map = new HashMap<>();
        map.put("table-operations.include", include);
        map.put("table-operations.exclude", exclude);
        return TableOperationEventFilter.fromConfig(ReadonlyConfig.fromMap(map));
    }

    @Test
    void noConfigIsAllowAll() {
        TableOperationEventFilter f = filter(Collections.emptyList(), Collections.emptyList());
        Assertions.assertTrue(f.isNoOp());
        TruncateTableEvent event = truncateEvent();
        Assertions.assertSame(event, f.filter(event));
    }

    @Test
    void includeKeepsTruncate() {
        TableOperationEventFilter f =
                filter(Collections.singletonList("truncate.table"), Collections.emptyList());
        TruncateTableEvent event = truncateEvent();
        Assertions.assertSame(event, f.filter(event));
    }

    @Test
    void excludeDropsTruncate() {
        TableOperationEventFilter f =
                filter(Collections.emptyList(), Collections.singletonList("truncate.table"));
        Assertions.assertNull(f.filter(truncateEvent()));
    }

    @Test
    void excludeWinsOverInclude() {
        TableOperationEventFilter f =
                filter(
                        Collections.singletonList("truncate.table"),
                        Collections.singletonList("truncate.table"));
        Assertions.assertNull(f.filter(truncateEvent()));
    }

    @Test
    void unknownNameFailsFast() {
        IllegalArgumentException ex =
                Assertions.assertThrows(
                        IllegalArgumentException.class,
                        () -> filter(Arrays.asList("drop.table"), Collections.emptyList()));
        Assertions.assertTrue(ex.getMessage().contains("drop.table"));
        Assertions.assertTrue(ex.getMessage().contains("truncate.table"));
    }

    @Test
    void validateOptionsFailsFastOnUnknownIncludeName() {
        Map<String, Object> map = new HashMap<>();
        map.put("table-operations.include", Arrays.asList("truncate.tabble"));
        IllegalArgumentException ex =
                Assertions.assertThrows(
                        IllegalArgumentException.class,
                        () ->
                                TableOperationEventFilter.validateOptions(
                                        ReadonlyConfig.fromMap(map)));
        Assertions.assertTrue(ex.getMessage().contains("table-operations.include"));
        Assertions.assertTrue(ex.getMessage().contains("truncate.tabble"));
    }
}
