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
package org.apache.seatunnel.connectors.seatunnel.redis.row;

import org.apache.seatunnel.api.table.type.RowKind;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.List;

public class TestForDeleteRows {

    public static List<SeaTunnelRow> getRows() {
        return Arrays.asList(
                getSeaTunnelRowInsert1(),
                getSeaTunnelRowInsert2(),
                getSeaTunnelRowInsert3(),
                getSeaTunnelRowUpdateBefore(),
                getSeaTunnelRowUpdateAfter(),
                getSeaTunnelRowDelete());
    }

    private static SeaTunnelRow getSeaTunnelRowInsert1() {
        return new SeaTunnelRow(
                new Object[] {
                    1,
                    true,
                    (byte) 1,
                    (short) 2,
                    3,
                    4L,
                    4.3f,
                    5.3d,
                    BigDecimal.valueOf(6.3).setScale(1),
                    "NEW",
                    LocalDateTime.parse("2020-02-02T02:02:02")
                });
    }

    private static SeaTunnelRow getSeaTunnelRowInsert2() {
        return new SeaTunnelRow(
                new Object[] {
                    2,
                    true,
                    (byte) 1,
                    (short) 2,
                    3,
                    4L,
                    4.3f,
                    5.3d,
                    BigDecimal.valueOf(6.3).setScale(1),
                    "NEW",
                    LocalDateTime.parse("2020-02-02T02:02:02")
                });
    }

    private static SeaTunnelRow getSeaTunnelRowInsert3() {
        return new SeaTunnelRow(
                new Object[] {
                    3,
                    true,
                    (byte) 1,
                    (short) 2,
                    3,
                    4L,
                    4.3f,
                    5.3d,
                    BigDecimal.valueOf(6.3).setScale(1),
                    "NEW",
                    LocalDateTime.parse("2020-02-02T02:02:02")
                });
    }

    private static SeaTunnelRow getSeaTunnelRowUpdateBefore() {
        final SeaTunnelRow seaTunnelRow =
                new SeaTunnelRow(
                        new Object[] {
                            1,
                            true,
                            (byte) 1,
                            (short) 2,
                            3,
                            4L,
                            4.3f,
                            5.3d,
                            BigDecimal.valueOf(6.3).setScale(1),
                            "NEW",
                            LocalDateTime.parse("2020-02-02T02:02:02")
                        });
        seaTunnelRow.setRowKind(RowKind.UPDATE_BEFORE);
        return seaTunnelRow;
    }

    private static SeaTunnelRow getSeaTunnelRowUpdateAfter() {
        final SeaTunnelRow seaTunnelRow =
                new SeaTunnelRow(
                        new Object[] {
                            1,
                            true,
                            (byte) 2,
                            (short) 2,
                            3,
                            4L,
                            4.3f,
                            5.3d,
                            BigDecimal.valueOf(6.3).setScale(1),
                            "NEW",
                            LocalDateTime.parse("2020-02-02T02:02:02")
                        });
        seaTunnelRow.setRowKind(RowKind.UPDATE_AFTER);
        return seaTunnelRow;
    }

    private static SeaTunnelRow getSeaTunnelRowDelete() {
        final SeaTunnelRow seaTunnelRow =
                new SeaTunnelRow(
                        new Object[] {
                            2,
                            true,
                            (byte) 1,
                            (short) 2,
                            3,
                            4L,
                            4.3f,
                            5.3d,
                            BigDecimal.valueOf(6.3).setScale(1),
                            "NEW",
                            LocalDateTime.parse("2020-02-02T02:02:02")
                        });
        seaTunnelRow.setRowKind(RowKind.DELETE);
        return seaTunnelRow;
    }
}
