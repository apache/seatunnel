package org.apache.seatunnel.connectors.seatunnel.redis.row;

import org.apache.seatunnel.api.table.type.RowKind;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.List;

public class TestKeyOrValueIsNullRows {

    public static List<SeaTunnelRow> getRows() {
        return Arrays.asList(
                getSeaTunnelRowWithStringNullInsert1(),
                getSeaTunnelRowInsert2(),
                getSeaTunnelRowInsert3(),
                getSeaTunnelRowWithStringNullUpdateBefore(),
                getSeaTunnelRowWithStringNullUpdateAfter(),
                getSeaTunnelRowWithStringNullDelete());
    }

    private static SeaTunnelRow getSeaTunnelRowWithStringNullInsert1() {
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
                    null,
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
                    "NEW2",
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
                    "NEW3",
                    LocalDateTime.parse("2020-02-02T02:02:02")
                });
    }

    private static SeaTunnelRow getSeaTunnelRowWithStringNullUpdateBefore() {
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
                            null,
                            LocalDateTime.parse("2020-02-02T02:02:02")
                        });
        seaTunnelRow.setRowKind(RowKind.UPDATE_BEFORE);
        return seaTunnelRow;
    }

    private static SeaTunnelRow getSeaTunnelRowWithStringNullUpdateAfter() {
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
                            null,
                            LocalDateTime.parse("2020-02-02T02:02:02")
                        });
        seaTunnelRow.setRowKind(RowKind.UPDATE_AFTER);
        return seaTunnelRow;
    }

    private static SeaTunnelRow getSeaTunnelRowWithStringNullDelete() {
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
                            null,
                            LocalDateTime.parse("2020-02-02T02:02:02")
                        });
        seaTunnelRow.setRowKind(RowKind.DELETE);
        return seaTunnelRow;
    }
}
