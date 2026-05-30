package org.apache.seatunnel.azuredataexplorer.serialization;

import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.azuredataexplorer.exception.AzureDataExplorerConnectorException;
import org.apache.seatunnel.azuredataexplorer.exception.AzureDataExplorerErrorCode;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/**
 * Serializes a SeaTunnelRow to an RFC-4180 CSV line for ADX ingestion. Column order matches the
 * SeaTunnelRowType field order. Null fields are emitted as empty (ADX treats empty as null for most
 * types).
 */
public class AzureDataExplorerRowSerializer {

    private static final DateTimeFormatter DT_FMT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS");
    private static final DateTimeFormatter DATE_FMT = DateTimeFormatter.ofPattern("yyyy-MM-dd");

    private final SeaTunnelRowType rowType;

    public AzureDataExplorerRowSerializer(SeaTunnelRowType rowType) {
        this.rowType = rowType;
    }

    /** Returns a CSV line (including trailing newline) for the given row. */
    public String toCsvLine(SeaTunnelRow row) {
        SeaTunnelDataType<?>[] types = rowType.getFieldTypes();
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < types.length; i++) {
            if (i > 0) sb.append(',');
            Object val = row.getField(i);
            if (val != null) {
                sb.append(serializeField(val, types[i]));
            }
            // null => empty field
        }
        sb.append('\n');
        return sb.toString();
    }

    private String serializeField(Object val, SeaTunnelDataType<?> type) {
        if (type == BasicType.BOOLEAN_TYPE) return val.toString();
        if (type == BasicType.BYTE_TYPE) return Byte.toString((Byte) val);
        if (type == BasicType.SHORT_TYPE) return Short.toString((Short) val);
        if (type == BasicType.INT_TYPE) return Integer.toString((Integer) val);
        if (type == BasicType.LONG_TYPE) return Long.toString((Long) val);
        if (type == BasicType.FLOAT_TYPE) return Float.toString((Float) val);
        if (type == BasicType.DOUBLE_TYPE) return Double.toString((Double) val);
        if (type == BasicType.STRING_TYPE) return csvQuote(val.toString());
        if (type instanceof DecimalType) return val.toString();
        if (type instanceof LocalTimeType) {
            if (type == LocalTimeType.LOCAL_DATE_TIME_TYPE)
                return ((LocalDateTime) val).format(DT_FMT);
            if (type == LocalTimeType.LOCAL_DATE_TYPE) return ((LocalDate) val).format(DATE_FMT);
            return csvQuote(val.toString());
        }
        throw new AzureDataExplorerConnectorException(
                AzureDataExplorerErrorCode.UNSUPPORTED_DATA_TYPE,
                "Cannot serialize type: " + type.getSqlType());
    }

    /** RFC-4180: quote if value contains comma, double-quote, CR, or LF. */
    static String csvQuote(String s) {
        if (s.indexOf(',') >= 0
                || s.indexOf('"') >= 0
                || s.indexOf('\r') >= 0
                || s.indexOf('\n') >= 0) {
            return "\"" + s.replace("\"", "\"\"\"") + "\"";
        }
        return s;
    }
}
