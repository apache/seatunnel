package org.apache.seatunnel.format.sensorsdata;

import org.apache.commons.lang3.StringUtils;

import lombok.Getter;

/**
 * TODO
 *
 * @author chwang
 * @version 1.0.0
 * @since 2024/03/25 15:00
 */
public class SensorsDataTypes {
    public enum DataTypes {
        UNKNOWN,
        BOOLEAN,
        DECIMAL,
        INT,
        BIGINT,
        FLOAT,
        DOUBLE,
        NUMBER,
        STRING,
        DATE,
        TIMESTAMP,
        LIST,
        LIST_COMMA,
        LIST_SEMICOLON;

        public static DataTypes of(String s) {
            String str = StringUtils.upperCase(StringUtils.trim(s));
            if (StringUtils.isBlank(str)) {
                return DataTypes.UNKNOWN;
            }
            if (StringUtils.startsWith(str, "TIMESTAMP")) {
                // TIMESTAMP 可以带时区, 参见 TypeUtilTest
                return DataTypes.TIMESTAMP;
            }
            switch (str) {
                case "BOOLEAN":
                    return DataTypes.BOOLEAN;
                case "DECIMAL":
                    return DataTypes.DECIMAL;
                case "INT":
                    return DataTypes.INT;
                case "BIGINT":
                case "LONG":
                    return DataTypes.BIGINT;
                case "FLOAT":
                    return DataTypes.FLOAT;
                case "DOUBLE":
                    return DataTypes.DOUBLE;
                case "NUMBER":
                    return DataTypes.NUMBER;
                case "LIST":
                    return DataTypes.LIST;
                case "LIST_COMMA":
                    return DataTypes.LIST_COMMA;
                case "LIST_SEMICOLON":
                    return DataTypes.LIST_SEMICOLON;
                case "DATE":
                    return DataTypes.DATE;
                case "STRING":
                    return DataTypes.STRING;
                default:
                    return DataTypes.UNKNOWN;
            }
        }
    }

    @Getter private final DataTypes type;
    @Getter private final String extra;

    SensorsDataTypes(DataTypes type, String extra) {
        this.type = type;
        this.extra = extra;
    }

    public static SensorsDataTypes of(String str) {
        DataTypes type = DataTypes.of(str);
        String suffix =
                StringUtils.length(str) > type.name().length()
                        ? StringUtils.trim(StringUtils.substring(str, type.name().length()))
                        : null;
        return new SensorsDataTypes(type, suffix);
    }
}
