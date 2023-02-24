package org.apache.seatunnel.connectors.seatunnel.starrocks.util;

import lombok.Getter;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class CreateTableParser {

    private static final Pattern COLUMN_PATTERN = Pattern.compile("`?(\\w+)`?\\s*([\\w|\\W]*)");

    public static Map<String, ColumnInfo> getColumnList(String createTableSql) {
        Map<String, ColumnInfo> columns = new HashMap<>();
        StringBuilder columnBuilder = new StringBuilder();
        int startIndex = createTableSql.indexOf("(");
        createTableSql = createTableSql.substring(startIndex + 1);

        boolean insideParentheses = false;
        for (int i = 0; i < createTableSql.length(); i++) {
            char c = createTableSql.charAt(i);
            if (c == '(') {
                insideParentheses = true;
                columnBuilder.append(c);
            } else if ((c == ',' || c == ')') && !insideParentheses) {
                parseColumn(columnBuilder.toString(), columns, startIndex + i);
                columnBuilder.setLength(0);
            } else if (c == ')') {
                insideParentheses = false;
                columnBuilder.append(c);
            } else {
                columnBuilder.append(c);
            }
        }
        return columns;
    }

    private static void parseColumn(String columnString, Map<String, ColumnInfo> columnList, int index) {
        Matcher matcher = COLUMN_PATTERN.matcher(columnString.trim());
        if (matcher.matches()) {
            String columnName = matcher.group(1);
            String otherInfo = matcher.group(2).trim();
            StringBuilder columnBuilder = new StringBuilder(columnName).append(" ").append(otherInfo);
            if (columnBuilder.toString().toUpperCase().contains("PRIMARY KEY") ||
                columnBuilder.toString().toUpperCase().contains("CREATE TABLE")) {
                return;
            }
            columnList.put(columnName, new ColumnInfo(columnName, otherInfo, index));
        }
    }

    @Getter
    public static final class ColumnInfo {

        public ColumnInfo(String name, String info, int index) {
            this.name = name;
            this.info = info;
            this.index = index;
        }

        String name;
        String info;
        int index;
    }

}
