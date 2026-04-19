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

package org.apache.seatunnel.connectors.seatunnel.cdc.postgres.source.parser;

import org.apache.seatunnel.shade.org.apache.commons.lang3.StringUtils;

import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.converter.BasicTypeDefine;
import org.apache.seatunnel.api.table.schema.event.AlterTableAddColumnEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableChangeColumnEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableColumnEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableDropColumnEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableModifyColumnEvent;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.psql.PostgresTypeConverter;

import io.debezium.relational.SystemVariables;
import io.debezium.relational.Tables;
import io.debezium.relational.ddl.DdlChanges;
import io.debezium.relational.ddl.DdlParser;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class PostgresAlterTableParser implements DdlParser {

    private static final Pattern MODIFIER_PATTERN =
            Pattern.compile(
                    "(?i)\\b(NOT\\s+NULL|NULL|DEFAULT|CHECK|CONSTRAINT|REFERENCES|PRIMARY\\s+KEY|UNIQUE|COLLATE|GENERATED|STORED|USING|COMMENT)\\b");
    private static final Pattern TYPE_PATTERN =
            Pattern.compile("^(?<type>.+?)(?:\\((?<args>.*)\\))?$");

    private final TablePath defaultTablePath;
    private final List<AlterTableColumnEvent> parsedEvents = new ArrayList<>();
    private final DdlChanges ddlChanges = new DdlChanges();
    private final SystemVariables systemVariables = new SystemVariables();

    private String currentDatabase;
    private String currentSchema;

    public PostgresAlterTableParser(TablePath tablePath) {
        this.defaultTablePath = tablePath;
    }

    @Override
    public void parse(String ddlContent, Tables databaseTables) {
        parsedEvents.clear();
        ddlChanges.reset();
        if (StringUtils.isBlank(ddlContent)) {
            return;
        }

        String ddl = ddlContent.trim();
        if (ddl.endsWith(";")) {
            ddl = ddl.substring(0, ddl.length() - 1).trim();
        }
        if (!ddl.toUpperCase(Locale.ROOT).startsWith("ALTER TABLE")) {
            return;
        }

        String statement = ddl.substring("ALTER TABLE".length()).trim();
        if (statement.toUpperCase(Locale.ROOT).startsWith("ONLY ")) {
            statement = statement.substring("ONLY ".length()).trim();
        }

        int actionIndex = findFirstActionIndex(statement);
        if (actionIndex < 0) {
            return;
        }

        String tableExpression = statement.substring(0, actionIndex).trim();
        String actionsExpression = statement.substring(actionIndex).trim();
        TableIdentifier tableIdentifier =
                TableIdentifier.of(null, resolveTablePath(tableExpression));

        for (String action : splitTopLevelClauses(actionsExpression)) {
            parseAction(tableIdentifier, action.trim());
        }
    }

    @Override
    public void setCurrentDatabase(String databaseName) {
        this.currentDatabase = databaseName;
    }

    @Override
    public void setCurrentSchema(String schemaName) {
        this.currentSchema = schemaName;
    }

    @Override
    public DdlChanges getDdlChanges() {
        return ddlChanges;
    }

    @Override
    public String terminator() {
        return ";";
    }

    @Override
    public SystemVariables systemVariables() {
        return systemVariables;
    }

    public List<AlterTableColumnEvent> getAndClearParsedEvents() {
        List<AlterTableColumnEvent> events = new ArrayList<>(parsedEvents);
        parsedEvents.clear();
        return events;
    }

    private void parseAction(TableIdentifier tableIdentifier, String action) {
        if (StringUtils.isBlank(action)) {
            return;
        }

        if (startsWithIgnoreCase(action, "ADD")) {
            parseAddAction(tableIdentifier, action);
        } else if (startsWithIgnoreCase(action, "DROP")) {
            parseDropAction(tableIdentifier, action);
        } else if (startsWithIgnoreCase(action, "RENAME")) {
            parseRenameAction(tableIdentifier, action);
        } else if (startsWithIgnoreCase(action, "ALTER")) {
            parseAlterAction(tableIdentifier, action);
        }
    }

    private void parseAddAction(TableIdentifier tableIdentifier, String action) {
        String remainder = action.substring("ADD".length()).trim();
        if (startsWithIgnoreCase(remainder, "COLUMN")) {
            remainder = remainder.substring("COLUMN".length()).trim();
        }
        if (startsWithIgnoreCase(remainder, "IF NOT EXISTS")) {
            remainder = remainder.substring("IF NOT EXISTS".length()).trim();
        }

        if (remainder.startsWith("(") && remainder.endsWith(")")) {
            for (String fragment : splitTopLevelClauses(stripOuterParentheses(remainder))) {
                parseAddColumn(tableIdentifier, fragment.trim());
            }
            return;
        }

        parseAddColumn(tableIdentifier, remainder);
    }

    private void parseAddColumn(TableIdentifier tableIdentifier, String fragment) {
        ParsedColumn parsedColumn = parseColumnDefinition(fragment);
        if (parsedColumn != null) {
            parsedEvents.add(AlterTableAddColumnEvent.add(tableIdentifier, parsedColumn.column));
        }
    }

    private void parseDropAction(TableIdentifier tableIdentifier, String action) {
        String remainder = action.substring("DROP".length()).trim();
        if (startsWithIgnoreCase(remainder, "COLUMN")) {
            remainder = remainder.substring("COLUMN".length()).trim();
        }
        if (startsWithIgnoreCase(remainder, "IF EXISTS")) {
            remainder = remainder.substring("IF EXISTS".length()).trim();
        }

        if (remainder.startsWith("(") && remainder.endsWith(")")) {
            for (String columnName : splitTopLevelClauses(stripOuterParentheses(remainder))) {
                addDropEvent(tableIdentifier, columnName.trim());
            }
            return;
        }

        addDropEvent(tableIdentifier, remainder);
    }

    private void addDropEvent(TableIdentifier tableIdentifier, String columnName) {
        ParsedIdentifier column = parseIdentifier(columnName);
        if (column == null) {
            return;
        }
        String remainder = column.remainder();
        if (StringUtils.isBlank(remainder)
                || startsWithIgnoreCase(remainder, "CASCADE")
                || startsWithIgnoreCase(remainder, "RESTRICT")) {
            parsedEvents.add(new AlterTableDropColumnEvent(tableIdentifier, column.identifier()));
        }
    }

    private void parseRenameAction(TableIdentifier tableIdentifier, String action) {
        String remainder = action.substring("RENAME".length()).trim();
        if (startsWithIgnoreCase(remainder, "COLUMN")) {
            remainder = remainder.substring("COLUMN".length()).trim();
        }

        ParsedIdentifier oldColumn = parseIdentifier(remainder);
        if (oldColumn == null) {
            return;
        }
        remainder = oldColumn.remainder().trim();
        if (!startsWithIgnoreCase(remainder, "TO")) {
            return;
        }
        remainder = remainder.substring("TO".length()).trim();
        ParsedIdentifier newColumn = parseIdentifier(remainder);
        if (newColumn == null) {
            return;
        }

        parsedEvents.add(
                AlterTableChangeColumnEvent.change(
                        tableIdentifier,
                        oldColumn.identifier(),
                        PhysicalColumn.builder().name(newColumn.identifier()).build()));
    }

    private void parseAlterAction(TableIdentifier tableIdentifier, String action) {
        String remainder = action.substring("ALTER".length()).trim();
        if (startsWithIgnoreCase(remainder, "COLUMN")) {
            remainder = remainder.substring("COLUMN".length()).trim();
        }

        ParsedIdentifier column = parseIdentifier(remainder);
        if (column == null) {
            return;
        }
        remainder = column.remainder().trim();

        if (startsWithIgnoreCase(remainder, "SET DATA TYPE")) {
            remainder = remainder.substring("SET DATA TYPE".length()).trim();
        } else if (startsWithIgnoreCase(remainder, "TYPE")) {
            remainder = remainder.substring("TYPE".length()).trim();
        } else {
            return;
        }

        ParsedType parsedType = parseType(remainder);
        if (parsedType == null) {
            return;
        }

        Column columnDefinition =
                toPhysicalColumn(
                        column.identifier(),
                        parsedType.typeAlias(),
                        parsedType.columnType(),
                        parsedType.length(),
                        parsedType.precision(),
                        parsedType.scale(),
                        !isNotNull(remainder));

        AlterTableModifyColumnEvent event =
                AlterTableModifyColumnEvent.modify(tableIdentifier, columnDefinition);
        event.setTypeChanged(true);
        parsedEvents.add(event);
    }

    private ParsedColumn parseColumnDefinition(String fragment) {
        ParsedIdentifier column = parseIdentifier(fragment);
        if (column == null) {
            return null;
        }

        String remainder = column.remainder().trim();
        if (StringUtils.isBlank(remainder)) {
            return null;
        }

        ParsedType parsedType = parseType(remainder);
        if (parsedType == null) {
            return null;
        }

        boolean nullable = !isNotNull(remainder);
        Column columnDefinition =
                toPhysicalColumn(
                        column.identifier(),
                        parsedType.typeAlias(),
                        parsedType.columnType(),
                        parsedType.length(),
                        parsedType.precision(),
                        parsedType.scale(),
                        nullable);
        return new ParsedColumn(columnDefinition);
    }

    private Column toPhysicalColumn(
            String columnName,
            String typeAlias,
            String columnType,
            Long length,
            Long precision,
            Integer scale,
            boolean nullable) {
        BasicTypeDefine<Object> typeDefine =
                BasicTypeDefine.builder()
                        .name(columnName)
                        .columnType(columnType)
                        .dataType(typeAlias)
                        .length(length)
                        .precision(precision)
                        .scale(scale)
                        .nullable(nullable)
                        .build();
        return PostgresTypeConverter.INSTANCE.convert(typeDefine);
    }

    private ParsedType parseType(String remainder) {
        String normalized = collapseWhitespace(remainder);
        int modifierIndex = findModifierIndex(normalized);
        String typeExpression = normalized.substring(0, modifierIndex).trim();
        if (StringUtils.isBlank(typeExpression)) {
            return null;
        }

        String baseType = typeExpression.toLowerCase(Locale.ROOT);
        String timeZoneSuffix = "";
        if (baseType.endsWith(" with time zone")) {
            timeZoneSuffix = " with time zone";
            baseType = baseType.substring(0, baseType.length() - timeZoneSuffix.length()).trim();
        } else if (baseType.endsWith(" without time zone")) {
            timeZoneSuffix = " without time zone";
            baseType = baseType.substring(0, baseType.length() - timeZoneSuffix.length()).trim();
        }
        String args = null;
        Matcher matcher = TYPE_PATTERN.matcher(baseType);
        if (matcher.matches()) {
            baseType = matcher.group("type").trim();
            args = matcher.group("args");
        }
        if (StringUtils.isNotBlank(timeZoneSuffix)) {
            baseType = baseType + timeZoneSuffix;
        }

        Long length = null;
        Long precision = null;
        Integer scale = null;
        if (StringUtils.isNotBlank(args)) {
            List<String> parts = Arrays.asList(args.split(","));
            if (parts.size() == 1) {
                Long value = parseLong(parts.get(0));
                if (isCharacterType(baseType)) {
                    length = value;
                    precision = value;
                } else {
                    precision = value;
                    length = value;
                }
            } else if (parts.size() >= 2) {
                precision = parseLong(parts.get(0));
                scale = parseInteger(parts.get(1));
                length = precision;
            }
        }

        String typeAlias = normalizeTypeAlias(baseType);
        return new ParsedType(typeAlias, typeExpression, length, precision, scale);
    }

    private String normalizeTypeAlias(String baseType) {
        String arrayTypeAlias = normalizeArrayTypeAlias(baseType);
        if (arrayTypeAlias != null) {
            return arrayTypeAlias;
        }
        switch (baseType) {
            case "smallint":
            case "smallserial":
                return "int2";
            case "integer":
            case "int":
            case "serial":
                return "int4";
            case "bigint":
            case "bigserial":
                return "int8";
            case "real":
                return "float4";
            case "double precision":
                return "float8";
            case "decimal":
                return "numeric";
            case "character varying":
            case "varchar":
                return "varchar";
            case "character":
            case "bpchar":
            case "char":
                return "char";
            case "bool":
            case "boolean":
                return "boolean";
            case "timestamp without time zone":
            case "timestamp":
                return "timestamp";
            case "timestamp with time zone":
            case "timestamptz":
                return "timestamptz";
            case "time without time zone":
            case "time":
                return "time";
            case "time with time zone":
            case "timetz":
                return "timetz";
            default:
                return baseType;
        }
    }

    private String normalizeArrayTypeAlias(String baseType) {
        if (!baseType.endsWith("[]")) {
            return null;
        }

        String elementType = baseType.substring(0, baseType.length() - 2).trim();
        String normalizedElementType = normalizeTypeAlias(elementType);
        switch (normalizedElementType) {
            case "int2":
                return "_int2";
            case "int4":
                return "_int4";
            case "int8":
                return "_int8";
            case "float4":
                return "_float4";
            case "float8":
                return "_float8";
            case "varchar":
                return "_varchar";
            case "char":
                return "_bpchar";
            case "boolean":
                return "_bool";
            case "text":
                return "_text";
            default:
                return baseType;
        }
    }

    private boolean isCharacterType(String baseType) {
        return "varchar".equals(baseType)
                || "character varying".equals(baseType)
                || "character".equals(baseType)
                || "bpchar".equals(baseType)
                || "char".equals(baseType);
    }

    private int findModifierIndex(String text) {
        Matcher matcher = MODIFIER_PATTERN.matcher(text);
        if (matcher.find()) {
            return matcher.start();
        }
        return text.length();
    }

    private int findFirstActionIndex(String statement) {
        boolean inDoubleQuote = false;
        boolean inSingleQuote = false;
        for (int i = 0; i < statement.length(); i++) {
            char current = statement.charAt(i);
            if (current == '"' && !inSingleQuote) {
                inDoubleQuote = !inDoubleQuote;
                continue;
            }
            if (current == '\'' && !inDoubleQuote) {
                inSingleQuote = !inSingleQuote;
                continue;
            }
            if (!inDoubleQuote && !inSingleQuote && isActionKeyword(statement, i)) {
                return i;
            }
        }
        return -1;
    }

    private boolean isActionKeyword(String statement, int start) {
        return matchesAction(statement, start, "ADD")
                || matchesAction(statement, start, "DROP")
                || matchesAction(statement, start, "RENAME")
                || matchesAction(statement, start, "ALTER");
    }

    private boolean matchesAction(String statement, int start, String keyword) {
        int end = start + keyword.length();
        if (end > statement.length()
                || !statement.regionMatches(true, start, keyword, 0, keyword.length())) {
            return false;
        }
        boolean leftBoundary = start == 0 || !isIdentifierChar(statement.charAt(start - 1));
        boolean rightBoundary =
                end == statement.length() || !isIdentifierChar(statement.charAt(end));
        return leftBoundary && rightBoundary;
    }

    private boolean isIdentifierChar(char value) {
        return Character.isLetterOrDigit(value) || value == '_' || value == '$';
    }

    private List<String> splitTopLevelClauses(String statement) {
        if (StringUtils.isBlank(statement)) {
            return Collections.emptyList();
        }

        List<String> clauses = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        int depth = 0;
        boolean inSingleQuote = false;
        boolean inDoubleQuote = false;

        for (int i = 0; i < statement.length(); i++) {
            char c = statement.charAt(i);
            if (c == '\'' && !inDoubleQuote) {
                inSingleQuote = !inSingleQuote;
                current.append(c);
                continue;
            }
            if (c == '"' && !inSingleQuote) {
                inDoubleQuote = !inDoubleQuote;
                current.append(c);
                continue;
            }
            if (!inSingleQuote && !inDoubleQuote) {
                if (c == '(') {
                    depth++;
                } else if (c == ')' && depth > 0) {
                    depth--;
                } else if (c == ',' && depth == 0) {
                    clauses.add(current.toString().trim());
                    current.setLength(0);
                    continue;
                }
            }
            current.append(c);
        }

        String tail = current.toString().trim();
        if (!tail.isEmpty()) {
            clauses.add(tail);
        }
        return clauses;
    }

    private TablePath resolveTablePath(String tableExpression) {
        List<String> parts = splitQualifiedIdentifier(tableExpression);
        String database =
                currentDatabase != null ? currentDatabase : defaultTablePath.getDatabaseName();
        String schema = currentSchema != null ? currentSchema : defaultTablePath.getSchemaName();
        String tableName = defaultTablePath.getTableName();

        if (parts.size() == 1) {
            tableName = parts.get(0);
        } else if (parts.size() == 2) {
            schema = parts.get(0);
            tableName = parts.get(1);
        } else if (parts.size() >= 3) {
            database = parts.get(parts.size() - 3);
            schema = parts.get(parts.size() - 2);
            tableName = parts.get(parts.size() - 1);
        }
        return TablePath.of(
                unquoteIdentifier(database),
                unquoteIdentifier(schema),
                unquoteIdentifier(tableName));
    }

    private List<String> splitQualifiedIdentifier(String text) {
        List<String> parts = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        boolean inDoubleQuote = false;

        for (int i = 0; i < text.length(); i++) {
            char c = text.charAt(i);
            if (c == '"') {
                inDoubleQuote = !inDoubleQuote;
                current.append(c);
                continue;
            }
            if (c == '.' && !inDoubleQuote) {
                parts.add(current.toString().trim());
                current.setLength(0);
                continue;
            }
            current.append(c);
        }

        String tail = current.toString().trim();
        if (!tail.isEmpty()) {
            parts.add(tail);
        }
        return parts;
    }

    private ParsedIdentifier parseIdentifier(String text) {
        String trimmed = text.trim();
        if (trimmed.isEmpty()) {
            return null;
        }

        if (trimmed.startsWith("\"")) {
            StringBuilder name = new StringBuilder();
            boolean escaped = false;
            int i = 1;
            for (; i < trimmed.length(); i++) {
                char c = trimmed.charAt(i);
                if (c == '"' && !escaped) {
                    if (i + 1 < trimmed.length() && trimmed.charAt(i + 1) == '"') {
                        name.append('"');
                        i++;
                        continue;
                    }
                    i++;
                    break;
                }
                name.append(c);
            }
            return new ParsedIdentifier(name.toString(), trimmed.substring(i).trim());
        }

        int end = 0;
        while (end < trimmed.length() && !Character.isWhitespace(trimmed.charAt(end))) {
            end++;
        }
        return new ParsedIdentifier(trimmed.substring(0, end), trimmed.substring(end).trim());
    }

    private String stripOuterParentheses(String text) {
        String trimmed = text.trim();
        if (trimmed.startsWith("(") && trimmed.endsWith(")")) {
            return trimmed.substring(1, trimmed.length() - 1).trim();
        }
        return trimmed;
    }

    private boolean startsWithIgnoreCase(String text, String prefix) {
        return text.regionMatches(true, 0, prefix, 0, prefix.length());
    }

    private boolean isNotNull(String text) {
        return Pattern.compile("(?i)\\bNOT\\s+NULL\\b").matcher(text).find();
    }

    private String collapseWhitespace(String text) {
        return text.trim().replaceAll("\\s+", " ");
    }

    private Long parseLong(String text) {
        return Long.parseLong(text.trim());
    }

    private Integer parseInteger(String text) {
        return Integer.parseInt(text.trim());
    }

    private String unquoteIdentifier(String identifier) {
        String trimmed = identifier == null ? null : identifier.trim();
        if (trimmed == null || trimmed.isEmpty()) {
            return trimmed;
        }
        if (trimmed.startsWith("\"") && trimmed.endsWith("\"")) {
            return trimmed.substring(1, trimmed.length() - 1).replace("\"\"", "\"");
        }
        return trimmed;
    }

    private static final class ParsedIdentifier {
        private final String identifier;
        private final String remainder;

        private ParsedIdentifier(String identifier, String remainder) {
            this.identifier = identifier;
            this.remainder = remainder;
        }

        private String identifier() {
            return identifier;
        }

        private String remainder() {
            return remainder;
        }
    }

    private static final class ParsedColumn {
        private final Column column;

        private ParsedColumn(Column column) {
            this.column = column;
        }
    }

    private static final class ParsedType {
        private final String typeAlias;
        private final String columnType;
        private final Long length;
        private final Long precision;
        private final Integer scale;

        private ParsedType(
                String typeAlias, String columnType, Long length, Long precision, Integer scale) {
            this.typeAlias = typeAlias;
            this.columnType = columnType;
            this.length = length;
            this.precision = precision;
            this.scale = scale;
        }

        private String typeAlias() {
            return typeAlias;
        }

        private String columnType() {
            return columnType;
        }

        private Long length() {
            return length;
        }

        private Long precision() {
            return precision;
        }

        private Integer scale() {
            return scale;
        }
    }
}
