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

package org.apache.seatunnel.connectors.seatunnel.cdc.mysql.schema.ddl.listener;

import org.apache.seatunnel.connectors.seatunnel.cdc.mysql.schema.ddl.AntlrDdlParser;
import org.apache.seatunnel.connectors.seatunnel.cdc.mysql.schema.ddl.MySqlAntlrDdlParser;

import org.antlr.v4.runtime.tree.ParseTreeListener;

import io.debezium.antlr.DataTypeResolver;
import io.debezium.connector.mysql.antlr.listener.DefaultValueParserListener;
import io.debezium.ddl.parser.mysql.generated.MySqlParser;
import io.debezium.ddl.parser.mysql.generated.MySqlParserBaseListener;
import io.debezium.relational.Column;
import io.debezium.relational.ColumnEditor;
import io.debezium.relational.ddl.DataType;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

import java.sql.Types;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

@AllArgsConstructor
public class ColumnDefinitionParserListener extends MySqlParserBaseListener {
    private final MySqlAntlrDdlParser parser;
    private final DataTypeResolver dataTypeResolver;
    private final List<ParseTreeListener> listeners;
    private final boolean convertDefault;

    @Setter @Getter private ColumnEditor columnEditor;
    private boolean uniqueColumn;
    private DefaultValueParserListener defaultValueListener;
    private AtomicReference<Boolean> optionalColumn = new AtomicReference<>();

    public ColumnDefinitionParserListener(
            ColumnEditor columnEditor,
            MySqlAntlrDdlParser parser,
            List<ParseTreeListener> listeners,
            boolean convertDefault) {
        this.columnEditor = columnEditor;
        this.parser = parser;
        this.dataTypeResolver = parser.getDataTypeResolver();
        this.listeners = listeners;
        this.convertDefault = convertDefault;
    }

    public ColumnDefinitionParserListener(
            ColumnEditor columnEditor,
            MySqlAntlrDdlParser parser,
            List<ParseTreeListener> listeners) {
        this(columnEditor, parser, listeners, false);
    }

    public Column getColumn() {
        return columnEditor.create();
    }

    @Override
    public void enterColumnDefinition(MySqlParser.ColumnDefinitionContext ctx) {
        uniqueColumn = false;
        optionalColumn = new AtomicReference<>();
        resolveColumnDataType(ctx.dataType());
        defaultValueListener =
                new DefaultValueParserListener(
                        columnEditor, parser.getConverters(), optionalColumn, convertDefault);
        listeners.add(defaultValueListener);
        super.enterColumnDefinition(ctx);
    }

    @Override
    public void exitColumnDefinition(MySqlParser.ColumnDefinitionContext ctx) {
        if (optionalColumn.get() != null) {
            columnEditor.optional(optionalColumn.get().booleanValue());
        }
        defaultValueListener.convertDefaultValue(false);
        listeners.remove(defaultValueListener);
        super.exitColumnDefinition(ctx);
    }

    @Override
    public void enterUniqueKeyColumnConstraint(MySqlParser.UniqueKeyColumnConstraintContext ctx) {
        uniqueColumn = true;
        super.enterUniqueKeyColumnConstraint(ctx);
    }

    @Override
    public void enterNullNotnull(MySqlParser.NullNotnullContext ctx) {
        optionalColumn.set(Boolean.valueOf(ctx.NOT() == null));
        super.enterNullNotnull(ctx);
    }

    @Override
    public void enterAutoIncrementColumnConstraint(
            MySqlParser.AutoIncrementColumnConstraintContext ctx) {
        columnEditor.autoIncremented(true);
        columnEditor.generated(true);
        super.enterAutoIncrementColumnConstraint(ctx);
    }

    @Override
    public void enterSerialDefaultColumnConstraint(
            MySqlParser.SerialDefaultColumnConstraintContext ctx) {
        serialColumn();
        super.enterSerialDefaultColumnConstraint(ctx);
    }

    private void resolveColumnDataType(MySqlParser.DataTypeContext dataTypeContext) {
        String charsetName = null;
        DataType dataType = dataTypeResolver.resolveDataType(dataTypeContext);

        if (dataTypeContext instanceof MySqlParser.StringDataTypeContext) {
            // Same as LongVarcharDataTypeContext but with dimension handling
            MySqlParser.StringDataTypeContext stringDataTypeContext =
                    (MySqlParser.StringDataTypeContext) dataTypeContext;

            if (stringDataTypeContext.lengthOneDimension() != null) {
                Integer length =
                        Integer.valueOf(
                                stringDataTypeContext
                                        .lengthOneDimension()
                                        .decimalLiteral()
                                        .getText());
                columnEditor.length(length);
            }

            charsetName =
                    parser.extractCharset(
                            stringDataTypeContext.charsetName(),
                            stringDataTypeContext.collationName());
        } else if (dataTypeContext instanceof MySqlParser.LongVarcharDataTypeContext) {
            // Same as StringDataTypeContext but without dimension handling
            MySqlParser.LongVarcharDataTypeContext longVarcharTypeContext =
                    (MySqlParser.LongVarcharDataTypeContext) dataTypeContext;

            charsetName =
                    parser.extractCharset(
                            longVarcharTypeContext.charsetName(),
                            longVarcharTypeContext.collationName());
        } else if (dataTypeContext instanceof MySqlParser.NationalStringDataTypeContext) {
            MySqlParser.NationalStringDataTypeContext nationalStringDataTypeContext =
                    (MySqlParser.NationalStringDataTypeContext) dataTypeContext;

            if (nationalStringDataTypeContext.lengthOneDimension() != null) {
                Integer length =
                        Integer.valueOf(
                                nationalStringDataTypeContext
                                        .lengthOneDimension()
                                        .decimalLiteral()
                                        .getText());
                columnEditor.length(length);
            }
        } else if (dataTypeContext instanceof MySqlParser.NationalVaryingStringDataTypeContext) {
            MySqlParser.NationalVaryingStringDataTypeContext nationalVaryingStringDataTypeContext =
                    (MySqlParser.NationalVaryingStringDataTypeContext) dataTypeContext;

            if (nationalVaryingStringDataTypeContext.lengthOneDimension() != null) {
                Integer length =
                        Integer.valueOf(
                                nationalVaryingStringDataTypeContext
                                        .lengthOneDimension()
                                        .decimalLiteral()
                                        .getText());
                columnEditor.length(length);
            }
        } else if (dataTypeContext instanceof MySqlParser.DimensionDataTypeContext) {
            MySqlParser.DimensionDataTypeContext dimensionDataTypeContext =
                    (MySqlParser.DimensionDataTypeContext) dataTypeContext;

            Integer length = null;
            Integer scale = null;
            if (dimensionDataTypeContext.lengthOneDimension() != null) {
                length =
                        Integer.valueOf(
                                dimensionDataTypeContext
                                        .lengthOneDimension()
                                        .decimalLiteral()
                                        .getText());
            }

            if (dimensionDataTypeContext.lengthTwoDimension() != null) {
                List<MySqlParser.DecimalLiteralContext> decimalLiterals =
                        dimensionDataTypeContext.lengthTwoDimension().decimalLiteral();
                length = Integer.valueOf(decimalLiterals.get(0).getText());
                scale = Integer.valueOf(decimalLiterals.get(1).getText());
            }

            if (dimensionDataTypeContext.lengthTwoOptionalDimension() != null) {
                List<MySqlParser.DecimalLiteralContext> decimalLiterals =
                        dimensionDataTypeContext.lengthTwoOptionalDimension().decimalLiteral();
                length = Integer.valueOf(decimalLiterals.get(0).getText());

                if (decimalLiterals.size() > 1) {
                    scale = Integer.valueOf(decimalLiterals.get(1).getText());
                }
            }
            if (length != null) {
                columnEditor.length(length);
            }
            if (scale != null) {
                columnEditor.scale(scale);
            }
        } else if (dataTypeContext instanceof MySqlParser.CollectionDataTypeContext) {
            MySqlParser.CollectionDataTypeContext collectionDataTypeContext =
                    (MySqlParser.CollectionDataTypeContext) dataTypeContext;
            if (collectionDataTypeContext.charsetName() != null) {
                charsetName = collectionDataTypeContext.charsetName().getText();
            }

            if (dataType.name().toUpperCase().equals("SET")) {
                // After DBZ-132, it will always be comma separated
                int optionsSize =
                        collectionDataTypeContext.collectionOptions().collectionOption().size();
                columnEditor.length(
                        Math.max(0, optionsSize * 2 - 1)); // number of options + number of commas
            } else {
                columnEditor.length(1);
            }
        }

        String dataTypeName = dataType.name().toUpperCase();

        if (dataTypeName.equals("ENUM") || dataTypeName.equals("SET")) {
            // type expression has to be set, because the value converter needs to know the enum or
            // set options
            MySqlParser.CollectionDataTypeContext collectionDataTypeContext =
                    (MySqlParser.CollectionDataTypeContext) dataTypeContext;

            List<String> collectionOptions =
                    collectionDataTypeContext.collectionOptions().collectionOption().stream()
                            .map(AntlrDdlParser::getText)
                            .collect(Collectors.toList());

            columnEditor.type(dataTypeName);
            columnEditor.enumValues(collectionOptions);
        } else if (dataTypeName.equals("SERIAL")) {
            // SERIAL is an alias for BIGINT UNSIGNED NOT NULL AUTO_INCREMENT UNIQUE
            columnEditor.type("BIGINT UNSIGNED");
            serialColumn();
        } else {
            columnEditor.type(dataTypeName);
        }

        int jdbcDataType = dataType.jdbcType();
        columnEditor.jdbcType(jdbcDataType);

        if (columnEditor.length() == -1) {
            columnEditor.length((int) dataType.length());
        }
        if (!columnEditor.scale().isPresent() && dataType.scale() != Column.UNSET_INT_VALUE) {
            columnEditor.scale(dataType.scale());
        }
        if (Types.NCHAR == jdbcDataType || Types.NVARCHAR == jdbcDataType) {
            // NCHAR and NVARCHAR columns always uses utf8 as charset
            columnEditor.charsetName("utf8");
        } else {
            columnEditor.charsetName(charsetName);
        }
    }

    private void serialColumn() {
        if (optionalColumn.get() == null) {
            optionalColumn.set(Boolean.FALSE);
        }
        uniqueColumn = true;
        columnEditor.autoIncremented(true);
        columnEditor.generated(true);
    }
}
