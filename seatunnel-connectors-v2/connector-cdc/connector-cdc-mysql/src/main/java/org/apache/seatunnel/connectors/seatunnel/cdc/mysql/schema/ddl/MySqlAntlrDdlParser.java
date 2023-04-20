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

package org.apache.seatunnel.connectors.seatunnel.cdc.mysql.schema.ddl;

import org.apache.seatunnel.api.table.catalog.TablePath;

import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTree;

import com.mysql.cj.CharsetMapping;
import io.debezium.antlr.AntlrDdlParserListener;
import io.debezium.antlr.DataTypeResolver;
import io.debezium.connector.mysql.MySqlValueConverters;
import io.debezium.ddl.parser.mysql.generated.MySqlLexer;
import io.debezium.ddl.parser.mysql.generated.MySqlParser;
import lombok.Getter;

import java.sql.Types;
import java.util.Arrays;

public class MySqlAntlrDdlParser extends AntlrDdlParser<MySqlLexer, MySqlParser> {
    @Getter private MySqlValueConverters converters;

    public MySqlAntlrDdlParser(
            String terminator, boolean throwErrorsFromTreeWalk, MySqlValueConverters converters) {
        super(terminator, throwErrorsFromTreeWalk);
        this.converters = converters;
    }

    @Override
    protected MySqlLexer createNewLexerInstance(CharStream charStream) {
        return new MySqlLexer(charStream);
    }

    @Override
    protected MySqlParser createNewParserInstance(CommonTokenStream commonTokenStream) {
        return new MySqlParser(commonTokenStream);
    }

    @Override
    protected AntlrDdlParserListener createParseTreeWalkerListener() {
        return new MySqlAntlrDdlParserListener(this);
    }

    @Override
    protected ParseTree parseTree(MySqlParser parser) {
        return parser.root();
    }

    @Override
    protected DataTypeResolver initializeDataTypeResolver() {
        DataTypeResolver.Builder dataTypeResolverBuilder = new DataTypeResolver.Builder();

        dataTypeResolverBuilder.registerDataTypes(
                MySqlParser.StringDataTypeContext.class.getCanonicalName(),
                Arrays.asList(
                        new DataTypeResolver.DataTypeEntry(Types.CHAR, MySqlParser.CHAR),
                        new DataTypeResolver.DataTypeEntry(
                                Types.VARCHAR, MySqlParser.CHAR, MySqlParser.VARYING),
                        new DataTypeResolver.DataTypeEntry(Types.VARCHAR, MySqlParser.VARCHAR),
                        new DataTypeResolver.DataTypeEntry(Types.VARCHAR, MySqlParser.TINYTEXT),
                        new DataTypeResolver.DataTypeEntry(Types.VARCHAR, MySqlParser.TEXT),
                        new DataTypeResolver.DataTypeEntry(Types.VARCHAR, MySqlParser.MEDIUMTEXT),
                        new DataTypeResolver.DataTypeEntry(Types.VARCHAR, MySqlParser.LONGTEXT),
                        new DataTypeResolver.DataTypeEntry(Types.NCHAR, MySqlParser.NCHAR),
                        new DataTypeResolver.DataTypeEntry(
                                Types.NVARCHAR, MySqlParser.NCHAR, MySqlParser.VARYING),
                        new DataTypeResolver.DataTypeEntry(Types.NVARCHAR, MySqlParser.NVARCHAR),
                        new DataTypeResolver.DataTypeEntry(
                                Types.BINARY, MySqlParser.CHAR, MySqlParser.BINARY),
                        new DataTypeResolver.DataTypeEntry(
                                Types.BINARY, MySqlParser.VARCHAR, MySqlParser.BINARY),
                        new DataTypeResolver.DataTypeEntry(
                                Types.BINARY, MySqlParser.TINYTEXT, MySqlParser.BINARY),
                        new DataTypeResolver.DataTypeEntry(
                                Types.BINARY, MySqlParser.TEXT, MySqlParser.BINARY),
                        new DataTypeResolver.DataTypeEntry(
                                Types.BINARY, MySqlParser.MEDIUMTEXT, MySqlParser.BINARY),
                        new DataTypeResolver.DataTypeEntry(
                                Types.BINARY, MySqlParser.LONGTEXT, MySqlParser.BINARY),
                        new DataTypeResolver.DataTypeEntry(
                                Types.BINARY, MySqlParser.NCHAR, MySqlParser.BINARY),
                        new DataTypeResolver.DataTypeEntry(
                                Types.BINARY, MySqlParser.NVARCHAR, MySqlParser.BINARY),
                        new DataTypeResolver.DataTypeEntry(Types.CHAR, MySqlParser.CHARACTER),
                        new DataTypeResolver.DataTypeEntry(
                                Types.VARCHAR, MySqlParser.CHARACTER, MySqlParser.VARYING)));
        dataTypeResolverBuilder.registerDataTypes(
                MySqlParser.NationalStringDataTypeContext.class.getCanonicalName(),
                Arrays.asList(
                        new DataTypeResolver.DataTypeEntry(
                                        Types.NVARCHAR, MySqlParser.NATIONAL, MySqlParser.VARCHAR)
                                .setSuffixTokens(MySqlParser.BINARY),
                        new DataTypeResolver.DataTypeEntry(
                                        Types.NCHAR, MySqlParser.NATIONAL, MySqlParser.CHARACTER)
                                .setSuffixTokens(MySqlParser.BINARY),
                        new DataTypeResolver.DataTypeEntry(
                                        Types.NVARCHAR, MySqlParser.NCHAR, MySqlParser.VARCHAR)
                                .setSuffixTokens(MySqlParser.BINARY)));
        dataTypeResolverBuilder.registerDataTypes(
                MySqlParser.NationalVaryingStringDataTypeContext.class.getCanonicalName(),
                Arrays.asList(
                        new DataTypeResolver.DataTypeEntry(
                                Types.NVARCHAR,
                                MySqlParser.NATIONAL,
                                MySqlParser.CHAR,
                                MySqlParser.VARYING),
                        new DataTypeResolver.DataTypeEntry(
                                Types.NVARCHAR,
                                MySqlParser.NATIONAL,
                                MySqlParser.CHARACTER,
                                MySqlParser.VARYING)));
        dataTypeResolverBuilder.registerDataTypes(
                MySqlParser.DimensionDataTypeContext.class.getCanonicalName(),
                Arrays.asList(
                        new DataTypeResolver.DataTypeEntry(Types.SMALLINT, MySqlParser.TINYINT)
                                .setSuffixTokens(
                                        MySqlParser.SIGNED,
                                        MySqlParser.UNSIGNED,
                                        MySqlParser.ZEROFILL),
                        new DataTypeResolver.DataTypeEntry(Types.SMALLINT, MySqlParser.INT1)
                                .setSuffixTokens(
                                        MySqlParser.SIGNED,
                                        MySqlParser.UNSIGNED,
                                        MySqlParser.ZEROFILL),
                        new DataTypeResolver.DataTypeEntry(Types.SMALLINT, MySqlParser.SMALLINT)
                                .setSuffixTokens(
                                        MySqlParser.SIGNED,
                                        MySqlParser.UNSIGNED,
                                        MySqlParser.ZEROFILL),
                        new DataTypeResolver.DataTypeEntry(Types.SMALLINT, MySqlParser.INT2)
                                .setSuffixTokens(
                                        MySqlParser.SIGNED,
                                        MySqlParser.UNSIGNED,
                                        MySqlParser.ZEROFILL),
                        new DataTypeResolver.DataTypeEntry(Types.INTEGER, MySqlParser.MEDIUMINT)
                                .setSuffixTokens(
                                        MySqlParser.SIGNED,
                                        MySqlParser.UNSIGNED,
                                        MySqlParser.ZEROFILL),
                        new DataTypeResolver.DataTypeEntry(Types.INTEGER, MySqlParser.INT3)
                                .setSuffixTokens(
                                        MySqlParser.SIGNED,
                                        MySqlParser.UNSIGNED,
                                        MySqlParser.ZEROFILL),
                        new DataTypeResolver.DataTypeEntry(Types.INTEGER, MySqlParser.MIDDLEINT)
                                .setSuffixTokens(
                                        MySqlParser.SIGNED,
                                        MySqlParser.UNSIGNED,
                                        MySqlParser.ZEROFILL),
                        new DataTypeResolver.DataTypeEntry(Types.INTEGER, MySqlParser.INT)
                                .setSuffixTokens(
                                        MySqlParser.SIGNED,
                                        MySqlParser.UNSIGNED,
                                        MySqlParser.ZEROFILL),
                        new DataTypeResolver.DataTypeEntry(Types.INTEGER, MySqlParser.INTEGER)
                                .setSuffixTokens(
                                        MySqlParser.SIGNED,
                                        MySqlParser.UNSIGNED,
                                        MySqlParser.ZEROFILL),
                        new DataTypeResolver.DataTypeEntry(Types.INTEGER, MySqlParser.INT4)
                                .setSuffixTokens(
                                        MySqlParser.SIGNED,
                                        MySqlParser.UNSIGNED,
                                        MySqlParser.ZEROFILL),
                        new DataTypeResolver.DataTypeEntry(Types.BIGINT, MySqlParser.BIGINT)
                                .setSuffixTokens(
                                        MySqlParser.SIGNED,
                                        MySqlParser.UNSIGNED,
                                        MySqlParser.ZEROFILL),
                        new DataTypeResolver.DataTypeEntry(Types.BIGINT, MySqlParser.INT8)
                                .setSuffixTokens(
                                        MySqlParser.SIGNED,
                                        MySqlParser.UNSIGNED,
                                        MySqlParser.ZEROFILL),
                        new DataTypeResolver.DataTypeEntry(Types.REAL, MySqlParser.REAL)
                                .setSuffixTokens(
                                        MySqlParser.SIGNED,
                                        MySqlParser.UNSIGNED,
                                        MySqlParser.ZEROFILL),
                        new DataTypeResolver.DataTypeEntry(Types.DOUBLE, MySqlParser.DOUBLE)
                                .setSuffixTokens(
                                        MySqlParser.PRECISION,
                                        MySqlParser.SIGNED,
                                        MySqlParser.UNSIGNED,
                                        MySqlParser.ZEROFILL),
                        new DataTypeResolver.DataTypeEntry(Types.DOUBLE, MySqlParser.FLOAT8)
                                .setSuffixTokens(
                                        MySqlParser.PRECISION,
                                        MySqlParser.SIGNED,
                                        MySqlParser.UNSIGNED,
                                        MySqlParser.ZEROFILL),
                        new DataTypeResolver.DataTypeEntry(Types.FLOAT, MySqlParser.FLOAT)
                                .setSuffixTokens(
                                        MySqlParser.SIGNED,
                                        MySqlParser.UNSIGNED,
                                        MySqlParser.ZEROFILL),
                        new DataTypeResolver.DataTypeEntry(Types.FLOAT, MySqlParser.FLOAT4)
                                .setSuffixTokens(
                                        MySqlParser.SIGNED,
                                        MySqlParser.UNSIGNED,
                                        MySqlParser.ZEROFILL),
                        new DataTypeResolver.DataTypeEntry(Types.DECIMAL, MySqlParser.DECIMAL)
                                .setSuffixTokens(
                                        MySqlParser.SIGNED,
                                        MySqlParser.UNSIGNED,
                                        MySqlParser.ZEROFILL)
                                .setDefaultLengthScaleDimension(10, 0),
                        new DataTypeResolver.DataTypeEntry(Types.DECIMAL, MySqlParser.DEC)
                                .setSuffixTokens(
                                        MySqlParser.SIGNED,
                                        MySqlParser.UNSIGNED,
                                        MySqlParser.ZEROFILL)
                                .setDefaultLengthScaleDimension(10, 0),
                        new DataTypeResolver.DataTypeEntry(Types.DECIMAL, MySqlParser.FIXED)
                                .setSuffixTokens(
                                        MySqlParser.SIGNED,
                                        MySqlParser.UNSIGNED,
                                        MySqlParser.ZEROFILL)
                                .setDefaultLengthScaleDimension(10, 0),
                        new DataTypeResolver.DataTypeEntry(Types.NUMERIC, MySqlParser.NUMERIC)
                                .setSuffixTokens(
                                        MySqlParser.SIGNED,
                                        MySqlParser.UNSIGNED,
                                        MySqlParser.ZEROFILL)
                                .setDefaultLengthScaleDimension(10, 0),
                        new DataTypeResolver.DataTypeEntry(Types.BIT, MySqlParser.BIT),
                        new DataTypeResolver.DataTypeEntry(Types.TIME, MySqlParser.TIME),
                        new DataTypeResolver.DataTypeEntry(
                                Types.TIMESTAMP_WITH_TIMEZONE, MySqlParser.TIMESTAMP),
                        new DataTypeResolver.DataTypeEntry(Types.TIMESTAMP, MySqlParser.DATETIME),
                        new DataTypeResolver.DataTypeEntry(Types.BINARY, MySqlParser.BINARY),
                        new DataTypeResolver.DataTypeEntry(Types.VARBINARY, MySqlParser.VARBINARY),
                        new DataTypeResolver.DataTypeEntry(Types.BLOB, MySqlParser.BLOB),
                        new DataTypeResolver.DataTypeEntry(Types.INTEGER, MySqlParser.YEAR)));
        dataTypeResolverBuilder.registerDataTypes(
                MySqlParser.SimpleDataTypeContext.class.getCanonicalName(),
                Arrays.asList(
                        new DataTypeResolver.DataTypeEntry(Types.DATE, MySqlParser.DATE),
                        new DataTypeResolver.DataTypeEntry(Types.BLOB, MySqlParser.TINYBLOB),
                        new DataTypeResolver.DataTypeEntry(Types.BLOB, MySqlParser.MEDIUMBLOB),
                        new DataTypeResolver.DataTypeEntry(Types.BLOB, MySqlParser.LONGBLOB),
                        new DataTypeResolver.DataTypeEntry(Types.BOOLEAN, MySqlParser.BOOL),
                        new DataTypeResolver.DataTypeEntry(Types.BOOLEAN, MySqlParser.BOOLEAN),
                        new DataTypeResolver.DataTypeEntry(Types.BIGINT, MySqlParser.SERIAL)));
        dataTypeResolverBuilder.registerDataTypes(
                MySqlParser.CollectionDataTypeContext.class.getCanonicalName(),
                Arrays.asList(
                        new DataTypeResolver.DataTypeEntry(Types.CHAR, MySqlParser.ENUM)
                                .setSuffixTokens(MySqlParser.BINARY),
                        new DataTypeResolver.DataTypeEntry(Types.CHAR, MySqlParser.SET)
                                .setSuffixTokens(MySqlParser.BINARY)));
        dataTypeResolverBuilder.registerDataTypes(
                MySqlParser.SpatialDataTypeContext.class.getCanonicalName(),
                Arrays.asList(
                        new DataTypeResolver.DataTypeEntry(
                                Types.OTHER, MySqlParser.GEOMETRYCOLLECTION),
                        new DataTypeResolver.DataTypeEntry(Types.OTHER, MySqlParser.GEOMCOLLECTION),
                        new DataTypeResolver.DataTypeEntry(Types.OTHER, MySqlParser.LINESTRING),
                        new DataTypeResolver.DataTypeEntry(
                                Types.OTHER, MySqlParser.MULTILINESTRING),
                        new DataTypeResolver.DataTypeEntry(Types.OTHER, MySqlParser.MULTIPOINT),
                        new DataTypeResolver.DataTypeEntry(Types.OTHER, MySqlParser.MULTIPOLYGON),
                        new DataTypeResolver.DataTypeEntry(Types.OTHER, MySqlParser.POINT),
                        new DataTypeResolver.DataTypeEntry(Types.OTHER, MySqlParser.POLYGON),
                        new DataTypeResolver.DataTypeEntry(Types.OTHER, MySqlParser.JSON),
                        new DataTypeResolver.DataTypeEntry(Types.OTHER, MySqlParser.GEOMETRY)));
        dataTypeResolverBuilder.registerDataTypes(
                MySqlParser.LongVarbinaryDataTypeContext.class.getCanonicalName(),
                Arrays.asList(
                        new DataTypeResolver.DataTypeEntry(Types.BLOB, MySqlParser.LONG)
                                .setSuffixTokens(MySqlParser.VARBINARY)));
        dataTypeResolverBuilder.registerDataTypes(
                MySqlParser.LongVarcharDataTypeContext.class.getCanonicalName(),
                Arrays.asList(
                        new DataTypeResolver.DataTypeEntry(Types.VARCHAR, MySqlParser.LONG)
                                .setSuffixTokens(MySqlParser.VARCHAR)));

        return dataTypeResolverBuilder.build();
    }

    public void runIfNotNull(Runnable function, Object... nullableObjects) {
        for (Object nullableObject : nullableObjects) {
            if (nullableObject == null) {
                return;
            }
        }
        function.run();
    }

    public TablePath parseQualifiedTableId(MySqlParser.FullIdContext fullIdContext) {
        final char[] fullTableName = fullIdContext.getText().toCharArray();
        StringBuilder component = new StringBuilder();
        String dbName = null;
        String tableName = null;
        final char EMPTY = '\0';
        char lastQuote = EMPTY;
        for (int i = 0; i < fullTableName.length; i++) {
            char c = fullTableName[i];
            if (isQuote(c)) {
                // Opening quote
                if (lastQuote == EMPTY) {
                    lastQuote = c;
                }
                // Closing quote
                else if (lastQuote == c) {
                    // escape of quote by doubling
                    if (i < fullTableName.length - 1 && fullTableName[i + 1] == c) {
                        component.append(c);
                        i++;
                    } else {
                        lastQuote = EMPTY;
                    }
                }
                // Quote that is part of name
                else {
                    component.append(c);
                }
            }
            // dot that is not in quotes, so name separator
            else if (c == '.' && lastQuote == EMPTY) {
                dbName = component.toString();
                component = new StringBuilder();
            }
            // Any char is part of name including quoted dot
            else {
                component.append(c);
            }
        }
        tableName = component.toString();
        return resolveTablePath(dbName != null ? dbName : currentSchema(), tableName);
    }

    public String parseName(MySqlParser.UidContext uidContext) {
        return withoutQuotes(uidContext);
    }

    public String extractCharset(
            MySqlParser.CharsetNameContext charsetNode,
            MySqlParser.CollationNameContext collationNode) {
        String charsetName = null;
        if (charsetNode != null && charsetNode.getText() != null) {
            charsetName = withoutQuotes(charsetNode.getText());
        } else if (collationNode != null && collationNode.getText() != null) {
            final String collationName = withoutQuotes(collationNode.getText()).toLowerCase();
            for (int index = 0; index < CharsetMapping.MAP_SIZE; index++) {
                if (collationName.equals(
                        CharsetMapping.getStaticCollationNameForCollationIndex(index))) {
                    charsetName = CharsetMapping.getStaticMysqlCharsetNameForCollationIndex(index);
                    break;
                }
            }
        }
        return charsetName;
    }
}
