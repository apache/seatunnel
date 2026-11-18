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

package org.apache.seatunnel.connectors.seatunnel.maxcompute.util;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;

public class CreateTableParserTest {

    @Test
    public void testDelimitersInsideQuotedTextDoNotSplitColumns() {
        String template =
                "CREATE TABLE `db`.`tbl` (\n"
                        + "`id`,\n"
                        + "`level` VARCHAR(10) COMMENT 'low,medium,high',\n"
                        + "`unit` INT COMMENT 'seconds)',\n"
                        + "`note` STRING COMMENT 'it\\'s a, b',\n"
                        + "`creator`,\n"
                        + "`create_time` DATETIME NOT NULL,\n"
                        + "${rowtype_fields}\n"
                        + ") ENGINE=OLAP";

        Map<String, CreateTableParser.ColumnInfo> columns =
                CreateTableParser.getColumnList(template);

        Assertions.assertEquals(
                new HashSet<>(
                        Arrays.asList("id", "level", "unit", "note", "creator", "create_time")),
                columns.keySet());
        Assertions.assertEquals(
                "VARCHAR(10) COMMENT 'low,medium,high'", columns.get("level").getInfo());

        CreateTableParser.ColumnInfo id = columns.get("id");
        Assertions.assertEquals("id", template.substring(id.getStartIndex(), id.getEndIndex()));
        Assertions.assertEquals("", id.getInfo());

        CreateTableParser.ColumnInfo creator = columns.get("creator");
        Assertions.assertEquals(
                "creator", template.substring(creator.getStartIndex(), creator.getEndIndex()));
        Assertions.assertEquals("", creator.getInfo());
    }

    @Test
    public void testDoubleQuotesBackticksAndDoubledQuotes() {
        String template =
                "CREATE TABLE `db`.`tbl` (\n"
                        + "`level` STRING COMMENT \"low,medium)\",\n"
                        + "`note` STRING COMMENT 'it''s, a',\n"
                        + "`p,q` INT,\n"
                        + "`path\\` INT,\n"
                        + "`creator`,\n"
                        + "${rowtype_fields}\n"
                        + ") ENGINE=OLAP";

        Map<String, CreateTableParser.ColumnInfo> columns =
                CreateTableParser.getColumnList(template);

        Assertions.assertEquals("STRING COMMENT \"low,medium)\"", columns.get("level").getInfo());
        Assertions.assertEquals("STRING COMMENT 'it''s, a'", columns.get("note").getInfo());
        Assertions.assertFalse(columns.containsKey("q"));
        Assertions.assertTrue(columns.containsKey("path"));

        CreateTableParser.ColumnInfo creator = columns.get("creator");
        Assertions.assertEquals(
                "creator", template.substring(creator.getStartIndex(), creator.getEndIndex()));
    }

    @Test
    public void testUnterminatedQuoteFails() {
        String template =
                "CREATE TABLE `db`.`tbl` (\n"
                        + "`id`,\n"
                        + "`level` STRING COMMENT 'user's level',\n"
                        + "${rowtype_fields}\n"
                        + ") ENGINE=OLAP";

        Assertions.assertThrows(
                IllegalArgumentException.class, () -> CreateTableParser.getColumnList(template));
    }

    @Test
    public void testQuotesInsideSqlCommentsAreIgnored() {
        String template =
                "CREATE TABLE `db`.`tbl` (\n"
                        + "`id` BIGINT NOT NULL -- don't change, (this) type\n"
                        + ", `level` STRING /* it's a, b) */,\n"
                        + "`creator`,\n"
                        + "${rowtype_fields}\n"
                        + ") ENGINE=OLAP";

        Map<String, CreateTableParser.ColumnInfo> columns =
                CreateTableParser.getColumnList(template);

        Assertions.assertEquals(
                new HashSet<>(Arrays.asList("id", "level", "creator")), columns.keySet());
        Assertions.assertEquals(
                "BIGINT NOT NULL -- don't change, (this) type", columns.get("id").getInfo());
        Assertions.assertEquals("STRING /* it's a, b) */", columns.get("level").getInfo());

        CreateTableParser.ColumnInfo creator = columns.get("creator");
        Assertions.assertEquals(
                "creator", template.substring(creator.getStartIndex(), creator.getEndIndex()));

        String unterminated = "CREATE TABLE `db`.`tbl` (\n`id` /* no end,\n${rowtype_fields}\n)";
        Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> CreateTableParser.getColumnList(unterminated));
    }
}
