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

package org.apache.seatunnel.connectors.seatunnel.clickhouse.source;

import org.apache.seatunnel.connectors.seatunnel.clickhouse.shard.Shard;

import java.io.Serializable;
import java.util.Objects;

public class ClickhousePart implements Serializable, Comparable<ClickhousePart> {

    /** SerialVersionUID */
    private static final long serialVersionUID = 2735091038047635015L;

    private final String name;
    private final String database;
    private final String table;
    private final Shard shard;
    private int offset = 0;

    /** Flag indicating whether all data from this part has been completely read. */
    private boolean isEndOfPart = false;

    public ClickhousePart(String name, String database, String table, Shard shard) {
        this.name = name;
        this.database = database;
        this.table = table;
        this.shard = shard;
    }

    public String getDatabase() {
        return database;
    }

    public String getTable() {
        return table;
    }

    public Shard getShard() {
        return shard;
    }

    public String getName() {
        return name;
    }

    public boolean isEndOfPart() {
        return isEndOfPart;
    }

    public void setEndOfPart(boolean endOfPart) {
        this.isEndOfPart = endOfPart;
    }

    public void setOffset(int offset) {
        this.offset = offset;
    }

    public int getOffset() {
        return offset;
    }

    @Override
    public int compareTo(ClickhousePart o) {
        return 0;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ClickhousePart that = (ClickhousePart) o;
        return Objects.equals(name, that.name)
                && Objects.equals(database, that.database)
                && Objects.equals(table, that.table)
                && Objects.equals(shard, that.shard);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, database, table, shard);
    }

    @Override
    public String toString() {
        return "ClickhousePart{"
                + "name='"
                + name
                + '\''
                + ", database='"
                + database
                + '\''
                + ", table='"
                + table
                + '\''
                + ", shard="
                + shard
                + ", offset="
                + offset
                + ", isEndOfPart="
                + isEndOfPart
                + '}';
    }
}
