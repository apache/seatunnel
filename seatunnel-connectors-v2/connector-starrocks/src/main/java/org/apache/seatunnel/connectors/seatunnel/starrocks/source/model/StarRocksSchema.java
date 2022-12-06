/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.starrocks.source.model;

import com.starrocks.thrift.TScanColumnDesc;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

public class StarRocksSchema {

    private HashMap<String, Column> schemaMap;
    private List<Column> columns;

    public StarRocksSchema() {
        schemaMap = new HashMap<>();
        columns = new LinkedList<>();
    }

    public HashMap<String, Column> getSchema() {
        return schemaMap;
    }

    public List<Column> getColumns() {
        return columns;
    }

    public Column getColumn(int idx) {
        return columns.get(idx);
    }

    public void setSchema(HashMap<String, Column> schema) {
        this.schemaMap = schema;
    }

    public void put(String name, String type, String comment, int scale, int precision) {
        Column column = new Column(name, type, comment, scale, precision);
        columns.add(column);
        schemaMap.put(name, column);
    }

    public void put(Column column) {
        columns.add(column);
        schemaMap.put(column.getName(), column);
    }

    public Column get(String columnName) {
        return schemaMap.get(columnName);
    }

    public int size() {
        return schemaMap.size();
    }

    public static StarRocksSchema genSchema(List<TScanColumnDesc> tscanColumnDescs) {
        StarRocksSchema schema = new StarRocksSchema();
        for (TScanColumnDesc tscanColumnDesc : tscanColumnDescs) {
            schema.put(tscanColumnDesc.getName(), tscanColumnDesc.getType().name(), "", 0, 0);
        }
        return schema;
    }
}
