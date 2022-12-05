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

package org.apache.seatunnel.connectors.seatunnel.clickhouse.sink.inject;

import java.sql.PreparedStatement;
import java.sql.SQLException;

public class LongInjectFunction implements ClickhouseFieldInjectFunction {

    @Override
    public void injectFields(PreparedStatement statement, int index, Object value) throws SQLException {
        statement.setLong(index, Long.parseLong(value.toString()));
    }

    @Override
    public boolean isCurrentFieldType(String fieldType) {
        return "UInt32".equals(fieldType)
            || "UInt64".equals(fieldType)
            || "Int64".equals(fieldType)
            || "IntervalYear".equals(fieldType)
            || "IntervalQuarter".equals(fieldType)
            || "IntervalMonth".equals(fieldType)
            || "IntervalWeek".equals(fieldType)
            || "IntervalDay".equals(fieldType)
            || "IntervalHour".equals(fieldType)
            || "IntervalMinute".equals(fieldType)
            || "IntervalSecond".equals(fieldType);
    }
}
