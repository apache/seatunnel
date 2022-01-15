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

package org.apache.seatunnel.flink.sink;

import org.apache.flink.api.common.io.RichOutputFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.types.Row;
import org.influxdb.BatchOptions;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Point;
import org.influxdb.dto.Query;

import java.util.List;
import java.util.concurrent.TimeUnit;

public class InfluxDbOutputFormat extends RichOutputFormat<Row> {

    private final InfluxDB influxDB;
    private final String measurement;
    private final List<String> tags;
    private final List<String> fields;

    public InfluxDbOutputFormat(String serverURL, String database, String measurement, List<String> tags, List<String> fields) {
        this.influxDB = InfluxDBFactory.connect(serverURL);
        this.influxDB.query(new Query("CREATE DATABASE " + database));
        this.influxDB.setDatabase(database);
        this.influxDB.enableBatch(
                BatchOptions.DEFAULTS
                        .threadFactory(runnable -> {
                            Thread thread = new Thread(runnable);
                            thread.setDaemon(true);
                            return thread;
                        })
        );
        this.measurement = measurement;
        this.tags = tags;
        this.fields = fields;
    }

    @Override
    public void open(int taskNumber, int numTasks) {
    }

    @Override
    public void configure(Configuration parameters) {
    }

    @Override
    public void writeRecord(Row element) {
        Point.Builder builder = Point.measurement(this.measurement);
        builder.time(Long.valueOf(element.getField(0).toString()), TimeUnit.MILLISECONDS);
        for (int i = 1; i < tags.size() + 1; i++) {
            Object v = element.getField(i);
            if (v != null) {
                builder.tag(tags.get(i - 1), String.valueOf(v));
            }
        }
        for (int i = tags.size() + 1; i < element.getArity(); i++) {
            Object v = element.getField(i);
            if (v != null) {
                if (v instanceof Number) {
                    builder.addField(fields.get(i - 1), (Number) v);
                } else if (v instanceof String) {
                    builder.addField(fields.get(i - 1), (String) v);
                } else if (v instanceof Boolean) {
                    builder.addField(fields.get(i - 1), (Boolean) v);
                } else {
                    throw new RuntimeException("Not support type of field: " + v);
                }
            }
        }
        Point point = builder.build();
        influxDB.write(point);
    }

    @Override
    public void close() {
        if (this.influxDB != null) {
            this.influxDB.close();
        }
    }
}
