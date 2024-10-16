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

package org.apache.seatunnel.connectors.seatunnel.tdengine.sink;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.exception.CommonErrorCodeDeprecated;
import org.apache.seatunnel.connectors.seatunnel.common.sink.AbstractSinkWriter;
import org.apache.seatunnel.connectors.seatunnel.tdengine.config.TDengineSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.tdengine.exception.TDengineConnectorException;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;

import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Objects;

import static org.apache.seatunnel.connectors.seatunnel.tdengine.utils.TDengineUtil.checkDriverExist;

@Slf4j
public class TDengineSinkWriter extends AbstractSinkWriter<SeaTunnelRow, Void> {

    private static final DateTimeFormatter FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");
    private final Connection conn;
    private final TDengineSourceConfig config;
    private int tagsNum;

    @SneakyThrows
    public TDengineSinkWriter(Config pluginConfig, SeaTunnelRowType seaTunnelRowType) {
        config = TDengineSourceConfig.buildSourceConfig(pluginConfig);
        String jdbcUrl =
                StringUtils.join(
                        config.getUrl(),
                        config.getDatabase(),
                        "?user=",
                        config.getUsername(),
                        "&password=",
                        config.getPassword());
        // check td driver whether exist and if not, try to register
        checkDriverExist(jdbcUrl);
        conn = DriverManager.getConnection(jdbcUrl);
        try (Statement statement = conn.createStatement();
                final ResultSet metaResultSet =
                        statement.executeQuery(
                                "desc " + config.getDatabase() + "." + config.getStable())) {

            while (metaResultSet.next()) {
                if (StringUtils.equals("TAG", metaResultSet.getString("note"))) {
                    tagsNum++;
                }
            }
        }
    }

    @SneakyThrows
    @Override
    public void write(SeaTunnelRow element) {
        final ArrayList<Object> tags = Lists.newArrayList();
        for (int i = element.getArity() - tagsNum; i < element.getArity(); i++) {
            tags.add(element.getField(i));
        }
        final String tagValues = StringUtils.join(convertDataType(tags.toArray()), ",");

        final Object[] metrics =
                ArrayUtils.subarray(element.getFields(), 1, element.getArity() - tagsNum);

        try (Statement statement =
                conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
            String sql =
                    String.format(
                            "INSERT INTO %s using %s tags ( %s ) VALUES ( %s );",
                            element.getField(0),
                            config.getStable(),
                            tagValues,
                            StringUtils.join(convertDataType(metrics), ","));
            final int rowCount = statement.executeUpdate(sql);
            if (rowCount == 0) {
                Throwables.propagateIfPossible(
                        new TDengineConnectorException(
                                CommonErrorCodeDeprecated.SQL_OPERATION_FAILED,
                                "insert error:" + element));
            }
        }
    }

    @Override
    public void close() {
        if (Objects.nonNull(conn)) {
            try {
                conn.close();
            } catch (SQLException e) {
                throw new TDengineConnectorException(
                        CommonErrorCodeDeprecated.WRITER_OPERATION_FAILED,
                        "TDengine writer connection close failed",
                        e);
            }
        }
    }

    private Object[] convertDataType(Object[] objects) {
        return Arrays.stream(objects)
                .map(
                        object -> {
                            if (LocalDateTime.class.equals(object.getClass())) {
                                // transform timezone according to the config
                                return "'"
                                        + ((LocalDateTime) object)
                                                .atZone(ZoneId.systemDefault())
                                                .withZoneSameInstant(
                                                        ZoneId.of(config.getTimezone()))
                                                .format(FORMATTER)
                                        + "'";
                            } else if (String.class.equals(object.getClass())) {
                                return "'" + object + "'";
                            }
                            return object;
                        })
                .toArray();
    }
}
