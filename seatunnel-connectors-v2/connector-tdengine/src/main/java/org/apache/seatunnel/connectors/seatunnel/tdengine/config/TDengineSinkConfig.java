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

package org.apache.seatunnel.connectors.seatunnel.tdengine.config;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;

import lombok.Builder;
import lombok.Data;

import java.io.Serializable;
import java.util.List;
import java.util.Optional;

@Data
@Builder(builderClassName = "Builder")
public class TDengineSinkConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    private String url;
    private String username;
    private String password;
    private String database;
    private String stable;
    private String timezone;
    private String writeColumns;

    public static TDengineSinkConfig of(ReadonlyConfig config) {
        Builder builder = TDengineSinkConfig.builder();

        builder.url(config.get(TDengineSinkOptions.URL));
        builder.username(config.get(TDengineSinkOptions.USERNAME));
        builder.password(config.get(TDengineSinkOptions.PASSWORD));
        builder.database(config.get(TDengineSinkOptions.DATABASE));
        builder.stable(config.get(TDengineSinkOptions.STABLE));

        Optional<String> optionalTimezone = config.getOptional(TDengineSinkOptions.TIMEZONE);

        builder.timezone(optionalTimezone.orElseGet(TDengineSinkOptions.TIMEZONE::defaultValue));
        Optional<List<String>> optionalWriteColumns =
                config.getOptional(TDengineSinkOptions.WRITE_COLUMNS);
        if (optionalWriteColumns.isPresent()) {
            builder.writeColumns(String.join(",", optionalWriteColumns.get()));
        }
        return builder.build();
    }
}
