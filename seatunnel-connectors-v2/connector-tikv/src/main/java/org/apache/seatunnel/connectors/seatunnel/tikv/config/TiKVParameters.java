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

package org.apache.seatunnel.connectors.seatunnel.tikv.config;

import org.apache.seatunnel.api.common.PrepareFailException;
import org.apache.seatunnel.common.constants.PluginType;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import lombok.Builder;
import lombok.Data;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

@Data
public class TiKVParameters implements Serializable {
    /**
     * PD server host
     */
    public String host;
    /**
     * PD server port
     */
    public Integer pdPort;
    /**
     * TiKV server addresses
     */
    public String pdAddresses;

    public PluginType pluginType;

    private String keyword;

    private Set<String> keywords;

    private List<Range> rangeList;

    private Integer limit;

    private TiKVDataType tikvDataType;

    public String getPdAddresses() {
        return this.getHost() + ":" + this.getPdPort();
    }

    public void initConfig(Config config) {
        // set host
        this.host = config.getString(TiKVConfig.HOST);

        // set port
        this.pdPort = config.getInt(TiKVConfig.PD_PORT);

        // default: KEY DATA_TYPE
        this.tikvDataType = TiKVDataType.getDataType(config.getString(TiKVConfig.DATA_TYPE));

        // set key
        if (config.hasPath(TiKVConfig.KEYWORD)) {
            this.keyword = config.getString(TiKVConfig.KEYWORD);
        }

        if (config.hasPath(TiKVConfig.KEYWORDS)) {
            this.keywords = Arrays.stream(config.getString(TiKVConfig.KEYWORDS).split(",")).collect(Collectors.toSet());
        }

        // default 10000
        this.limit = config.hasPath(TiKVConfig.LIMIT) ? config.getInt(TiKVConfig.LIMIT) : TiKVConfig.LIMIT_DEFAULT;

        if (TiKVDataType.isRangDataType(this.tikvDataType) && config.hasPath(TiKVConfig.RANGES)) {
            try {
                this.rangeList = Arrays.stream(config.getString(TiKVConfig.RANGES).split(";"))
                    .map(e -> {
                        String[] split = e.split(",");
                        if (split.length > 1) {
                            return Range.builder()
                                .startKey(split[0])
                                .endKey(split[1])
                                .build();
                        } else {
                            return Range.builder()
                                .startKey(split[0])
                                .build();
                        }
                    })
                    .collect(Collectors.toList());
            } catch (Exception e) {
                throw new PrepareFailException(TiKVConfig.NAME, this.getPluginType(), String.format(TiKVConfig.CHECK_ERROR_FORMAT, TiKVConfig.RANGES));
            }
        }
    }

    @Data
    @Builder
    public static class Range {
        private String startKey;

        private String endKey;
    }

}
