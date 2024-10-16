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

package org.apache.seatunnel.connectors.seatunnel.wechat.sink;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.connectors.seatunnel.http.sink.HttpSink;
import org.apache.seatunnel.connectors.seatunnel.http.sink.HttpSinkWriter;
import org.apache.seatunnel.connectors.seatunnel.wechat.sink.config.WeChatSinkConfig;

import java.util.Optional;

public class WeChatSink extends HttpSink {

    public WeChatSink(Config pluginConfig, CatalogTable catalogTable) {
        super(pluginConfig, catalogTable);
    }

    @Override
    public String getPluginName() {
        return "WeChat";
    }

    @Override
    public HttpSinkWriter createWriter(SinkWriter.Context context) {
        return new HttpSinkWriter(
                seaTunnelRowType,
                super.httpParameter,
                new WeChatBotMessageSerializationSchema(
                        new WeChatSinkConfig(pluginConfig), seaTunnelRowType));
    }

    @Override
    public Optional<CatalogTable> getWriteCatalogTable() {
        return super.getWriteCatalogTable();
    }
}
