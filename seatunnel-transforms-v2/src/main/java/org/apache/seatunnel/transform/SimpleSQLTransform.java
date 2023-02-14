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

package org.apache.seatunnel.transform;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.api.transform.SeaTunnelTransform;
import org.apache.seatunnel.common.config.CheckConfigUtil;
import org.apache.seatunnel.common.config.CheckResult;
import org.apache.seatunnel.transform.common.AbstractSeaTunnelTransform;
import org.apache.seatunnel.transform.sqlengine.SimpleSQLEngine;
import org.apache.seatunnel.transform.sqlengine.SimpleSQLEngineFactory;
import org.apache.seatunnel.transform.sqlengine.SimpleSQLEngineFactory.EngineType;

import com.google.auto.service.AutoService;

import static org.apache.seatunnel.transform.sqlengine.SimpleSQLEngineFactory.EngineType.INTERNAL;

@AutoService(SeaTunnelTransform.class)
public class SimpleSQLTransform extends AbstractSeaTunnelTransform {

    public static final Option<String> KEY_QUERY =
            Options.key("query")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The simple query SQL");

    public static final Option<String> KEY_ENGINE =
            Options.key("engine")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The SQL engine for SimpleSQL transform");

    private String query;

    private EngineType engineType;

    private transient SimpleSQLEngine simpleSQLEngine;

    @Override
    public String getPluginName() {
        return "SimpleSQL";
    }

    @Override
    protected void setConfig(Config pluginConfig) {
        CheckResult checkResult = CheckConfigUtil.checkAllExists(pluginConfig, KEY_QUERY.key());
        if (!checkResult.isSuccess()) {
            throw new IllegalArgumentException("Failed to check config! " + checkResult.getMsg());
        }
        query = pluginConfig.getString(KEY_QUERY.key());
        if (pluginConfig.hasPath(KEY_ENGINE.key())) {
            engineType = EngineType.valueOf(pluginConfig.getString(KEY_ENGINE.key()).toUpperCase());
        } else {
            engineType = INTERNAL;
        }
    }

    @Override
    public void open() {
        simpleSQLEngine = SimpleSQLEngineFactory.getSimpleSQLEngine(engineType);
        simpleSQLEngine.init(inputTableName, inputRowType, query);
    }

    private void tryOpen() {
        if (simpleSQLEngine == null) {
            open();
        }
    }

    @Override
    protected SeaTunnelRowType transformRowType(SeaTunnelRowType inputRowType) {
        tryOpen();
        return simpleSQLEngine.typeMapping();
    }

    @Override
    protected SeaTunnelRow transformRow(SeaTunnelRow inputRow) {
        tryOpen();
        return simpleSQLEngine.transformBySQL(inputRow);
    }

    @Override
    public void close() {
        simpleSQLEngine.close();
    }
}
