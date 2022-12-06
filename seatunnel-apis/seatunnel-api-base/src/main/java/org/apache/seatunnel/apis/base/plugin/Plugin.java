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

package org.apache.seatunnel.apis.base.plugin;

import org.apache.seatunnel.apis.base.env.RuntimeEnv;
import org.apache.seatunnel.common.config.CheckResult;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import java.io.Serializable;

/**
 * A base interface indicates belonging to SeaTunnel.
 * Plugin will be used as follows:
 * <pre>{@code
 *      try(Plugin<?> plugin = new PluginA<>()) {
 *          plugin.setConfig(Config);
 *          CheckResult checkResult = plugin.checkConfig();
 *          if (checkResult.getSuccess()) {
 *              plugin.prepare();
 *              // plugin execute code
 *          }
 *      }
 *
 * }</pre>
 */

public interface Plugin<T extends RuntimeEnv> extends Serializable, AutoCloseable {
    String RESULT_TABLE_NAME = "result_table_name";
    String SOURCE_TABLE_NAME = "source_table_name";

    void setConfig(Config config);

    Config getConfig();

    default CheckResult checkConfig() {
        return CheckResult.success();
    }

    /**
     * This is a lifecycle method, this method will be executed after Plugin created.
     *
     * @param env environment
     */
    default void prepare(T env) {

    }

    /**
     * This is a lifecycle method, this method will be executed before Plugin destroy.
     * It's used to release some resource.
     *
     * @throws Exception when close failed.
     */
    @Override
    default void close() throws Exception {

    }

    /**
     * Return the plugin name, this is used in seatunnel conf DSL.
     *
     * @return plugin name.
     */
    default String getPluginName() {
        return this.getClass().getSimpleName();
    }
}
