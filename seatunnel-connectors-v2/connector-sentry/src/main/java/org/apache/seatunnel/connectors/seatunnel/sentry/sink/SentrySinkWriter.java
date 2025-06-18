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

package org.apache.seatunnel.connectors.seatunnel.sentry.sink;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.common.sink.AbstractSinkWriter;
import org.apache.seatunnel.connectors.seatunnel.sentry.config.SentrySinkOptions;

import io.sentry.Sentry;
import io.sentry.SentryOptions;

import java.io.IOException;

/** @description: SentrySinkWriter class */
public class SentrySinkWriter extends AbstractSinkWriter<SeaTunnelRow, Void> {

    public SentrySinkWriter(ReadonlyConfig pluginConfig) {
        SentryOptions options = new SentryOptions();
        options.setDsn(pluginConfig.get(SentrySinkOptions.DSN));
        if (pluginConfig.getOptional(SentrySinkOptions.ENV).isPresent()) {
            options.setEnvironment(pluginConfig.get(SentrySinkOptions.ENV));
        }
        if (pluginConfig.getOptional(SentrySinkOptions.RELEASE).isPresent()) {
            options.setRelease(pluginConfig.get(SentrySinkOptions.RELEASE));
        }
        if (pluginConfig.getOptional(SentrySinkOptions.CACHE_DIRPATH).isPresent()) {
            options.setCacheDirPath(pluginConfig.get(SentrySinkOptions.CACHE_DIRPATH));
        }
        if (pluginConfig.getOptional(SentrySinkOptions.MAX_CACHEITEMS).isPresent()) {
            options.setMaxCacheItems(pluginConfig.get(SentrySinkOptions.MAX_CACHEITEMS));
        }
        if (pluginConfig.getOptional(SentrySinkOptions.MAX_QUEUESIZE).isPresent()) {
            options.setMaxQueueSize(pluginConfig.get(SentrySinkOptions.MAX_QUEUESIZE));
        }
        if (pluginConfig.getOptional(SentrySinkOptions.FLUSH_TIMEOUTMILLIS).isPresent()) {
            options.setFlushTimeoutMillis(pluginConfig.get(SentrySinkOptions.FLUSH_TIMEOUTMILLIS));
        }
        if (pluginConfig.getOptional(SentrySinkOptions.ENABLE_EXTERNAL_CONFIGURATION).isPresent()) {
            options.setEnableExternalConfiguration(
                    pluginConfig.get(SentrySinkOptions.ENABLE_EXTERNAL_CONFIGURATION));
        }
        Sentry.init(options);
    }

    @Override
    public void write(SeaTunnelRow element) throws IOException {
        Sentry.captureMessage(element.toString());
    }

    @Override
    public void close() throws IOException {
        Sentry.close();
    }
}
