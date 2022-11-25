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

import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.common.sink.AbstractSinkWriter;
import org.apache.seatunnel.connectors.seatunnel.sentry.config.SentryConfig;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import io.sentry.Sentry;
import io.sentry.SentryOptions;

import java.io.IOException;

/**
 * @description: SentrySinkWriter class
 */

public class SentrySinkWriter extends AbstractSinkWriter<SeaTunnelRow, Void> {
    private SeaTunnelRowType seaTunnelRowType;

    public SentrySinkWriter(SeaTunnelRowType seaTunnelRowType,
                            Config pluginConfig) {
        SentryOptions options = new SentryOptions();
        options.setDsn(pluginConfig.getString(SentryConfig.DSN.key()));
        if (pluginConfig.hasPath(SentryConfig.ENV.key())) {
            options.setEnvironment(pluginConfig.getString(SentryConfig.ENV.key()));
        }
        if (pluginConfig.hasPath(SentryConfig.RELEASE.key())) {
            options.setRelease(pluginConfig.getString(SentryConfig.RELEASE.key()));
        }
        if (pluginConfig.hasPath(SentryConfig.CACHE_DIRPATH.key())) {
            options.setCacheDirPath(pluginConfig.getString(SentryConfig.CACHE_DIRPATH.key()));
        }
        if (pluginConfig.hasPath(SentryConfig.MAX_CACHEITEMS.key())) {
            options.setMaxCacheItems(pluginConfig.getInt(SentryConfig.MAX_CACHEITEMS.key()));
        }
        if (pluginConfig.hasPath(SentryConfig.MAX_QUEUESIZE.key())) {
            options.setMaxQueueSize(pluginConfig.getInt(SentryConfig.MAX_QUEUESIZE.key()));
        }
        if (pluginConfig.hasPath(SentryConfig.FLUSH_TIMEOUTMILLIS.key())) {
            options.setFlushTimeoutMillis(pluginConfig.getLong(SentryConfig.FLUSH_TIMEOUTMILLIS.key()));
        }
        if (pluginConfig.hasPath(SentryConfig.ENABLE_EXTERNAL_CONFIGURATION.key())) {
            options.setEnableExternalConfiguration(pluginConfig.getBoolean(SentryConfig.ENABLE_EXTERNAL_CONFIGURATION.key()));
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
