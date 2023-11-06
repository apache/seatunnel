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

package org.apache.seatunnel.api.common;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

/**
 * This interface is the life cycle of a plugin, after a plugin created, will execute prepare method
 * to do some initialize operation.
 *
 * @deprecated SeaTunnel will not invoke prepare when init plugin, instead by {@link
 *     org.apache.seatunnel.api.table.factory.Factory}
 */
@Deprecated
public interface SeaTunnelPluginLifeCycle {

    /**
     * Use the pluginConfig to do some initialize operation.
     *
     * @param pluginConfig plugin config.
     * @throws PrepareFailException if plugin prepare failed, the {@link PrepareFailException} will
     *     throw.
     * @deprecated SeaTunnel will not invoke prepare when init plugin, instead by {@link
     *     org.apache.seatunnel.api.table.factory.Factory}
     */
    @Deprecated
    default void prepare(Config pluginConfig) throws PrepareFailException {
        throw new UnsupportedOperationException("prepare method is not supported");
    }
}
