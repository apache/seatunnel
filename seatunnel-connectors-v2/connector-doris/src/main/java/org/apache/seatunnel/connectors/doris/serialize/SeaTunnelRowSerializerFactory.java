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

package org.apache.seatunnel.connectors.doris.serialize;

import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.doris.config.DorisSinkConfig;
import org.apache.seatunnel.connectors.doris.sink.writer.LoadConstants;

public class SeaTunnelRowSerializerFactory {

    /**
     * Create a DorisSerializer instance
     *
     * @param dorisSinkConfig
     * @param seaTunnelRowType
     * @return DorisSerializer
     */
    public static DorisSerializer createSerializer(
            DorisSinkConfig dorisSinkConfig, SeaTunnelRowType seaTunnelRowType) {
        return new SeaTunnelRowSerializer(
                dorisSinkConfig
                        .getStreamLoadProps()
                        .getProperty(LoadConstants.FORMAT_KEY)
                        .toLowerCase(),
                seaTunnelRowType,
                dorisSinkConfig.getStreamLoadProps().getProperty(LoadConstants.FIELD_DELIMITER_KEY),
                dorisSinkConfig.getEnableDelete(),
                dorisSinkConfig.isCaseSensitive());
    }
}
