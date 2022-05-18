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

package org.apache.seatunnel.translation.flink.types;

import org.apache.seatunnel.api.table.type.Converter;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;

import org.apache.flink.api.common.typeinfo.TypeInformation;

/**
 * Convert SeaTunnel {@link SeaTunnelDataType} to flink type.
 */
public interface FlinkTypeConverter<T1, T2> extends Converter<T1, T2> {

    /**
     * Convert SeaTunnel {@link SeaTunnelDataType} to flink {@link  TypeInformation}.
     *
     * @param seaTunnelDataType SeaTunnel {@link SeaTunnelDataType}
     * @return flink {@link TypeInformation}
     */
    @Override
    T2 convert(T1 seaTunnelDataType);

    /**
     * Convert flink {@link TypeInformation} to SeaTunnel {@link SeaTunnelDataType}.
     *
     * @param typeInformation flink {@link TypeInformation}
     * @return seatunnel {@link SeaTunnelDataType}
     */
    @Override
    T1 reconvert(T2 typeInformation);

}
