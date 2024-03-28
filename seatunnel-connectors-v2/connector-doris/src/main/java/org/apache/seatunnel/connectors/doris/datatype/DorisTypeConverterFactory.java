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

package org.apache.seatunnel.connectors.doris.datatype;

import org.apache.seatunnel.api.table.converter.BasicTypeDefine;
import org.apache.seatunnel.api.table.converter.TypeConverter;
import org.apache.seatunnel.common.exception.CommonError;
import org.apache.seatunnel.connectors.doris.config.DorisConfig;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DorisTypeConverterFactory {
    public static TypeConverter<BasicTypeDefine> getTypeConverter(@NonNull String dorisVersion) {
        if (dorisVersion.startsWith("Doris version doris-1.")) {
            return DorisTypeConverterV1.INSTANCE;
        } else if (dorisVersion.startsWith("Doris version doris-2.")) {
            return DorisTypeConverterV2.INSTANCE;
        } else {
            throw CommonError.unsupportedVersion(DorisConfig.IDENTIFIER, dorisVersion);
        }
    }
}
