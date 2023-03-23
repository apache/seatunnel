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

package org.apache.seatunnel.transform.copyfield;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;

import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;

@Getter
@Setter
public class CopyFieldTransformConfig implements Serializable {
    public static final Option<String> SRC_FIELD =
            Options.key("src_field")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Src field you want to copy");

    public static final Option<String> DEST_FIELD =
            Options.key("dest_field")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Copy Src field to Dest field");

    private String srcField;
    private int srcFieldIndex;
    private SeaTunnelDataType srcFieldDataType;
    private String destField;

    public static CopyFieldTransformConfig of(ReadonlyConfig config) {
        CopyFieldTransformConfig copyFieldTransformConfig = new CopyFieldTransformConfig();
        copyFieldTransformConfig.setSrcField(config.get(SRC_FIELD));
        copyFieldTransformConfig.setDestField(config.get(DEST_FIELD));
        return copyFieldTransformConfig;
    }
}
