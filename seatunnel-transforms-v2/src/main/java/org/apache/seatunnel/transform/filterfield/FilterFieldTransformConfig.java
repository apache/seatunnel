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

package org.apache.seatunnel.transform.filterfield;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;

import lombok.Getter;
import lombok.Setter;

import java.util.List;

@Setter
@Getter
public class FilterFieldTransformConfig {
    public static final Option<List<String>> KEY_FIELDS =
        Options.key("fields")
            .listType()
            .noDefaultValue()
            .withDescription(
                "The list of fields that need to be kept. Fields not in the list will be deleted");

    private String[] fields;

    public static FilterFieldTransformConfig of(ReadonlyConfig config) {
        FilterFieldTransformConfig filterFieldTransformConfig = new FilterFieldTransformConfig();
        filterFieldTransformConfig.setFields(config.get(KEY_FIELDS).toArray(new String[0]));
        return filterFieldTransformConfig;
    }
}
