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

package org.apache.seatunnel.transform.multifieldsplit;

import org.apache.seatunnel.shade.com.fasterxml.jackson.annotation.JsonAlias;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;

import lombok.Data;
import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

@Getter
@Setter
public class MultiFieldSplitTransformConfig implements Serializable {

    public static final Option<List<SplitOP>> SPLIT_OPS =
            Options.key("splitOPs")
                    .listType(SplitOP.class)
                    .noDefaultValue()
                    .withDescription("The result fields after split");

    private SplitOP[] splitOPS;
    private String[] outputFields;

    @Data
    public static class SplitOP implements Serializable {
        private String separator;

        @JsonAlias("split_field")
        private String splitField;

        @JsonAlias("output_fields")
        private String[] outputFields;
    }

    public static MultiFieldSplitTransformConfig of(ReadonlyConfig config) {
        MultiFieldSplitTransformConfig splitTransformConfig = new MultiFieldSplitTransformConfig();
        List<SplitOP> splitOPS = config.get(SPLIT_OPS);
        String[] allOutputFields =
                splitOPS.stream()
                        .flatMap(splitOP -> Arrays.stream(splitOP.getOutputFields()))
                        .toArray(String[]::new);
        splitTransformConfig.setSplitOPS(splitOPS.toArray(new SplitOP[0]));
        splitTransformConfig.setOutputFields(allOutputFields);
        return splitTransformConfig;
    }
}
