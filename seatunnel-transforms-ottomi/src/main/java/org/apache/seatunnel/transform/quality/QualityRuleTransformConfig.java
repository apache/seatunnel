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

package org.apache.seatunnel.transform.quality;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;

import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;
import java.util.List;

@Getter
@Setter
public class QualityRuleTransformConfig implements Serializable {

    public static final Option<List<Integer>> COLUMNS_INDEX =
            Options.key("columns_index")
                    .listType(Integer.class)
                    .noDefaultValue()
                    .withDescription("index of columns,eg:[0,1,2].");
    public static final Option<String> RULE_TYPE =
            Options.key("rule_type")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("rule type,eg: regex,java....");
    public static final Option<String> RULE_CODE =
            Options.key("rule_code")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("rule code info.");
    public static final Option<Boolean> QUALITY_RESULT =
            Options.key("quality_result")
                    .booleanType()
                    .noDefaultValue()
                    .withDescription("data quality check result.");
}
