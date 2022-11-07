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

package org.apache.seatunnel.connectors.seatunnel.assertion.sink;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;

public class AssertConfig {

    public static final String RULE_TYPE = "rule_type";

    public static final String RULE_VALUE = "rule_value";

    public static final String ROW_RULES = "row_rules";

    public static final String FIELD_NAME = "field_name";

    public static final String FIELD_TYPE = "field_type";

    public static final String FIELD_VALUE = "field_value";

    public static final String FIELD_RULES = "field_rules";

    public static final Option<Rules> RULES = Options.key("rules").objectType(Rules.class)
        .noDefaultValue().withDescription("Rule definition of user's available data. Each rule represents one field validation or row num validation.");


}
