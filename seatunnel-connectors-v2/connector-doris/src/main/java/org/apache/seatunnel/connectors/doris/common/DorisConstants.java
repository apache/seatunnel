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

package org.apache.seatunnel.connectors.doris.common;

import java.util.Arrays;
import java.util.List;

public class DorisConstants {

    public static final String LOAD_URL_TEMPLATE = "http://%s/api/%s/%s/_stream_load";

    public static final List<String> DORIS_SUCCESS_STATUS = Arrays.asList("Success", "Publish Timeout");

    public static final String DORIS_STRIP_OUTER_ARRAY_OPTION = "strip_outer_array";
    public static final String DORIS_STRIP_OUTER_ARRAY_OPTION_VALUE = "true";
    public static final String DORIS_FORMAT_OPTION = "format";
    public static final String DORIS_FORMAT_OPTION_VALUE = "json";
    public static final String DORIS_LABEL_OPTION = "label";

    public static final String DORIS_LABEL_PATTERN_VALUE = "yyyyMMdd_HHmmss";

}
