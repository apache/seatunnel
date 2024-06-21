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

package org.apache.seatunnel.transform.jarudf;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;

import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

@Getter
@Setter
public class JarUdfTransformConfig implements Serializable {

    public static final Option<String> JAR_CLASS_NAME =
            Options.key("jar_class_name")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("class name,eg:com.oceandatum.ottomi.Test");

    public static final Option<Map<String, String>> JAR_CONNECTION_PROPERTIES =
            Options.key("jar_connection_properties")
                    .mapType()
                    .noDefaultValue()
                    .withDescription("jar save connection info");

    public static final Option<List<Integer>> COLUMN_INDEX =
            Options.key("column_index")
                    .listType(Integer.class)
                    .noDefaultValue()
                    .withDescription("column index,eg:1,23,");

    public static final Option<String> OUT_FIELD_NAME =
            Options.key("out_field_name")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("udf output column name");
}
