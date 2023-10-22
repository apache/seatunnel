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
package org.apache.seatunnel.transform.jsonpath;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.transform.exception.TransformException;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.seatunnel.transform.exception.JsonPathTransformErrorCode.DEST_FIELD_MUST_NOT_EMPTY;
import static org.apache.seatunnel.transform.exception.JsonPathTransformErrorCode.FIELDS_MUST_NOT_EMPTY;
import static org.apache.seatunnel.transform.exception.JsonPathTransformErrorCode.PATH_MUST_NOT_EMPTY;
import static org.apache.seatunnel.transform.exception.JsonPathTransformErrorCode.SRC_FIELD_MUST_NOT_EMPTY;

public class JsonPathTransformConfig implements Serializable {

    public static final Option<String> PATH =
            Options.key("path")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("JSONPath for Selecting Field from JSON.");

    public static final Option<String> SRC_FIELD =
            Options.key("src_field")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("JSON source field.");

    public static final Option<String> DEST_FIELD =
            Options.key("dest_field")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("output field.");
    public static final Option<List<Map>> FIELDS =
            Options.key("fields")
                    .listType(Map.class)
                    .noDefaultValue()
                    .withDescription("supports configuring multiple fields.");

    private final List<FiledConfig> filedConfigs;

    public List<FiledConfig> getFiledConfigs() {
        return filedConfigs;
    }

    public JsonPathTransformConfig(List<FiledConfig> filedConfigs) {
        this.filedConfigs = filedConfigs;
    }

    public static JsonPathTransformConfig of(ReadonlyConfig config) {
        Optional<List<Map>> optional = config.getOptional(FIELDS);
        List<Map> fields =
                optional.orElseThrow(
                        () ->
                                new TransformException(
                                        FIELDS_MUST_NOT_EMPTY,
                                        FIELDS_MUST_NOT_EMPTY.getErrorMessage()));
        return new JsonPathTransformConfig(check(fields));
    }

    private static List<FiledConfig> check(List<Map> filedConfigs) {
        List<FiledConfig> configs = new ArrayList<>(filedConfigs.size());
        if (filedConfigs == null || filedConfigs.isEmpty()) {
            throw new TransformException(
                    FIELDS_MUST_NOT_EMPTY, FIELDS_MUST_NOT_EMPTY.getErrorMessage());
        }
        for (Map filedConfig : filedConfigs) {
            configs.add(checkOneField(filedConfig));
        }
        return configs;
    }

    private static FiledConfig checkOneField(Map config) {
        String path = (String) config.getOrDefault(PATH.key(), "");
        if (path == null || "".equals(path)) {
            throw new TransformException(
                    PATH_MUST_NOT_EMPTY, PATH_MUST_NOT_EMPTY.getErrorMessage());
        }
        String srcField = (String) config.getOrDefault(SRC_FIELD.key(), "");
        if (srcField == null || "".equals(srcField)) {
            throw new TransformException(
                    SRC_FIELD_MUST_NOT_EMPTY, SRC_FIELD_MUST_NOT_EMPTY.getErrorMessage());
        }
        String destField = (String) config.getOrDefault(DEST_FIELD.key(), "");
        if (destField == null || "".equals(destField)) {
            throw new TransformException(
                    DEST_FIELD_MUST_NOT_EMPTY, DEST_FIELD_MUST_NOT_EMPTY.getErrorMessage());
        }
        return new FiledConfig(path, srcField, destField);
    }
}
