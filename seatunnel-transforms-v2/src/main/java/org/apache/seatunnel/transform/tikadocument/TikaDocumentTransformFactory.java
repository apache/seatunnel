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

package org.apache.seatunnel.transform.tikadocument;

import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.table.connector.TableTransform;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableTransformFactory;
import org.apache.seatunnel.api.table.factory.TableTransformFactoryContext;
import org.apache.seatunnel.transform.common.TransformCommonOptions;

import com.google.auto.service.AutoService;

/** Factory for TikaDocument Transform */
@AutoService(Factory.class)
public class TikaDocumentTransformFactory implements TableTransformFactory {

    @Override
    public String factoryIdentifier() {
        return TikaDocumentTransform.PLUGIN_NAME;
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                // Required options
                .required(TikaDocumentTransformConfig.SOURCE_FIELD)
                // Optional options with groups
                .optional(TikaDocumentTransformConfig.OUTPUT_FIELDS)
                .optional(TikaDocumentTransformConfig.EXTRACT_TEXT)
                .optional(TikaDocumentTransformConfig.EXTRACT_METADATA)
                .optional(TikaDocumentTransformConfig.MAX_STRING_LENGTH)
                .optional(TikaDocumentTransformConfig.REMOVE_EMPTY_LINES)
                .optional(TikaDocumentTransformConfig.TRIM_WHITESPACE)
                .optional(TikaDocumentTransformConfig.NORMALIZE_WHITESPACE)
                .optional(TikaDocumentTransformConfig.MIN_CONTENT_LENGTH)
                .optional(TikaDocumentTransformConfig.ON_PARSE_ERROR)
                .optional(TikaDocumentTransformConfig.ON_UNSUPPORTED_FORMAT)
                .optional(TikaDocumentTransformConfig.LOG_ERRORS)
                .optional(TikaDocumentTransformConfig.TIMEOUT_MS)
                // Multi-table support options
                .optional(TransformCommonOptions.MULTI_TABLES)
                .optional(TransformCommonOptions.TABLE_MATCH_REGEX)
                .build();
    }

    @Override
    public TableTransform createTransform(TableTransformFactoryContext context) {
        return () ->
                new TikaDocumentMultiCatalogTransform(
                        context.getCatalogTables(), context.getOptions());
    }
}
