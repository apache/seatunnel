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

package org.apache.seatunnel.transform.chunk;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.OptionValidationException;

import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

@Getter
@Setter
public class TextChunkTransformConfig implements Serializable {

    public static final Option<String> TEXT_FIELD =
            Options.key("text_field")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The source text field to split into chunks");

    public static final Option<String> OUTPUT_FIELD =
            Options.key("output_field")
                    .stringType()
                    .defaultValue("chunk")
                    .withDescription("The output field name holding each chunk (STRING)");

    public static final Option<String> CHUNK_INDEX_FIELD =
            Options.key("chunk_index_field")
                    .stringType()
                    .defaultValue("chunk_index")
                    .withDescription(
                            "The output field name holding the chunk sequence index within a "
                                    + "document (INT, 0-based)");

    public static final Option<Integer> CHUNK_SIZE =
            Options.key("chunk_size")
                    .intType()
                    .defaultValue(1000)
                    .withDescription("Maximum length of each chunk, measured in characters");

    public static final Option<Integer> OVERLAP_SIZE =
            Options.key("overlap_size")
                    .intType()
                    .defaultValue(0)
                    .withDescription(
                            "Overlap length between adjacent chunks, measured in characters. "
                                    + "Must be less than chunk_size");

    public static final Option<List<String>> SEPARATORS =
            Options.key("separators")
                    .listType()
                    .defaultValue(Arrays.asList("\n\n", "\n", "。", "！", "？", ". ", " "))
                    .withDescription(
                            "Separators tried in priority order to avoid cutting mid-sentence. "
                                    + "If left empty, falls back to fixed-size splitting");

    private String textField;
    private String outputField;
    private String chunkIndexField;
    private int chunkSize;
    private int overlapSize;
    private List<String> separators;

    public static TextChunkTransformConfig of(ReadonlyConfig config) {
        TextChunkTransformConfig c = new TextChunkTransformConfig();
        c.setTextField(config.get(TEXT_FIELD));
        c.setOutputField(config.get(OUTPUT_FIELD));
        c.setChunkIndexField(config.get(CHUNK_INDEX_FIELD));
        c.setChunkSize(config.get(CHUNK_SIZE));
        c.setOverlapSize(config.get(OVERLAP_SIZE));
        c.setSeparators(config.get(SEPARATORS));
        return c;
    }

    public static void validate(TextChunkTransformConfig config) {
        if (config.getTextField() == null || config.getTextField().trim().isEmpty()) {
            throw new OptionValidationException("text_field is required, but was not configured");
        }
        if (config.getChunkSize() <= 0) {
            throw new OptionValidationException(
                    "chunk_size must be > 0, but was " + config.getChunkSize());
        }
        if (config.getOverlapSize() < 0 || config.getOverlapSize() >= config.getChunkSize()) {
            throw new OptionValidationException(
                    "overlap_size must satisfy 0 <= overlap_size < chunk_size, but was "
                            + config.getOverlapSize()
                            + " (chunk_size="
                            + config.getChunkSize()
                            + ")");
        }
        if (config.getOutputField().equals(config.getChunkIndexField())) {
            throw new OptionValidationException(
                    "output_field and chunk_index_field must be different, but both are '"
                            + config.getOutputField()
                            + "'");
        }
    }
}
