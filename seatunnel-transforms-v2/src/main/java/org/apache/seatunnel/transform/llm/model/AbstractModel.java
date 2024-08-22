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

package org.apache.seatunnel.transform.llm.model;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.api.table.type.SqlType;
import org.apache.seatunnel.format.json.RowToJsonConverters;

import java.io.IOException;
import java.util.List;

public abstract class AbstractModel implements Model {

    protected static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private final RowToJsonConverters.RowToJsonConverter rowToJsonConverters;
    private final String prompt;
    private final SqlType outputType;

    public AbstractModel(SeaTunnelRowType rowType, SqlType outputType, String prompt) {
        this.prompt = prompt;
        this.outputType = outputType;
        this.rowToJsonConverters = new RowToJsonConverters().createConverter(rowType, null);
    }

    private String getPromptWithLimit() {
        return prompt
                + "\n The following rules need to be followed: "
                + "\n 1. The received data is an array, and the result is returned in the form of an array."
                + "\n 2. Only the result needs to be returned, and no other information can be returned."
                + "\n 3. The element type of the array is "
                + outputType.toString()
                + "."
                + "\n Eg: [\"value1\", \"value2\"]";
    }

    @Override
    public List<String> inference(List<SeaTunnelRow> rows) throws IOException {
        ArrayNode rowsNode = OBJECT_MAPPER.createArrayNode();
        for (SeaTunnelRow row : rows) {
            ObjectNode rowNode = OBJECT_MAPPER.createObjectNode();
            rowToJsonConverters.convert(OBJECT_MAPPER, rowNode, row);
            rowsNode.add(rowNode);
        }
        return chatWithModel(getPromptWithLimit(), OBJECT_MAPPER.writeValueAsString(rowsNode));
    }

    protected abstract List<String> chatWithModel(String promptWithLimit, String rowsJson)
            throws IOException;
}
