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

package org.apache.seatunnel.connectors.seatunnel.hugegraph.config;

import org.apache.hugegraph.structure.constant.Frequency;
import org.apache.hugegraph.structure.constant.IdStrategy;

import lombok.Data;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

@Data
public class SchemaConfig implements Serializable {

    // General config
    private LabelType type;
    private String label;
    private String tablePath;

    // Property Config
    private List<String> properties;

    // General Label Config
    private Long ttl;
    private String ttlStartTime;
    private String enableLabelIndex;
    private Map<String, Object> userdata;

    // VertexLabel config
    private IdStrategy idStrategy;
    private List<String> idFields;

    // EdgeLabel Config
    private SourceTargetConfig sourceConfig;
    private SourceTargetConfig targetConfig;
    private Frequency frequency;

    // Mapping Config
    private MappingConfig mapping;

    public enum LabelType {
        VERTEX,
        EDGE
    }

    @Data
    public static class SourceTargetConfig implements Serializable {
        private String label;
        private List<String> idFields;
    }
}
