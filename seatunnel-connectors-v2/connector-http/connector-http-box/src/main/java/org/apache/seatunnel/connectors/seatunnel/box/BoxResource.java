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
package org.apache.seatunnel.connectors.seatunnel.box;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Names of the Box catalog and file extraction outputs.
 *
 * <p>The common folder discovery stages can run even when their own data is not selected.
 */
enum BoxResource {
    EVENTS("events"),
    SIGN_TEMPLATES("sign_templates"),
    COLLECTIONS("collections"),
    COLLECTION_ITEMS("collection_items"),
    SIGN_REQUEST("sign_request"),
    ADMIN_LOGS("admin_logs"),
    FILES("files"),
    FILE_COLLABORATIONS("file_collaborations"),
    FOLDER_COLLABORATIONS("folder_collaborations"),
    FILE_COMMENTS("file_comments"),
    RECENT_ITEMS("recent_items"),
    FILE_TASKS("file_tasks"),
    TRASHED_ITEMS("trashed_items"),
    USERS("users"),
    FOLDERS("folders"),
    TEXT_REPRESENTATION("text_representation"),
    AI_ASK("ai_ask"),
    AI_EXTRACT("ai_extract"),
    AI_EXTRACT_STRUCTURED("ai_extract_structured");

    final String name;

    BoxResource(String name) {
        this.name = name;
    }

    static BoxResource from(String name) {
        for (BoxResource value : values()) {
            if (value.name.equals(name)) {
                return value;
            }
        }
        throw BoxConfig.invalid("resources", "unknown Box resource");
    }

    static List<String> names() {
        return Arrays.stream(values())
                .map(resource -> resource.name)
                .collect(Collectors.toUnmodifiableList());
    }
}
