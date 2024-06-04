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

package org.apache.seatunnel.engine.common.env;

import lombok.Data;

@Data
public class Version {
    private String projectVersion = "<unknown>";
    private String gitCommitId = "DecafC0ffeeD0d0F00d";
    private String buildTime = "1970-01-01T00:00:00+0000";
    private String commitTime = "1970-01-01T00:00:00+0000";
    private String gitCommitAbbrev = "DeadD0d0";
}
