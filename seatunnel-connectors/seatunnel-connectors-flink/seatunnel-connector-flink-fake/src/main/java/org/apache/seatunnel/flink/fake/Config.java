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
package org.apache.seatunnel.flink.fake;

import org.apache.seatunnel.flink.fake.source.FakeSource;
import org.apache.seatunnel.flink.fake.source.FakeSourceStream;

/**
 * FakeSource {@link FakeSource} and
 * FakeSourceStream {@link FakeSourceStream} configuration parameters
 */
public final class Config {
    
    public static final String MOCK_DATA_ENABLE = "mock_data_enable";
    
    public static final String MOCK_DATA_BOUNDED = "mock_data_bounded";
    
    public static final String MOCK_DATA_SCHEMA = "mock_data_schema";
    
    public static final String MOCK_DATA_SCHEMA_NAME = "name";
    
    public static final String MOCK_DATA_SCHEMA_TYPE = "type";
    
    public static final String MOCK_DATA_SCHEMA_MOCK = "mock";
    
    public static final String MOCK_DATA_SIZE = "mock_data_size";
    
    public static final String MOCK_DATA_INTERVAL = "mock_data_interval";
}
