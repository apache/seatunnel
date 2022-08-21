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

package org.apache.seatunnel.api.table.factory;

import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.api.table.connector.TableSource;

import java.io.Serializable;

/**
 * This is an SPI interface, used to create {@link TableSource}. Each plugin need to have it own implementation.
 * todo: now we have not use this interface, we directly use {@link org.apache.seatunnel.api.source.SeaTunnelSource} as the SPI interface
 */
public interface TableSourceFactory extends Factory {

    <T, SplitT extends SourceSplit, StateT extends Serializable> TableSource<T, SplitT, StateT> createSource(TableFactoryContext context);
}
