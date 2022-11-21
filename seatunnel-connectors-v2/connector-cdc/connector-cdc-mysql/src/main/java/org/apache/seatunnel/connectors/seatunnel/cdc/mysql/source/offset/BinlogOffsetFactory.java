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

package org.apache.seatunnel.connectors.seatunnel.cdc.mysql.source.offset;

import org.seatunnel.connectors.cdc.base.source.offset.Offset;
import org.seatunnel.connectors.cdc.base.source.offset.OffsetFactory;

import java.util.Map;

/** An offset factory class create {@link BinlogOffset} instance. */
public class BinlogOffsetFactory extends OffsetFactory {

    public BinlogOffsetFactory() {}

    @Override
    public Offset earliest() {
        return null;
    }

    @Override
    public Offset neverStop() {
        return BinlogOffset.NO_STOPPING_OFFSET;
    }

    @Override
    public Offset latest() {
        return null;
    }

    @Override
    public Offset specific(Map<String, String> offset) {
        return new BinlogOffset(offset);
    }

    @Override
    public Offset specific(String filename, Long position) {
        return new BinlogOffset(filename, position);
    }

    @Override
    public Offset timstamp(long timestmap) {
        throw new UnsupportedOperationException("not supported create new Offset by timestamp.");
    }
}
