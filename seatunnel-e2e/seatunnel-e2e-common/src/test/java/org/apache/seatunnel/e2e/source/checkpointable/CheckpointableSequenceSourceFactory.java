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

package org.apache.seatunnel.e2e.source.checkpointable;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.api.table.connector.TableSource;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSourceFactory;
import org.apache.seatunnel.api.table.factory.TableSourceFactoryContext;

import com.google.auto.service.AutoService;

import java.io.Serializable;

@AutoService(Factory.class)
public class CheckpointableSequenceSourceFactory implements TableSourceFactory {

    public static final Option<Long> START_OFFSET =
            Options.key("start_offset").longType().defaultValue(0L);

    public static final Option<Long> END_OFFSET =
            Options.key("end_offset").longType().defaultValue(Long.MAX_VALUE);

    public static final Option<Integer> SPLIT_NUM =
            Options.key("split_num").intType().defaultValue(1);

    public static final Option<Integer> RECORDS_PER_POLL =
            Options.key("records_per_poll").intType().defaultValue(1);

    public static final Option<Long> EMIT_INTERVAL_MS =
            Options.key("emit_interval_ms").longType().defaultValue(50L);

    /**
     * When enabled, the enumerator hands at most one split to each reader instead of draining the
     * whole pending queue into the first reader that registers.
     *
     * <p>The default keeps the historical behaviour so existing fixtures and tests are unaffected.
     * The per-reader mode exists so tests can give every parallel reader its own checkpoint state:
     * with the default mode a single reader owns all splits and the other subtasks checkpoint an
     * empty state, which cannot exercise per-subtask state restoration.
     */
    public static final Option<Boolean> ONE_SPLIT_PER_READER =
            Options.key("one_split_per_reader")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Assign at most one split to every source reader instead of assigning all pending splits to the first registered reader");

    /**
     * When set to a non-negative value, every reader stops emitting as soon as its own split has
     * advanced this many offsets from the split's start and keeps polling without emitting until
     * the file named by {@link #RESUME_FILE} exists.
     *
     * <p>This turns "the job is interrupted in the middle of its input" into a controlled
     * precondition: the readers park at a known, non-terminal offset of their own split instead of
     * racing to the end of a finite range while the test prepares the savepoint. The historical
     * default {@code -1} keeps the fixture emitting without a gate.
     */
    public static final Option<Long> PAUSE_AFTER_OFFSETS_PER_SPLIT =
            Options.key("pause_after_offsets_per_split")
                    .longType()
                    .defaultValue(-1L)
                    .withDescription(
                            "Stop emitting after every split advanced this many offsets and resume once resume_file exists; negative disables the gate");

    /**
     * Path of the file that releases {@link #PAUSE_AFTER_OFFSETS_PER_SPLIT}. The gate is open when
     * this option is empty, and also when the file already exists, so a restored run whose split
     * state is past the pause offset never parks again.
     */
    public static final Option<String> RESUME_FILE =
            Options.key("resume_file")
                    .stringType()
                    .defaultValue("")
                    .withDescription(
                            "File whose creation releases a reader paused by pause_after_offsets_per_split");

    @Override
    public String factoryIdentifier() {
        return "CheckpointableSequenceSource";
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .optional(
                        START_OFFSET,
                        END_OFFSET,
                        SPLIT_NUM,
                        RECORDS_PER_POLL,
                        EMIT_INTERVAL_MS,
                        ONE_SPLIT_PER_READER,
                        PAUSE_AFTER_OFFSETS_PER_SPLIT,
                        RESUME_FILE)
                .build();
    }

    @Override
    public <T, SplitT extends SourceSplit, StateT extends Serializable>
            TableSource<T, SplitT, StateT> createSource(TableSourceFactoryContext context) {
        return () ->
                (SeaTunnelSource<T, SplitT, StateT>)
                        new CheckpointableSequenceSource(context.getOptions());
    }

    @Override
    public Class<? extends SeaTunnelSource> getSourceClass() {
        return CheckpointableSequenceSource.class;
    }
}
