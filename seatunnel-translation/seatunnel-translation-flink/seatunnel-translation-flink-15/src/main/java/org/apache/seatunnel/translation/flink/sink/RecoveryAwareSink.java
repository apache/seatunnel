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

package org.apache.seatunnel.translation.flink.sink;

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.sink2.Committer;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.StatefulSink;
import org.apache.flink.api.connector.sink2.TwoPhaseCommittingSink;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.connector.sink2.CommittableMessage;
import org.apache.flink.streaming.api.connector.sink2.WithPostCommitTopology;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.transformations.SinkV1Adapter;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Optional;

/** Preserves V1 operator state while identifying restored committables on Flink 1.15+. */
public final class RecoveryAwareSink<InputT, CommT, StateT>
        implements StatefulSink<InputT, FlinkWriterState<StateT>>,
                WithPostCommitTopology<InputT, CommitWrapper<CommT>> {
    private final StatefulSink<InputT, FlinkWriterState<StateT>> stateful;
    private final WithPostCommitTopology<InputT, CommitWrapper<CommT>> committing;

    private RecoveryAwareSink(Sink<InputT> delegate) {
        stateful = (StatefulSink<InputT, FlinkWriterState<StateT>>) delegate;
        committing = (WithPostCommitTopology<InputT, CommitWrapper<CommT>>) delegate;
    }

    public static <I, C, S, G> Sink<I> wrap(FlinkSink<I, C, S, G> sink) {
        Sink<I> delegate = SinkV1Adapter.wrap(new RecoverableLegacySink<>(sink));
        if (delegate instanceof WithPostCommitTopology) {
            if (delegate instanceof StatefulSink) {
                return new RecoveryAwareSink<>(delegate);
            }
            // SinkV1Adapter exposes aggregate-only sinks without StatefulSink, even when the
            // legacy sink offers writer state. Preserve that capability and operator-state shape.
            return new CommittingRecoveryAwareSink<>(
                    (WithPostCommitTopology<I, CommitWrapper<C>>) delegate);
        }
        return delegate;
    }

    @Override
    public Writer<InputT, CommitWrapper<CommT>, FlinkWriterState<StateT>> createWriter(
            Sink.InitContext context) throws IOException {
        return new Writer<>(stateful.createWriter(context));
    }

    @Override
    public Writer<InputT, CommitWrapper<CommT>, FlinkWriterState<StateT>> restoreWriter(
            Sink.InitContext context, Collection<FlinkWriterState<StateT>> states)
            throws IOException {
        return new Writer<>(stateful.restoreWriter(context, states));
    }

    @Override
    public SimpleVersionedSerializer<FlinkWriterState<StateT>> getWriterStateSerializer() {
        return stateful.getWriterStateSerializer();
    }

    @Override
    public Committer<CommitWrapper<CommT>> createCommitter() throws IOException {
        return committing.createCommitter();
    }

    @Override
    public SimpleVersionedSerializer<CommitWrapper<CommT>> getCommittableSerializer() {
        return committing.getCommittableSerializer();
    }

    @Override
    public void addPostCommitTopology(DataStream<CommittableMessage<CommitWrapper<CommT>>> stream) {
        committing.addPostCommitTopology(new RecoveryDataStream<>(stream));
    }

    private static final class CommittingRecoveryAwareSink<I, C>
            implements WithPostCommitTopology<I, CommitWrapper<C>> {
        private final WithPostCommitTopology<I, CommitWrapper<C>> delegate;

        private CommittingRecoveryAwareSink(WithPostCommitTopology<I, CommitWrapper<C>> delegate) {
            this.delegate = delegate;
        }

        @Override
        public PrecommittingSinkWriter<I, CommitWrapper<C>> createWriter(Sink.InitContext context)
                throws IOException {
            return delegate.createWriter(context);
        }

        @Override
        public Committer<CommitWrapper<C>> createCommitter() throws IOException {
            return delegate.createCommitter();
        }

        @Override
        public SimpleVersionedSerializer<CommitWrapper<C>> getCommittableSerializer() {
            return delegate.getCommittableSerializer();
        }

        @Override
        public void addPostCommitTopology(DataStream<CommittableMessage<CommitWrapper<C>>> stream) {
            delegate.addPostCommitTopology(new RecoveryDataStream<>(stream));
        }
    }

    static final class RecoveryDataStream<T>
            extends DataStream<CommittableMessage<CommitWrapper<T>>> {
        RecoveryDataStream(DataStream<CommittableMessage<CommitWrapper<T>>> stream) {
            super(stream.getExecutionEnvironment(), stream.getTransformation());
        }

        @Override
        public DataStream<CommittableMessage<CommitWrapper<T>>> global() {
            return new RecoveryDataStream<>(super.global());
        }

        @Override
        public <R> SingleOutputStreamOperator<R> transform(
                String name,
                TypeInformation<R> type,
                OneInputStreamOperator<CommittableMessage<CommitWrapper<T>>, R> operator) {
            return super.transform(name, type, new RestoredCommittableOperatorFactory<>(operator));
        }
    }

    private static final class RecoverableLegacySink<I, C, S, G>
            implements org.apache.flink.api.connector.sink.Sink<
                    I, CommitWrapper<C>, FlinkWriterState<S>, G> {
        private final FlinkSink<I, C, S, G> delegate;
        // Serializer and committer factories share this sink instance so deserialized payloads
        // are marked before the same operator reconciles restored state.
        private transient RecoveredCommitTracker tracker;

        private RecoverableLegacySink(FlinkSink<I, C, S, G> delegate) {
            this.delegate = delegate;
        }

        private RecoveredCommitTracker tracker() {
            if (tracker == null) {
                tracker = new RecoveredCommitTracker();
            }
            return tracker;
        }

        @Override
        public org.apache.flink.api.connector.sink.SinkWriter<
                        I, CommitWrapper<C>, FlinkWriterState<S>>
                createWriter(
                        org.apache.flink.api.connector.sink.Sink.InitContext context,
                        List<FlinkWriterState<S>> states)
                        throws IOException {
            return delegate.createWriter(context, states);
        }

        @Override
        public Optional<org.apache.flink.api.connector.sink.Committer<CommitWrapper<C>>>
                createCommitter() throws IOException {
            return delegate.createCommitter();
        }

        @Override
        public Optional<org.apache.flink.api.connector.sink.GlobalCommitter<CommitWrapper<C>, G>>
                createGlobalCommitter() throws IOException {
            return delegate.createRecoverableGlobalCommitter(tracker());
        }

        @Override
        public Optional<SimpleVersionedSerializer<CommitWrapper<C>>> getCommittableSerializer() {
            return delegate.getCommittableSerializer()
                    .map(serializer -> new RecoveryCommitSerializer<>(serializer, tracker()));
        }

        @Override
        public Optional<SimpleVersionedSerializer<G>> getGlobalCommittableSerializer() {
            return delegate.getGlobalCommittableSerializer();
        }

        @Override
        public Optional<SimpleVersionedSerializer<FlinkWriterState<S>>> getWriterStateSerializer() {
            return delegate.getWriterStateSerializer();
        }
    }

    public static final class Writer<I, C, S>
            implements StatefulSink.StatefulSinkWriter<I, S>,
                    TwoPhaseCommittingSink.PrecommittingSinkWriter<I, C> {
        private final StatefulSink.StatefulSinkWriter<I, S> delegate;

        private Writer(StatefulSink.StatefulSinkWriter<I, S> delegate) {
            this.delegate = delegate;
        }

        @Override
        public void write(I value, SinkWriter.Context context)
                throws IOException, InterruptedException {
            delegate.write(value, context);
        }

        @Override
        public void flush(boolean endOfInput) throws IOException, InterruptedException {
            delegate.flush(endOfInput);
        }

        @Override
        public List<S> snapshotState(long checkpointId) throws IOException {
            return delegate.snapshotState(checkpointId);
        }

        @Override
        public Collection<C> prepareCommit() throws IOException, InterruptedException {
            return ((TwoPhaseCommittingSink.PrecommittingSinkWriter<I, C>) delegate)
                    .prepareCommit();
        }

        @Override
        public void writeWatermark(Watermark watermark) throws IOException, InterruptedException {
            delegate.writeWatermark(watermark);
        }

        @Override
        public void close() throws Exception {
            delegate.close();
        }
    }
}
