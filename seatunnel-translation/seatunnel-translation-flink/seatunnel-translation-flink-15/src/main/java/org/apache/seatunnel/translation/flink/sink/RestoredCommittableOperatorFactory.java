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

import org.apache.flink.streaming.api.connector.sink2.CommittableMessage;
import org.apache.flink.streaming.api.operators.AbstractStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;

/** Uses Flink's original factory for setup, then adds recovery classification to its operator. */
final class RestoredCommittableOperatorFactory<T, R> extends AbstractStreamOperatorFactory<R>
        implements OneInputStreamOperatorFactory<CommittableMessage<CommitWrapper<T>>, R> {
    private final SimpleOperatorFactory<R> delegate;

    RestoredCommittableOperatorFactory(
            OneInputStreamOperator<CommittableMessage<CommitWrapper<T>>, R> operator) {
        delegate = SimpleOperatorFactory.of(operator);
        this.chainingStrategy = delegate.getChainingStrategy();
    }

    public <O extends StreamOperator<R>> O createStreamOperator(
            StreamOperatorParameters<R> parameters) {
        delegate.setProcessingTimeService(parameters.getProcessingTimeService());
        OneInputStreamOperator<CommittableMessage<CommitWrapper<T>>, R> operator =
                delegate.createStreamOperator(parameters);
        return (O) new RestoredCommittableOperator<>(operator);
    }

    public void setChainingStrategy(ChainingStrategy strategy) {
        super.setChainingStrategy(strategy);
        delegate.setChainingStrategy(strategy);
    }

    public Class<? extends StreamOperator> getStreamOperatorClass(ClassLoader loader) {
        return delegate.getStreamOperatorClass(loader);
    }
}
