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

package org.apache.spark.sql.execution.streaming.continuous.seatunnel.rpc;

import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;

import org.apache.spark.SparkEnv;
import org.apache.spark.rpc.RpcCallContext;
import org.apache.spark.rpc.RpcEndpointRef;
import org.apache.spark.rpc.RpcEnv;
import org.apache.spark.rpc.ThreadSafeRpcEndpoint;

import scala.PartialFunction;
import scala.reflect.ClassTag;
import scala.runtime.BoxedUnit;

import java.io.Serializable;

public class SplitEnumeratorEndpoint implements ThreadSafeRpcEndpoint {

    @Override
    public RpcEnv rpcEnv() {
        return SparkEnv.get().rpcEnv();
    }

    @Override
    public PartialFunction<Object, BoxedUnit> receiveAndReply(RpcCallContext context) {
        return new PartialFunction<Object, BoxedUnit>() {
            @Override
            public boolean isDefinedAt(Object x) {
                return x instanceof PollNext;
            }

            @Override
            public BoxedUnit apply(Object v1) {
                PollNext pollNextReq = (PollNext) v1;
                context.reply(new PollResponse(pollNextReq.subTaskId, null));
                return BoxedUnit.UNIT;
            }
        };
    }

    public static class PollNext implements Serializable {
        private final int subTaskId;

        public PollNext(int subTaskId) {
            this.subTaskId = subTaskId;
        }

        public int subTaskId() {
            return this.subTaskId;
        }
    }

    public static class PollResponse implements Serializable {
        private final int subTaskId;
        private final SourceSplit split;

        public PollResponse(int subTaskId, SourceSplit split) {
            this.subTaskId = subTaskId;
            this.split = split;
        }

        public int getSubTaskId() {
            return subTaskId;
        }

        public SourceSplit getSplit() {
            return split;
        }
    }

    public static interface ISplitEnumerator {
        public SourceSplit pollNext(int subTaskId);
    }

    public static class SplitEnumeratorStub implements ISplitEnumerator {
        private final RpcEndpointRef endpointRef;

        public SplitEnumeratorStub(RpcEndpointRef endpointRef) {
            this.endpointRef = endpointRef;
        }

        @Override
        public SourceSplit pollNext(int subTaskId) {
            PollNext rep = new PollNext(subTaskId);
            PollResponse resp =
                    endpointRef.<PollResponse>askSync(rep, ClassTag.apply(PollResponse.class));
            return resp.getSplit();
        }
    }

    public static class SplitEnumeratorSrv<SplitT extends SourceSplit, StateT extends Serializable>
            implements ISplitEnumerator {
        private SourceSplitEnumerator<SplitT, StateT> splitEnumerator;

        @Override
        public SourceSplit pollNext(int subTaskId) {
            try {
                splitEnumerator.run();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            return null;
        }
    }
}
