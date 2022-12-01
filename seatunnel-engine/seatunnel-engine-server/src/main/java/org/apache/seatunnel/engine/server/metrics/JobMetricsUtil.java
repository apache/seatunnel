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

package org.apache.seatunnel.engine.server.metrics;

import static org.apache.seatunnel.api.common.metrics.MetricTags.ADDRESS;
import static org.apache.seatunnel.api.common.metrics.MetricTags.MEMBER;

import org.apache.seatunnel.api.common.metrics.JobMetrics;
import org.apache.seatunnel.api.common.metrics.Measurement;
import org.apache.seatunnel.api.common.metrics.MetricTags;
import org.apache.seatunnel.api.common.metrics.RawJobMetrics;

import com.hazelcast.cluster.Member;
import com.hazelcast.internal.metrics.MetricConsumer;
import com.hazelcast.internal.metrics.MetricDescriptor;
import com.hazelcast.internal.metrics.impl.MetricsCompressor;
import com.hazelcast.internal.util.MapUtil;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.UnaryOperator;

public final class JobMetricsUtil {

    private JobMetricsUtil() {
    }

    public static String getTaskGroupLocationFromMetricsDescriptor(MetricDescriptor descriptor){
        for (int i = 0; i < descriptor.tagCount(); i++) {
            if (MetricTags.TASK_GROUP_LOCATION.equals(descriptor.tag(i))) {
                return descriptor.tagValue(i);
            }
        }
        return null;
    }

    public static UnaryOperator<MetricDescriptor> addMemberPrefixFn(Member member) {
        String uuid = member.getUuid().toString();
        String addr = member.getAddress().toString();
        return d -> d.copy().withTag(MEMBER, uuid).withTag(ADDRESS, addr);
    }

    public static JobMetrics toJobMetrics(List<RawJobMetrics> rawJobMetrics) {
        JobMetricsConsumer consumer = null;
        for (RawJobMetrics metrics : rawJobMetrics) {
            if (metrics.getBlob() == null) {
                continue;
            }
            if (consumer == null) {
                consumer = new JobMetricsConsumer();
            }
            consumer.timestamp = metrics.getTimestamp();
            MetricsCompressor.extractMetrics(metrics.getBlob(), consumer);
        }
        return consumer == null ? JobMetrics.empty() : JobMetrics.of(consumer.metrics);

    }

    private static class JobMetricsConsumer implements MetricConsumer {

        final Map<String, List<Measurement>> metrics = new HashMap<>();
        long timestamp;

        @Override
        public void consumeLong(MetricDescriptor descriptor, long value) {
            metrics.computeIfAbsent(descriptor.metric(), k -> new ArrayList<>())
                   .add(measurement(descriptor, value));
        }

        @Override
        public void consumeDouble(MetricDescriptor descriptor, double value) {
            metrics.computeIfAbsent(descriptor.metric(), k -> new ArrayList<>())
                .add(measurement(descriptor, value));
        }

        private Measurement measurement(MetricDescriptor descriptor, Object value) {
            Map<String, String> tags = MapUtil.createHashMap(descriptor.tagCount());
            for (int i = 0; i < descriptor.tagCount(); i++) {
                tags.put(descriptor.tag(i), descriptor.tagValue(i));
            }
            if (descriptor.discriminator() != null || descriptor.discriminatorValue() != null) {
                tags.put(descriptor.discriminator(), descriptor.discriminatorValue());
            }
            return Measurement.of(descriptor.metric(), value, timestamp, tags);
        }
    }
}
