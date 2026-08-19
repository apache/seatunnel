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

package org.apache.seatunnel.connectors.seatunnel.bigtable.source;

import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.connectors.seatunnel.bigtable.client.BigtableClient;
import org.apache.seatunnel.connectors.seatunnel.bigtable.config.BigtableParameters;

import com.google.cloud.bigtable.data.v2.models.KeyOffset;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * 为并行读取枚举 {@link BigtableSourceSplit}。
 *
 * <p>主流程：
 *
 * <ol>
 *   <li>通过 {@link BigtableClient#sampleRowKeys()} 按 tablet 近似等大小切分 key range
 *   <li>与用户配置的 {@code start_rowkey}/{@code end_rowkey} 求交
 *   <li>按 splitId 哈希取模分配给已注册 Reader
 * </ol>
 *
 * <p>切分失败（采样异常、空采样、求交为空）时回退为覆盖用户 range 的单 split，保证作业仍能跑。 Checkpoint 仍同时持久化 {@code assignedSplits} 与
 * {@code pendingSplits}（#11144）。
 */
@Slf4j
public class BigtableSourceSplitEnumerator
        implements SourceSplitEnumerator<BigtableSourceSplit, BigtableSourceState> {

    private final Context<BigtableSourceSplit> context;
    private final BigtableParameters parameters;
    private final Set<BigtableSourceSplit> assignedSplits;
    private Set<BigtableSourceSplit> pendingSplits;
    private boolean initialized = false;

    /** Enumerator 侧懒创建的 Data API client，仅用于 sampleRowKeys；单测可注入。 */
    private BigtableClient bigtableClient;

    /**
     * Guards the shared assignment state ({@code assignedSplits}, {@code pendingSplits}, {@code
     * initialized}) against concurrent enumerator callbacks. {@link #initializePendingSplits()} and
     * {@link #assignSplit(int)} must only run while this lock is held.
     */
    private final Object stateLock = new Object();

    public BigtableSourceSplitEnumerator(
            Context<BigtableSourceSplit> context, BigtableParameters parameters) {
        this(context, parameters, null, null);
    }

    public BigtableSourceSplitEnumerator(
            Context<BigtableSourceSplit> context,
            BigtableParameters parameters,
            BigtableSourceState sourceState) {
        this(context, parameters, sourceState, null);
    }

    /**
     * 包可见构造器，供单测注入 {@link BigtableClient}，避免真实连云。
     *
     * @param context 引擎分配上下文
     * @param parameters 连接与扫描参数
     * @param sourceState checkpoint 恢复状态，首次启动为 {@code null}
     * @param bigtableClient 预置 client；为 {@code null} 时在首次切分时懒创建
     */
    BigtableSourceSplitEnumerator(
            Context<BigtableSourceSplit> context,
            BigtableParameters parameters,
            BigtableSourceState sourceState,
            BigtableClient bigtableClient) {
        this.context = context;
        this.parameters = parameters;
        this.bigtableClient = bigtableClient;
        if (sourceState == null) {
            this.assignedSplits = new HashSet<>();
            this.pendingSplits = new HashSet<>();
            this.initialized = false;
        } else {
            this.assignedSplits = new HashSet<>(sourceState.getAssignedSplits());
            // Restore pending splits first so a returned-but-not-yet-reassigned split survives
            // recovery even when its ID is already present in assignedSplits.
            this.pendingSplits = new HashSet<>(sourceState.getPendingSplits());
            // Only skip table-split discovery when the checkpoint captured real enumerator
            // progress.
            // An empty-empty checkpoint (before any reader registered) must still discover splits.
            this.initialized = !this.assignedSplits.isEmpty() || !this.pendingSplits.isEmpty();
        }
    }

    @Override
    public void open() {
        // State is fully initialized in the constructor; nothing to reset on open().
    }

    @Override
    public void run() throws Exception {
        // Splits are assigned lazily when readers register.
    }

    @Override
    public void close() throws IOException {
        if (bigtableClient != null) {
            bigtableClient.close();
            bigtableClient = null;
        }
    }

    @Override
    public void addSplitsBack(List<BigtableSourceSplit> splits, int subtaskId) {
        if (!splits.isEmpty()) {
            synchronized (stateLock) {
                pendingSplits.addAll(splits);
                if (context.registeredReaders().contains(subtaskId)) {
                    assignSplit(subtaskId);
                }
            }
        }
    }

    @Override
    public int currentUnassignedSplitSize() {
        synchronized (stateLock) {
            return pendingSplits.size();
        }
    }

    @Override
    public void registerReader(int subtaskId) {
        synchronized (stateLock) {
            initializePendingSplits();
            assignSplit(subtaskId);
        }
    }

    @Override
    public BigtableSourceState snapshotState(long checkpointId) throws Exception {
        synchronized (stateLock) {
            return new BigtableSourceState(assignedSplits, pendingSplits);
        }
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {}

    @Override
    public void handleSplitRequest(int subtaskId) {}

    private void initializePendingSplits() {
        if (initialized) {
            return;
        }
        Set<BigtableSourceSplit> tableSplits = buildSplits();
        Set<String> existingIds =
                pendingSplits.stream()
                        .map(BigtableSourceSplit::splitId)
                        .collect(Collectors.toSet());
        existingIds.addAll(
                assignedSplits.stream()
                        .map(BigtableSourceSplit::splitId)
                        .collect(Collectors.toSet()));
        tableSplits.stream()
                .filter(s -> !existingIds.contains(s.splitId()))
                .forEach(pendingSplits::add);
        initialized = true;
    }

    /**
     * 通过 {@link BigtableClient#sampleRowKeys()} 生成多 split；失败则回退单 split。
     *
     * <p>采样点按字典序切成半开区间 {@code [prev, current)}，再与用户 range 求交。若最后一个采样 key 不是空（表尾），额外补一段 {@code
     * [lastSample, "")} 以免漏扫表尾。
     *
     * @return 至少一个 split；切分失败时为覆盖用户 range 的单 split
     */
    Set<BigtableSourceSplit> buildSplits() {
        String userStart = parameters.getStartRowkey() != null ? parameters.getStartRowkey() : "";
        String userEnd = parameters.getEndRowkey() != null ? parameters.getEndRowkey() : "";

        List<KeyOffset> samples;
        try {
            samples = getBigtableClient().sampleRowKeys();
        } catch (Exception e) {
            log.warn(
                    "sampleRowKeys failed for table [{}], fallback to single split",
                    parameters.getTable(),
                    e);
            return Collections.singleton(new BigtableSourceSplit(0, userStart, userEnd));
        }

        if (samples == null || samples.isEmpty()) {
            return Collections.singleton(new BigtableSourceSplit(0, userStart, userEnd));
        }

        Set<BigtableSourceSplit> splits = new LinkedHashSet<>();
        String rangeStart = "";
        int index = 0;
        for (KeyOffset sample : samples) {
            String rangeEnd = keyToUtf8(sample);
            index = addIntersectedSplit(splits, index, rangeStart, rangeEnd, userStart, userEnd);
            rangeStart = rangeEnd;
        }

        // 最后一个 sample 不是空 key 时，API 未给出表尾哨兵，补齐 [lastSample, 表尾)
        if (!rangeStart.isEmpty()) {
            addIntersectedSplit(splits, index, rangeStart, "", userStart, userEnd);
        }

        if (splits.isEmpty()) {
            return Collections.singleton(new BigtableSourceSplit(0, userStart, userEnd));
        }
        log.info(
                "Enumerated {} Bigtable splits for table [{}]",
                splits.size(),
                parameters.getTable());
        return splits;
    }

    /**
     * 把 tablet 区间与用户 range 求交后加入结果集。
     *
     * @return 若加入了 split 则返回 {@code index + 1}，否则原 {@code index}
     */
    private static int addIntersectedSplit(
            Set<BigtableSourceSplit> splits,
            int index,
            String rangeStart,
            String rangeEnd,
            String userStart,
            String userEnd) {
        String splitStart = maxStart(rangeStart, userStart);
        String splitEnd = minEnd(rangeEnd, userEnd);
        if (isValidRange(splitStart, splitEnd)) {
            splits.add(new BigtableSourceSplit(index, splitStart, splitEnd));
            return index + 1;
        }
        return index;
    }

    /** 将采样点 key 转为 UTF-8；null / 空 ByteString 表示表尾。 */
    private static String keyToUtf8(KeyOffset sample) {
        if (sample == null || sample.getKey() == null) {
            return "";
        }
        return sample.getKey().toStringUtf8();
    }

    /** start 取较大者（空表示表头，视为最小）。 */
    private static String maxStart(String a, String b) {
        if (a.isEmpty()) {
            return b;
        }
        if (b.isEmpty()) {
            return a;
        }
        return a.compareTo(b) >= 0 ? a : b;
    }

    /** end 取较小者（空表示表尾，视为最大）。 */
    private static String minEnd(String a, String b) {
        if (a.isEmpty()) {
            return b;
        }
        if (b.isEmpty()) {
            return a;
        }
        return a.compareTo(b) <= 0 ? a : b;
    }

    /** {@code [start, end)} 是否非空；end 为空表示直到表尾，只要区间不反向即合法。 */
    private static boolean isValidRange(String start, String end) {
        if (end.isEmpty()) {
            return true;
        }
        if (start.isEmpty()) {
            return true;
        }
        return start.compareTo(end) < 0;
    }

    private void assignSplit(int taskId) {
        List<BigtableSourceSplit> toAssign = new ArrayList<>();
        if (context.currentParallelism() == 1) {
            toAssign.addAll(pendingSplits);
        } else {
            for (BigtableSourceSplit split : pendingSplits) {
                int owner =
                        (split.splitId().hashCode() & Integer.MAX_VALUE)
                                % context.currentParallelism();
                if (owner == taskId) {
                    toAssign.add(split);
                }
            }
        }
        context.assignSplit(taskId, toAssign);
        assignedSplits.addAll(toAssign);
        toAssign.forEach(pendingSplits::remove);
        log.info(
                "SubTask {} assigned [{}]",
                taskId,
                toAssign.stream()
                        .map(BigtableSourceSplit::splitId)
                        .collect(Collectors.joining(",")));
        context.signalNoMoreSplits(taskId);
    }

    private BigtableClient getBigtableClient() {
        if (bigtableClient == null) {
            bigtableClient = BigtableClient.createInstance(parameters);
        }
        return bigtableClient;
    }
}
