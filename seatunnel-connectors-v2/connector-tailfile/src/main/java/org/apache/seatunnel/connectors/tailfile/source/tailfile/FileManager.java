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

package org.apache.seatunnel.connectors.tailfile.source.tailfile;

import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.tailfile.source.TailFileSourceConfig;
import org.apache.seatunnel.connectors.tailfile.source.TailFileSourceSplit;

import lombok.extern.slf4j.Slf4j;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Consumer;
import java.util.stream.Collectors;

@Slf4j
public class FileManager implements Closeable {
    private final TailFileSourceConfig config;
    private final ConcurrentMap<Long, FileHarvester> fileRegistry;

    public FileManager(TailFileSourceConfig config) {
        this.config = config;
        this.fileRegistry = new ConcurrentHashMap<>();
    }

    public boolean isEmpty() {
        return fileRegistry.isEmpty();
    }

    public synchronized void register(List<TailFileSourceSplit> splits) {
        for (TailFileSourceSplit split : splits) {
            try {
                register(split);
            } catch (IOException e) {
                log.warn("Skip register TailFile {} due to exception", split, e);
            }
        }
    }

    private FileHarvester register(TailFileSourceSplit split) throws IOException {
        FileHarvester fileHarvester = fileRegistry.get(split.getInode());
        if (fileHarvester == null) {
            File file = new File(split.getFilepath());
            long startPos =
                    split.getPos() == 0 && config.isSkipToEnd() ? file.length() : split.getPos();

            log.info("Opening file: {}, inode: {}, startPos: {}", file, split.getInode(), startPos);
            fileHarvester = new FileHarvester(file, split.getInode(), startPos, config);
            fileRegistry.put(split.getInode(), fileHarvester);
            return fileHarvester;
        }

        if (split.getFilepath().equals(fileHarvester.getPath())) {
            log.warn("Skip register TailFile {} as it already exists", split);
            return fileHarvester;
        }

        log.info(
                "File rename detected, old: {}, new: {}",
                fileHarvester.getPath(),
                split.getFilepath());
        if (null != fileHarvester.getRaf()) {
            fileHarvester.close();
        }

        File file = new File(split.getFilepath());
        fileHarvester =
                new FileHarvester(file, split.getInode(), fileHarvester.getFileStartPos(), config);
        log.info(
                "Reopening file: {}, inode: {}, startPos: {}",
                file,
                split.getInode(),
                fileHarvester.getFileStartPos());

        boolean updated =
                fileHarvester.getLastUpdated() > 0
                        && fileHarvester.getLastUpdated() < file.lastModified();
        boolean moreToRead = updated || (file.length() > fileHarvester.getFileStartPos());
        if (moreToRead) {
            if (file.length() < fileHarvester.getFileStartPos()) {
                log.warn(
                        "File {} current size {} is smaller than the last read position {}!",
                        split.getFilepath(),
                        file.length(),
                        fileHarvester.getFileStartPos());
                log.warn(
                        "Restarting from pos 0, file: {}, inode: {}",
                        split.getFilepath(),
                        split.getInode());
                fileHarvester.resetPos(fileHarvester.getPath(), split.getInode(), 0);
            }
        }
        fileHarvester.setNeedTail(moreToRead);

        fileRegistry.put(split.getInode(), fileHarvester);
        return fileHarvester;
    }

    public synchronized List<TailFileSourceSplit> snapshot() {
        long now = System.currentTimeMillis();
        List<TailFileSourceSplit> splits = new ArrayList<>(fileRegistry.size());
        for (Long inode : fileRegistry.keySet()) {
            FileHarvester fileHarvester = fileRegistry.get(inode);
            if (now - fileHarvester.getLastUpdated() >= config.getIdleTimeout()
                    && fileHarvester.getRaf() != null) {
                if (!fileHarvester.isNeedTail()) {
                    fileHarvester.close();
                    log.info(
                            "Closed file: "
                                    + fileHarvester.getPath()
                                    + ", inode: "
                                    + inode
                                    + ", pos: "
                                    + fileHarvester.getFileStartPos());
                }
            }
            if (now - fileHarvester.getLastUpdated() >= config.getIgnoreOlder()
                    && !fileHarvester.isNeedTail()) {
                log.info(
                        "Ignore file: "
                                + fileHarvester.getPath()
                                + ", inode: "
                                + inode
                                + ", pos: "
                                + fileHarvester.getFileStartPos());
                if (fileHarvester.getRaf() != null) {
                    fileHarvester.close();
                }
                fileRegistry.remove(inode);
            } else {
                splits.add(TailFileSourceSplit.of(fileHarvester));
            }
        }
        return splits;
    }

    public synchronized List<Long> checkNeedTailFiles() {
        List<Map.Entry<Long, Long>> needTailFiles = new ArrayList<>();
        for (Long inode : fileRegistry.keySet()) {
            FileHarvester fileHarvester = fileRegistry.get(inode);

            File file = fileHarvester.getFile();
            if (!file.exists()) {
                if (fileHarvester.isNeedTail()) {
                    log.warn(
                            "File {} not exists, inode: {}, pos: {}",
                            fileHarvester.getPath(),
                            fileHarvester.getInode(),
                            fileHarvester.getFileStartPos());
                    fileHarvester.setNeedTail(false);
                }
                continue;
            }

            if (fileHarvester.isNeedTail()) {
                needTailFiles.add(
                        new AbstractMap.SimpleEntry<>(
                                inode, fileHarvester.getFile().lastModified()));
                continue;
            }

            try {
                boolean updated =
                        fileHarvester.getLastUpdated() > 0
                                && fileHarvester.getLastUpdated() < file.lastModified();
                boolean moreToRead = updated || (file.length() > fileHarvester.getFileStartPos());
                if (moreToRead) {
                    if (fileHarvester.getRaf() == null) {
                        log.info(
                                "Opening file: "
                                        + file
                                        + ", inode: "
                                        + inode
                                        + ", pos: "
                                        + fileHarvester.getFileStartPos());
                        fileHarvester =
                                new FileHarvester(
                                        file,
                                        fileHarvester.getInode(),
                                        fileHarvester.getFileStartPos(),
                                        config);
                        fileRegistry.put(fileHarvester.getInode(), fileHarvester);
                    }
                    if (file.length() < fileHarvester.getFileStartPos()) {
                        log.warn(
                                "File {} current size {} is smaller than the last read position {}!",
                                fileHarvester.getPath(),
                                file.length(),
                                fileHarvester.getFileStartPos());
                        log.warn(
                                "Restarting from pos 0, file: {}, inode: {}",
                                fileHarvester.getPath(),
                                fileHarvester.getInode());
                        fileHarvester.resetPos(
                                fileHarvester.getPath(), fileHarvester.getInode(), 0);
                    }
                    needTailFiles.add(
                            new AbstractMap.SimpleEntry<>(
                                    inode, fileHarvester.getFile().lastModified()));
                }
                fileHarvester.setNeedTail(moreToRead);
            } catch (IOException e) {
                log.warn(
                        "Skip checkNeedTail TailFile {} due to exception",
                        fileHarvester.getPath(),
                        e);
            }
        }
        // sort by last modified time asc
        needTailFiles.sort(Map.Entry.comparingByValue());
        return needTailFiles.stream().map(Map.Entry::getKey).collect(Collectors.toList());
    }

    public synchronized int tailFile(Long inode, Consumer<SeaTunnelRow> consumer)
            throws IOException {
        FileHarvester fileHarvester = fileRegistry.get(inode);
        if (fileHarvester.isNeedTail()) {
            long updateTime = System.currentTimeMillis();
            int rowCount =
                    fileHarvester.tail(
                            config.getMaxBatchCount(),
                            event -> {
                                SeaTunnelRow row =
                                        new SeaTunnelRow(
                                                new Object[] {
                                                    fileHarvester.getPath(),
                                                    event.getPos(),
                                                    fileHarvester.getInode(),
                                                    System.currentTimeMillis(),
                                                    event.getBody()
                                                });
                                consumer.accept(row);
                            });

            long fileNextStartPos = fileHarvester.getLinePos();
            fileHarvester.setFileStartPos(fileNextStartPos);
            fileHarvester.setLastUpdated(updateTime);
            fileHarvester.setNeedTail(rowCount > 0);
            return rowCount;
        }
        // 读取半行
        // 读取超大行长时间不结束
        // 读取末尾行无换行符
        // 暂停恢复后读取末尾换行符
        // 按照最近更新时间排序
        // 多行日志采集
        // 超大日志行截断
        // 发现新文件
        // 正在读取的文件日志文件内容被清空重写
        // 正在读取的文件被文件重命名
        // 正在读取的文件被移出扫描目录
        // 正在读取的文件被移回扫描目录
        // 正在读取的文件被删除

        // 扫描文件列表
        // 任务暂停重启
        // 淘汰老文件注册表
        return 0;
    }

    @Override
    public void close() {
        log.info("Closing FileManager");
        fileRegistry.forEach((inode, fileHarvester) -> fileHarvester.close());
    }
}
