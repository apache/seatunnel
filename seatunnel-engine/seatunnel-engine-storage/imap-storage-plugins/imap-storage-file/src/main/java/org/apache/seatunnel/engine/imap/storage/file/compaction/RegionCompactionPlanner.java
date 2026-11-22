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

package org.apache.seatunnel.engine.imap.storage.file.compaction;

import org.apache.hadoop.fs.Path;

import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/** Selects the sealed prefix of one writer's region, including its previous snapshot. */
@Slf4j
public final class RegionCompactionPlanner implements CompactionPlanner {
    private final Path directory;
    private final Set<Path> sealedFiles = new LinkedHashSet<>();
    private Path snapshot;
    private boolean failedSeal;

    public RegionCompactionPlanner(Path directory) {
        this.directory = directory;
    }

    /** Called only after the writer has successfully closed the segment. */
    public synchronized void sealed(Path path) {
        if (!directory.equals(path.getParent())) {
            throw new IllegalArgumentException(
                    "Sealed WAL is outside the writer's region: " + path);
        }
        if (!failedSeal) {
            sealedFiles.add(path);
        }
    }

    /**
     * Keeps an uncertain segment and every later segment outside compaction. Selecting beyond it
     * would move later mutations before the failed file when the snapshot replaces the prefix.
     */
    public synchronized void failedSeal(Path path) {
        if (!directory.equals(path.getParent())) {
            throw new IllegalArgumentException(
                    "Failed WAL is outside the writer's region: " + path);
        }
        if (!failedSeal) {
            failedSeal = true;
            sealedFiles.clear();
            log.warn(
                    "Paused IMap WAL compaction for region {} after a write or seal failure in {}. "
                            + "Existing files are retained and later writes use new segments.",
                    directory,
                    path);
        }
    }

    @Override
    public synchronized Optional<CompactionPlan> plan() {
        if (failedSeal || sealedFiles.isEmpty()) {
            return Optional.empty();
        }
        List<Path> inputs = new ArrayList<>();
        if (snapshot != null) {
            inputs.add(snapshot);
        }
        inputs.addAll(sealedFiles);
        return Optional.of(new CompactionPlan(inputs, directory));
    }

    @Override
    public synchronized void committed(CompactionPlan plan, Path snapshot) {
        sealedFiles.removeAll(plan.getInputs());
        this.snapshot = snapshot;
    }
}
