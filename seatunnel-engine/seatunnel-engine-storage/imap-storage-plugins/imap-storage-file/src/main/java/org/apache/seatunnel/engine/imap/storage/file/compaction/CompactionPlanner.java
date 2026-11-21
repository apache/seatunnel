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

import java.util.Optional;

/**
 * Selects exclusively owned immutable inputs; future selectors may span more than one region.
 *
 * <p>A multi-scope selector must coordinate ownership and invalidate every affected replay scope
 * before retiring its inputs. Moving output elsewhere alone does not establish a safe cutover.
 */
public interface CompactionPlanner {
    Optional<CompactionPlan> plan();

    /** Replaces exactly the selected inputs, retaining files sealed while compaction ran. */
    void committed(CompactionPlan plan, Path snapshot);
}
