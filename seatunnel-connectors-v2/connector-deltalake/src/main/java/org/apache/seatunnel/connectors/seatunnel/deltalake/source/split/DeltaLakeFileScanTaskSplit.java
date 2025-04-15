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

package org.apache.seatunnel.connectors.seatunnel.deltalake.source.split;

import lombok.AllArgsConstructor;
import lombok.Getter;
import io.delta.kernel.expressions.Predicate;
import lombok.Setter;
import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.api.table.catalog.TablePath;

import java.util.Objects;

@Getter
@AllArgsConstructor
public class DeltaLakeFileScanTaskSplit implements SourceSplit {

  private static final long serialVersionUID = -9043797960947110643L;

  private final TablePath tablePath;

  private final String filePath;
  private final long fileSize;
  private final long start;
  private final long length;

  // Optional predicate (residual filters)
  private final Predicate residualPredicate;

  @Setter
  private volatile long recordOffset;

  public DeltaLakeFileScanTaskSplit(
          TablePath tablePath,
          String filePath,
          long fileSize,
          long start,
          long length,
          Predicate residualPredicate) {
    this.tablePath = tablePath;
    this.filePath = filePath;
    this.fileSize = fileSize;
    this.start = start;
    this.length = length;
    this.residualPredicate = residualPredicate;
    this.recordOffset = 0;
  }

  @Override
  public String splitId() {
    return filePath + "_" + start + "_" + length;
  }

  @Override
  public String toString() {
    return "DeltalakeFileScanTaskSplit{" +
            "filePath='" + filePath + '\'' +
            ", start=" + start +
            ", length=" + length +
            ", recordOffset=" + recordOffset +
            ", residual=" + residualPredicate +
            '}';
  }

  @Override
  public int hashCode() {
    return Objects.hash(filePath, start, length);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (!(obj instanceof DeltaLakeFileScanTaskSplit)) return false;
    DeltaLakeFileScanTaskSplit other = (DeltaLakeFileScanTaskSplit) obj;
    return Objects.equals(filePath, other.filePath)
            && start == other.start
            && length == other.length;
  }
}
