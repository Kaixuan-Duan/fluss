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

package org.apache.fluss.server.coordinator.event;

import org.apache.fluss.metadata.TablePath;

import java.util.Objects;

/**
 * An event for the registration data change of an existing partition znode, i.e. the partition name
 * has been repointed to another partition id by a partition swap (INSERT OVERWRITE new-partition
 * PoC). The coordinator refreshes its name to id mappings and pushes the new partition metadata to
 * tablet servers on this event.
 */
public class PartitionRegistrationChangeEvent implements CoordinatorEvent {

    private final TablePath tablePath;
    private final long tableId;
    private final String partitionName;
    private final long partitionId;

    public PartitionRegistrationChangeEvent(
            TablePath tablePath, long tableId, long partitionId, String partitionName) {
        this.tablePath = tablePath;
        this.tableId = tableId;
        this.partitionId = partitionId;
        this.partitionName = partitionName;
    }

    public TablePath getTablePath() {
        return tablePath;
    }

    public long getTableId() {
        return tableId;
    }

    public long getPartitionId() {
        return partitionId;
    }

    public String getPartitionName() {
        return partitionName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PartitionRegistrationChangeEvent that = (PartitionRegistrationChangeEvent) o;
        return tableId == that.tableId
                && partitionId == that.partitionId
                && Objects.equals(tablePath, that.tablePath)
                && Objects.equals(partitionName, that.partitionName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tablePath, tableId, partitionName, partitionId);
    }

    @Override
    public String toString() {
        return "PartitionRegistrationChangeEvent{"
                + "tablePath="
                + tablePath
                + ", tableId="
                + tableId
                + ", partitionName='"
                + partitionName
                + '\''
                + ", partitionId="
                + partitionId
                + '}';
    }
}
