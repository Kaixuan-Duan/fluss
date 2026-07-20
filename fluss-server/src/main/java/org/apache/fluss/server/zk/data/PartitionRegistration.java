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

package org.apache.fluss.server.zk.data;

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.metadata.TablePartition;

import javax.annotation.Nullable;

import java.util.Objects;

/**
 * The registration information of partition in {@link ZkData.PartitionZNode}. It is used to store
 * the partition information in zookeeper.
 *
 * @see PartitionRegistrationJsonSerde for json serialization and deserialization.
 */
public class PartitionRegistration {

    private final long tableId;
    private final long partitionId;

    /**
     * The remote data directory of the partition. It is null if and only if it is deserialized by
     * {@link PartitionRegistrationJsonSerde} from an existing node produced by an older version
     * that does not support multiple remote paths. But immediately after that, we will set it as
     * the default remote file path configured by {@link ConfigOptions#REMOTE_DATA_DIR} (see {@link
     * org.apache.fluss.server.zk.ZooKeeperClient#getPartition}). This unifies subsequent usage and
     * eliminates the need to account for differences between versions.
     */
    private final @Nullable String remoteDataDir;

    /**
     * The physical partition name used to name the data directories ({@code
     * {physicalName}-p{partitionId}}) on tablet servers and remote storage. It is null for
     * partitions whose physical name equals the logical (znode) name, which is the common case. The
     * INSERT OVERWRITE new-partition PoC sets it explicitly so a shadow partition registered under
     * an internal logical name stores data under the origin partition's name. Once set at creation
     * time it is immutable and travels with the partition id across swaps.
     */
    private final @Nullable String physicalPartitionName;

    public PartitionRegistration(long tableId, long partitionId, @Nullable String remoteDataDir) {
        this(tableId, partitionId, remoteDataDir, null);
    }

    public PartitionRegistration(
            long tableId,
            long partitionId,
            @Nullable String remoteDataDir,
            @Nullable String physicalPartitionName) {
        this.tableId = tableId;
        this.partitionId = partitionId;
        this.remoteDataDir = remoteDataDir;
        this.physicalPartitionName = physicalPartitionName;
    }

    public long getTableId() {
        return tableId;
    }

    public long getPartitionId() {
        return partitionId;
    }

    @Nullable
    public String getRemoteDataDir() {
        return remoteDataDir;
    }

    @Nullable
    public String getPhysicalPartitionName() {
        return physicalPartitionName;
    }

    public TablePartition toTablePartition() {
        return new TablePartition(tableId, partitionId);
    }

    /**
     * Returns a new registration with the given remote data directory. Should only be called by
     * {@link org.apache.fluss.server.zk.ZooKeeperClient#getPartition} when deserialize an old
     * PartitionRegistration node without remote data dir configured.
     *
     * @param remoteDataDir the remote data directory
     * @return a new registration with the given remote data directory
     */
    public PartitionRegistration newRemoteDataDir(String remoteDataDir) {
        return new PartitionRegistration(
                tableId, partitionId, remoteDataDir, physicalPartitionName);
    }

    /**
     * Returns a new registration with the given physical partition name. Used by partition swap to
     * materialize the default physical name (= the znode name at creation time) into an explicit
     * value before the registration is moved under another znode.
     *
     * @param physicalPartitionName the physical partition name
     * @return a new registration with the given physical partition name
     */
    public PartitionRegistration newPhysicalPartitionName(String physicalPartitionName) {
        return new PartitionRegistration(
                tableId, partitionId, remoteDataDir, physicalPartitionName);
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PartitionRegistration that = (PartitionRegistration) o;
        return tableId == that.tableId
                && partitionId == that.partitionId
                && Objects.equals(remoteDataDir, that.remoteDataDir)
                && Objects.equals(physicalPartitionName, that.physicalPartitionName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tableId, partitionId, remoteDataDir, physicalPartitionName);
    }

    @Override
    public String toString() {
        return "PartitionRegistration{"
                + "tableId="
                + tableId
                + ", partitionId="
                + partitionId
                + ", remoteDataDir='"
                + remoteDataDir
                + '\''
                + ", physicalPartitionName='"
                + physicalPartitionName
                + '\''
                + '}';
    }
}
