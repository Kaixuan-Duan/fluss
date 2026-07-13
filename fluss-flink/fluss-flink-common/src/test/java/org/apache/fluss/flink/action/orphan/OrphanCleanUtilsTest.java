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

package org.apache.fluss.flink.action.orphan;

import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.PartitionInfo;
import org.apache.fluss.metadata.ResolvedPartitionSpec;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.types.DataTypes;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;

import static org.apache.fluss.record.TestData.DEFAULT_REMOTE_DATA_DIR;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link OrphanCleanUtils#enumerateBuckets(TableInfo, PartitionInfo)}. Verify the
 * per-partition bucket count (bucket.num.actual) is respected and the table-level count is used
 * only as a fallback.
 */
class OrphanCleanUtilsTest {

    private static final long TABLE_ID = 42L;
    private static final TablePath TABLE_PATH = TablePath.of("db", "t");

    @Test
    void enumerateBucketsNonPartitionedUsesTableLevelCount() {
        TableInfo tableInfo = tableInfo(4, false);
        List<TableBucket> buckets = OrphanCleanUtils.enumerateBuckets(tableInfo, null);
        assertThat(buckets).hasSize(4);
        for (int b = 0; b < 4; b++) {
            assertThat(buckets.get(b)).isEqualTo(new TableBucket(TABLE_ID, b));
        }
    }

    @Test
    void enumerateBucketsPartitionedUsesPerPartitionCount() {
        // Table-level count is 8 (post-ALTER) but the partition was created before the ALTER at 4.
        // enumerateBuckets MUST use 4, not 8, otherwise the cleaner would touch bucket paths 4-7
        // that do not exist for this partition.
        TableInfo tableInfo = tableInfo(8, true);
        PartitionInfo partitionInfo = partition(100L, "old", 4);
        List<TableBucket> buckets = OrphanCleanUtils.enumerateBuckets(tableInfo, partitionInfo);
        assertThat(buckets).hasSize(4);
        for (int b = 0; b < 4; b++) {
            assertThat(buckets.get(b)).isEqualTo(new TableBucket(TABLE_ID, 100L, b));
        }
    }

    @Test
    void enumerateBucketsPartitionedNewUsesPerPartitionCount() {
        // Partition created after ALTER at bucketCount = table-level, both are 8.
        TableInfo tableInfo = tableInfo(8, true);
        PartitionInfo partitionInfo = partition(300L, "new", 8);
        List<TableBucket> buckets = OrphanCleanUtils.enumerateBuckets(tableInfo, partitionInfo);
        assertThat(buckets).hasSize(8);
        assertThat(buckets.get(0)).isEqualTo(new TableBucket(TABLE_ID, 300L, 0));
        assertThat(buckets.get(7)).isEqualTo(new TableBucket(TABLE_ID, 300L, 7));
    }

    private static TableInfo tableInfo(int numBuckets, boolean isPartitioned) {
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("value", DataTypes.STRING())
                        .primaryKey("id")
                        .build();
        List<String> partitionKeys =
                isPartitioned ? Collections.singletonList("pt") : Collections.emptyList();
        return new TableInfo(
                TABLE_PATH,
                TABLE_ID,
                0,
                schema,
                Collections.emptyList(),
                partitionKeys,
                numBuckets,
                new Configuration(),
                new Configuration(),
                DEFAULT_REMOTE_DATA_DIR,
                null,
                System.currentTimeMillis(),
                System.currentTimeMillis());
    }

    private static PartitionInfo partition(long partitionId, String name, int bucketCount) {
        ResolvedPartitionSpec spec = ResolvedPartitionSpec.fromPartitionValue("pt", name);
        return new PartitionInfo(partitionId, spec, DEFAULT_REMOTE_DATA_DIR, bucketCount);
    }
}
