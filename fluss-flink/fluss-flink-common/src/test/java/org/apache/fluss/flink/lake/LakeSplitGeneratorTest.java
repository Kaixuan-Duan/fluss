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

package org.apache.fluss.flink.lake;

import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.client.initializer.OffsetsInitializer;
import org.apache.fluss.client.metadata.LakeSnapshot;
import org.apache.fluss.flink.lake.split.LakeSnapshotAndFlussLogSplit;
import org.apache.fluss.flink.source.split.SourceSplitBase;
import org.apache.fluss.lake.source.LakeSource;
import org.apache.fluss.lake.source.LakeSplit;
import org.apache.fluss.lake.source.Planner;
import org.apache.fluss.metadata.PartitionInfo;
import org.apache.fluss.metadata.ResolvedPartitionSpec;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.types.DataTypes;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit test for the fail-loud guard in {@link LakeSplitGenerator}: for a primary-key table, if the
 * lake snapshot of a partition contains a bucket id outside the partition's enumerated bucket range
 * (which can only happen if the per-partition bucket count is inconsistent with the tiered data),
 * union-read split generation must refuse rather than silently drop the out-of-range lake data.
 */
class LakeSplitGeneratorTest {

    @Test
    @SuppressWarnings("unchecked")
    void testPrimaryKeyOutOfRangeLakeBucketFailsLoud() throws Exception {
        TablePath tablePath = TablePath.of("db", "pk_rescale");
        // partitioned primary-key table; table-level bucket count is 2
        int partitionBucketCount = 2;
        TableDescriptor descriptor =
                TableDescriptor.builder()
                        .schema(
                                Schema.newBuilder()
                                        .column("a", DataTypes.INT())
                                        .column("b", DataTypes.STRING())
                                        .column("c", DataTypes.STRING())
                                        .primaryKey("a", "c")
                                        .build())
                        .distributedBy(partitionBucketCount, "a")
                        .partitionedBy("c")
                        .build();
        TableInfo tableInfo = TableInfo.of(tablePath, 1L, 1, descriptor, null, 1L, 1L);

        // a lake split for partition "p" landing in bucket 5, which is outside [0, 2)
        LakeSplit outOfRangeSplit = mock(LakeSplit.class);
        when(outOfRangeSplit.partition()).thenReturn(Collections.singletonList("p"));
        when(outOfRangeSplit.bucket()).thenReturn(5);

        Planner<LakeSplit> planner = mock(Planner.class);
        when(planner.plan()).thenReturn(Collections.singletonList(outOfRangeSplit));
        LakeSource<LakeSplit> lakeSource = mock(LakeSource.class);
        when(lakeSource.createPlanner(any())).thenReturn(planner);

        Admin admin = mock(Admin.class);
        when(admin.getReadableLakeSnapshot(tablePath))
                .thenReturn(
                        CompletableFuture.completedFuture(new LakeSnapshot(1L, new HashMap<>())));

        OffsetsInitializer.BucketOffsetsRetriever retriever =
                mock(OffsetsInitializer.BucketOffsetsRetriever.class);
        OffsetsInitializer stoppingOffsetInitializer = mock(OffsetsInitializer.class);
        Map<Integer, Long> stoppingOffsets = new HashMap<>();
        stoppingOffsets.put(0, 0L);
        stoppingOffsets.put(1, 0L);
        when(stoppingOffsetInitializer.getBucketOffsets(eq("p"), anyList(), any()))
                .thenReturn(stoppingOffsets);

        // the partition "p" carries its own bucket count of 2 (so bucket 5 is out of range)
        PartitionInfo partitionInfo =
                new PartitionInfo(
                        7L,
                        ResolvedPartitionSpec.fromPartitionName(tableInfo.getPartitionKeys(), "p"),
                        null,
                        partitionBucketCount);
        Set<PartitionInfo> partitions = Collections.singleton(partitionInfo);

        LakeSplitGenerator generator =
                new LakeSplitGenerator(
                        tableInfo,
                        admin,
                        lakeSource,
                        retriever,
                        stoppingOffsetInitializer,
                        partitionBucketCount,
                        () -> partitions);

        assertThatThrownBy(generator::generateHybridLakeFlussSplits)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("outside the enumerated range")
                .hasMessageContaining("refusing to generate union-read splits");
    }

    @Test
    @SuppressWarnings("unchecked")
    void testPrimaryKeyInRangeLakeBucketSucceeds() throws Exception {
        TablePath tablePath = TablePath.of("db", "pk_ok");
        int partitionBucketCount = 4;
        TableDescriptor descriptor =
                TableDescriptor.builder()
                        .schema(
                                Schema.newBuilder()
                                        .column("a", DataTypes.INT())
                                        .column("b", DataTypes.STRING())
                                        .column("c", DataTypes.STRING())
                                        .primaryKey("a", "c")
                                        .build())
                        .distributedBy(partitionBucketCount, "a")
                        .partitionedBy("c")
                        .build();
        TableInfo tableInfo = TableInfo.of(tablePath, 1L, 1, descriptor, null, 1L, 1L);

        // an in-range lake split (bucket 1 within [0, 4))
        LakeSplit inRangeSplit = mock(LakeSplit.class);
        when(inRangeSplit.partition()).thenReturn(Collections.singletonList("p"));
        when(inRangeSplit.bucket()).thenReturn(1);

        Planner<LakeSplit> planner = mock(Planner.class);
        when(planner.plan()).thenReturn(Collections.singletonList(inRangeSplit));
        LakeSource<LakeSplit> lakeSource = mock(LakeSource.class);
        when(lakeSource.createPlanner(any())).thenReturn(planner);

        Admin admin = mock(Admin.class);
        when(admin.getReadableLakeSnapshot(tablePath))
                .thenReturn(
                        CompletableFuture.completedFuture(new LakeSnapshot(1L, new HashMap<>())));

        OffsetsInitializer.BucketOffsetsRetriever retriever =
                mock(OffsetsInitializer.BucketOffsetsRetriever.class);
        OffsetsInitializer stoppingOffsetInitializer = mock(OffsetsInitializer.class);
        Map<Integer, Long> stoppingOffsets = new HashMap<>();
        for (int bucket = 0; bucket < partitionBucketCount; bucket++) {
            stoppingOffsets.put(bucket, 0L);
        }
        when(stoppingOffsetInitializer.getBucketOffsets(eq("p"), anyList(), any()))
                .thenReturn(stoppingOffsets);

        PartitionInfo partitionInfo =
                new PartitionInfo(
                        7L,
                        ResolvedPartitionSpec.fromPartitionName(tableInfo.getPartitionKeys(), "p"),
                        null,
                        partitionBucketCount);

        LakeSplitGenerator generator =
                new LakeSplitGenerator(
                        tableInfo,
                        admin,
                        lakeSource,
                        retriever,
                        stoppingOffsetInitializer,
                        partitionBucketCount,
                        () -> Collections.singleton(partitionInfo));

        // no out-of-range bucket: generation succeeds and produces one hybrid lake+log split per
        // bucket of the partition's enumerated range [0, partitionBucketCount)
        List<SourceSplitBase> splits = generator.generateHybridLakeFlussSplits();
        assertThat(splits).isNotNull().hasSize(partitionBucketCount);
        for (int bucket = 0; bucket < partitionBucketCount; bucket++) {
            SourceSplitBase split = splits.get(bucket);
            assertThat(split).isInstanceOf(LakeSnapshotAndFlussLogSplit.class);
            assertThat(split.getPartitionName()).isEqualTo("p");
            assertThat(split.getTableBucket().getPartitionId()).isEqualTo(7L);
            assertThat(split.getTableBucket().getBucket()).isEqualTo(bucket);
        }
    }
}
