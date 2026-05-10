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

package org.apache.fluss.client.table.scanner.log;

import org.apache.fluss.client.table.scanner.ScanRecord;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.record.ChangeType;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.apache.fluss.testutils.DataTestUtils.row;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link ScanRecords}. */
public class ScanRecordsTest {
    @Test
    void iterator() {
        Map<TableBucket, List<ScanRecord>> records = new LinkedHashMap<>();
        long tableId = 0;
        records.put(new TableBucket(tableId, 0), new ArrayList<>());
        ScanRecord record1 = new ScanRecord(0L, 1000L, ChangeType.INSERT, row(1, "a"));
        ScanRecord record2 = new ScanRecord(1L, 1000L, ChangeType.UPDATE_BEFORE, row(1, "a"));
        ScanRecord record3 = new ScanRecord(2L, 1000L, ChangeType.UPDATE_AFTER, row(1, "a1"));
        ScanRecord record4 = new ScanRecord(3L, 1000L, ChangeType.DELETE, row(1, "a1"));
        records.put(new TableBucket(tableId, 1), Arrays.asList(record1, record2, record3, record4));
        records.put(new TableBucket(tableId, 2), new ArrayList<>());

        ScanRecords scanRecords = new ScanRecords(records);
        Iterator<ScanRecord> iter = scanRecords.iterator();

        int c = 0;
        for (; iter.hasNext(); c++) {
            ScanRecord record = iter.next();
            assertThat(record.logOffset()).isEqualTo(c);
        }
        assertThat(c).isEqualTo(4);
    }

    /** Verifies the legacy single-arg constructor leaves {@code lastConsumedOffset} undefined. */
    @Test
    void legacyConstructorHasNoLastConsumedOffset() {
        TableBucket tb = new TableBucket(0L, 0);
        Map<TableBucket, List<ScanRecord>> records = new HashMap<>();
        records.put(tb, Collections.emptyList());

        ScanRecords scanRecords = new ScanRecords(records);

        assertThat(scanRecords.buckets()).containsExactly(tb);
        assertThat(scanRecords.lastConsumedOffset(tb)).isNull();
    }

    /**
     * Verifies buckets with only advanced offsets are still exposed via {@link
     * ScanRecords#buckets()} and carry their {@code lastConsumedOffset}.
     */
    @Test
    void emptyBucketIsExposedViaBuckets() {
        TableBucket bucketWithRecords = new TableBucket(0L, 0);
        TableBucket emptyBucket = new TableBucket(0L, 1);

        Map<TableBucket, List<ScanRecord>> records = new HashMap<>();
        records.put(
                bucketWithRecords,
                Collections.singletonList(
                        new ScanRecord(5L, 1000L, ChangeType.INSERT, row(1, "a"))));
        // Empty-progress buckets also appear in records (as emptyList).
        records.put(emptyBucket, Collections.emptyList());

        Map<TableBucket, Long> lastConsumedOffsets = new HashMap<>();
        lastConsumedOffsets.put(bucketWithRecords, 6L);
        lastConsumedOffsets.put(emptyBucket, 10L);

        ScanRecords scanRecords = new ScanRecords(records, lastConsumedOffsets);

        assertThat(scanRecords.buckets()).containsExactlyInAnyOrder(bucketWithRecords, emptyBucket);
        assertThat(scanRecords.records(emptyBucket)).isEmpty();
        assertThat(scanRecords.lastConsumedOffset(bucketWithRecords)).isEqualTo(6L);
        assertThat(scanRecords.lastConsumedOffset(emptyBucket)).isEqualTo(10L);
        assertThat(scanRecords.lastConsumedOffset(new TableBucket(0L, 99))).isNull();
    }

    /**
     * Verifies {@link ScanRecords#isEmpty()} reflects only materialized records, regardless of
     * advanced offsets.
     */
    @Test
    void isEmptyReflectsOnlyMaterializedRecords() {
        TableBucket tb = new TableBucket(0L, 0);

        // No records and no progress: both isEmpty() and buckets() must be empty.
        ScanRecords trulyEmpty = ScanRecords.EMPTY;
        assertThat(trulyEmpty.isEmpty()).isTrue();
        assertThat(trulyEmpty.buckets()).isEmpty();

        // Progress-only round: isEmpty() must stay true (no materialized records),
        // while buckets() must still expose the advanced bucket for callers to detect.
        Map<TableBucket, List<ScanRecord>> emptyRecords = new HashMap<>();
        emptyRecords.put(tb, Collections.emptyList());
        Map<TableBucket, Long> progressOnly = new HashMap<>();
        progressOnly.put(tb, 42L);
        ScanRecords progressOnlyRecords = new ScanRecords(emptyRecords, progressOnly);
        assertThat(progressOnlyRecords.isEmpty()).isTrue();
        assertThat(progressOnlyRecords.buckets()).containsExactly(tb);
        assertThat(progressOnlyRecords.lastConsumedOffset(tb)).isEqualTo(42L);

        // Materialized records present: isEmpty() flips to false; legacy single-arg ctor still
        // works.
        Map<TableBucket, List<ScanRecord>> records = new HashMap<>();
        records.put(
                tb,
                Collections.singletonList(
                        new ScanRecord(0L, 1000L, ChangeType.INSERT, row(1, "a"))));
        ScanRecords withRecords = new ScanRecords(records);
        assertThat(withRecords.isEmpty()).isFalse();
        assertThat(withRecords.buckets()).containsExactly(tb);
    }
}
