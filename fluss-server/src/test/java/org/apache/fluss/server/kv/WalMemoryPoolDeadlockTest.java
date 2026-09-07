/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.server.kv;

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.config.MemorySize;
import org.apache.fluss.config.TableConfig;
import org.apache.fluss.exception.MemoryPoolTimeoutException;
import org.apache.fluss.exception.WalRecordBatchTooLargeException;
import org.apache.fluss.memory.LazyMemorySegmentPool;
import org.apache.fluss.memory.MemorySegment;
import org.apache.fluss.metadata.KvFormat;
import org.apache.fluss.metadata.LogFormat;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.SchemaGetter;
import org.apache.fluss.metadata.SchemaInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.record.KvRecord;
import org.apache.fluss.record.KvRecordBatch;
import org.apache.fluss.record.KvRecordTestUtils;
import org.apache.fluss.record.TestingSchemaGetter;
import org.apache.fluss.server.kv.autoinc.AutoIncrementManager;
import org.apache.fluss.server.kv.autoinc.TestingSequenceGeneratorFactory;
import org.apache.fluss.server.kv.rowmerger.RowMerger;
import org.apache.fluss.server.log.LogAppendInfo;
import org.apache.fluss.server.log.LogTablet;
import org.apache.fluss.server.log.LogTestUtils;
import org.apache.fluss.server.metrics.group.TestingMetricGroups;
import org.apache.fluss.shaded.arrow.org.apache.arrow.memory.RootAllocator;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.utils.IOUtils;
import org.apache.fluss.utils.clock.SystemClock;
import org.apache.fluss.utils.concurrent.FlussScheduler;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.fluss.compression.ArrowCompressionInfo.DEFAULT_COMPRESSION;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression tests for the WAL memory-pool deadlock: a primary-key write batch whose generated WAL
 * exceeds the node-wide shared buffer pool used to block the request thread forever inside {@code
 * LazyMemorySegmentPool.waitForSegment()} while holding the kv write lock.
 *
 * <p>Mirroring a real tablet server (one {@code LazyMemorySegmentPool} shared by all tablets of the
 * node, created in {@code KvManager}), the tests create kv tablets sharing one real pool of 1MB (a
 * shrunken version of the default 256MB) and verify the fixed behavior:
 *
 * <ol>
 *   <li>a batch whose changelog (~6MB) exceeds the pool fails fast with a non-retryable {@link
 *       WalRecordBatchTooLargeException} carrying remediation hints, instead of deadlocking; the
 *       pool pages and the write lock are released, so subsequent writes to both tables succeed;
 *   <li>a batch that holds only part of the pool (the rest is held by other batches) times out with
 *       a retriable {@link MemoryPoolTimeoutException} and succeeds once the pool is freed.
 * </ol>
 */
class WalMemoryPoolDeadlockTest {

    private static final short SCHEMA_ID = 1;

    /** Random-letter payload per record; random letters keep compression from shrinking it much. */
    private static final int PAYLOAD_SIZE = 100 * 1024;

    /** 60 records x 100KB gives a ~6MB changelog, far beyond the 1MB pool. */
    private static final int LARGE_BATCH_RECORDS = 60;

    private static final Schema SCHEMA =
            Schema.newBuilder()
                    .column("id", DataTypes.INT())
                    .column("payload", DataTypes.STRING())
                    .primaryKey("id")
                    .build();

    private final Configuration conf = new Configuration();
    private final KvRecordTestUtils.KvRecordBatchFactory kvRecordBatchFactory =
            KvRecordTestUtils.KvRecordBatchFactory.of(SCHEMA_ID);
    private final KvRecordTestUtils.KvRecordFactory kvRecordFactory =
            KvRecordTestUtils.KvRecordFactory.of(SCHEMA.getRowType());

    private @TempDir File tempLogDir;
    private @TempDir File kvDirA;
    private @TempDir File kvDirB;

    private LazyMemorySegmentPool memoryPool;
    private final List<LogTablet> logTablets = new ArrayList<>();
    private final List<KvTablet> kvTablets = new ArrayList<>();

    @BeforeEach
    void beforeEach() {
        // A shrunken version of the default server pool (256MB / 128KB pages): 1MB / 64KB pages.
        conf.set(ConfigOptions.SERVER_BUFFER_MEMORY_SIZE, MemorySize.parse("1mb"));
        conf.set(ConfigOptions.SERVER_BUFFER_PAGE_SIZE, MemorySize.parse("64kb"));
        conf.set(ConfigOptions.SERVER_BUFFER_PER_REQUEST_MEMORY_SIZE, MemorySize.parse("64kb"));
        memoryPool = LazyMemorySegmentPool.createServerBufferPool(conf);
    }

    @AfterEach
    void afterEach() throws Exception {
        IOUtils.closeAll(
                () -> {
                    for (KvTablet kvTablet : kvTablets) {
                        kvTablet.close();
                    }
                },
                () -> {
                    for (LogTablet logTablet : logTablets) {
                        logTablet.close();
                    }
                });
    }

    @Test
    void testWalBatchLargerThanPoolFailsFastInsteadOfDeadlocking() throws Exception {
        KvTablet kvTabletA =
                createSharedPoolKvTablet(
                        PhysicalTablePath.of(TablePath.of("testDb", "tA")), 0L, kvDirA);
        KvTablet kvTabletB =
                createSharedPoolKvTablet(
                        PhysicalTablePath.of(TablePath.of("testDb", "tB")), 1L, kvDirB);

        KvRecordBatch largeBatch = newLargeBatch();
        int totalPoolPages = memoryPool.totalPages();

        // The large write used to deadlock forever holding the kv write lock; it now fails fast.
        long startMs = System.currentTimeMillis();
        assertThatThrownBy(() -> kvTabletA.putAsLeader(largeBatch, null))
                .isInstanceOf(WalRecordBatchTooLargeException.class)
                .hasMessageContaining("client.writer.batch-size")
                .hasMessageContaining("server.buffer.memory-size");
        long elapsedMs = System.currentTimeMillis() - startMs;

        // fast-fail: no waiting for the pool wait timeout
        assertThat(elapsedMs).as("the oversized batch should fail fast").isLessThan(2000);

        // all pool pages held by the failed batch are returned, and the kv write lock is
        // released, so both the same table and another table sharing the pool accept writes
        assertThat(memoryPool.freePages()).isEqualTo(totalPoolPages);
        assertThat(
                        kvTabletA.putAsLeader(
                                kvRecordBatchFactory.ofRecords(
                                        kvRecordFactory.ofRecord(
                                                "small-key".getBytes(),
                                                new Object[] {1, "small-value"})),
                                null))
                .isNotNull();
        assertThat(
                        kvTabletB.putAsLeader(
                                kvRecordBatchFactory.ofRecords(
                                        kvRecordFactory.ofRecord(
                                                "small-key".getBytes(),
                                                new Object[] {1, "small-value"})),
                                null))
                .isNotNull();
    }

    @Test
    void testPoolExhaustedByOtherBatchesTimesOutAsRetriable() throws Exception {
        // a short wait timeout so the test does not have to wait for the 60s default
        conf.set(ConfigOptions.SERVER_BUFFER_POOL_WAIT_TIMEOUT, Duration.ofMillis(500));
        memoryPool = LazyMemorySegmentPool.createServerBufferPool(conf);

        KvTablet kvTablet =
                createSharedPoolKvTablet(
                        PhysicalTablePath.of(TablePath.of("testDb", "tA")), 0L, kvDirA);
        KvRecordBatch smallBatch =
                kvRecordBatchFactory.ofRecords(
                        kvRecordFactory.ofRecord(
                                "small-key".getBytes(), new Object[] {1, "small-value"}));

        // simulate other in-flight batches holding every page of the pool: the new write holds
        // no page itself, so it must time out with a retriable error instead of failing fast
        List<MemorySegment> takenPages = memoryPool.allocatePages(memoryPool.totalPages());
        assertThatThrownBy(() -> kvTablet.putAsLeader(smallBatch, null))
                .isInstanceOf(MemoryPoolTimeoutException.class)
                .hasMessageContaining("Timed out waiting for memory");

        // once the other batches return the pages, the same write succeeds
        memoryPool.returnAll(takenPages);
        LogAppendInfo appendInfo = kvTablet.putAsLeader(smallBatch, null);
        assertThat(appendInfo).isNotNull();
        assertThat(memoryPool.freePages()).isEqualTo(memoryPool.totalPages());
    }

    // ----------------------- helpers -----------------------

    /** Creates a kv tablet backed by the shared real memory pool, like KvManager does. */
    private KvTablet createSharedPoolKvTablet(
            PhysicalTablePath physicalPath, long tableId, File kvDir) throws Exception {
        SchemaGetter schemaGetter = new TestingSchemaGetter(new SchemaInfo(SCHEMA, SCHEMA_ID));
        LogTablet logTablet = createLogTablet(tempLogDir, tableId, physicalPath);
        logTablets.add(logTablet);
        TableConfig tableConf = new TableConfig(new Configuration());
        RowMerger rowMerger = RowMerger.create(tableConf, KvFormat.COMPACTED, schemaGetter);
        AutoIncrementManager autoIncrementManager =
                new AutoIncrementManager(
                        schemaGetter,
                        physicalPath.getTablePath(),
                        new TableConfig(new Configuration()),
                        new TestingSequenceGeneratorFactory());
        KvTablet kvTablet =
                KvTablet.create(
                        physicalPath,
                        logTablet.getTableBucket(),
                        logTablet,
                        kvDir,
                        conf,
                        TestingMetricGroups.TABLET_SERVER_METRICS,
                        new RootAllocator(Long.MAX_VALUE),
                        memoryPool,
                        KvFormat.COMPACTED,
                        rowMerger,
                        DEFAULT_COMPRESSION,
                        schemaGetter,
                        tableConf.getChangelogImage(),
                        KvManager.getDefaultRateLimiter(),
                        autoIncrementManager,
                        SystemClock.getInstance(),
                        tableConf);
        kvTablets.add(kvTablet);
        return kvTablet;
    }

    private LogTablet createLogTablet(File tempLogDir, long tableId, PhysicalTablePath tablePath)
            throws Exception {
        File logTabletDir =
                LogTestUtils.makeRandomLogTabletDir(
                        tempLogDir, tablePath.getDatabaseName(), tableId, tablePath.getTableName());
        return LogTablet.create(
                tempLogDir,
                tablePath,
                logTabletDir,
                conf,
                new AtomicBoolean(
                        conf.get(ConfigOptions.LOG_RETENTION_ROLL_ACTIVE_SEGMENT_ENABLED)),
                TestingMetricGroups.TABLET_SERVER_METRICS,
                0,
                new FlussScheduler(1),
                LogFormat.ARROW,
                1,
                true,
                SystemClock.getInstance(),
                true);
    }

    private KvRecordBatch newLargeBatch() throws Exception {
        Random rnd = new Random(42);
        List<KvRecord> records = new ArrayList<>();
        for (int i = 0; i < LARGE_BATCH_RECORDS; i++) {
            records.add(
                    kvRecordFactory.ofRecord(
                            ("large-" + i).getBytes(), new Object[] {i, randomPayload(rnd)}));
        }
        return kvRecordBatchFactory.ofRecords(records);
    }

    private static String randomPayload(Random rnd) {
        StringBuilder sb = new StringBuilder(PAYLOAD_SIZE);
        for (int i = 0; i < PAYLOAD_SIZE; i++) {
            sb.append((char) ('a' + rnd.nextInt(26)));
        }
        return sb.toString();
    }
}
