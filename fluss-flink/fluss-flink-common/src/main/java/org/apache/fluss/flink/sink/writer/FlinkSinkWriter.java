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

package org.apache.fluss.flink.sink.writer;

import org.apache.fluss.annotation.VisibleForTesting;
import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.writer.TableWriter;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.FlussRuntimeException;
import org.apache.fluss.flink.metrics.FlinkMetricRegistry;
import org.apache.fluss.flink.row.OperationType;
import org.apache.fluss.flink.row.RowWithOp;
import org.apache.fluss.flink.sink.serializer.FlussSerializationSchema;
import org.apache.fluss.flink.sink.serializer.SerializerInitContextImpl;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.metrics.Gauge;
import org.apache.fluss.metrics.Metric;
import org.apache.fluss.metrics.MetricNames;
import org.apache.fluss.row.InternalRow;

import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.types.logical.RowType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;

/** Base class for Flink {@link SinkWriter} implementations in Fluss. */
public abstract class FlinkSinkWriter<InputT> implements SinkWriter<InputT> {

    protected static final Logger LOG = LoggerFactory.getLogger(FlinkSinkWriter.class);

    private final TablePath tablePath;
    private final Configuration flussConfig;
    protected final RowType tableRowType;
    protected final @Nullable int[] targetColumnIndexes;
    private final MailboxExecutor mailboxExecutor;
    private final FlussSerializationSchema<InputT> serializationSchema;

    /**
     * When set, the underlying table writer routes every row to this partition regardless of the
     * row's own partition column value. Used by the INSERT OVERWRITE new-partition PoC.
     */
    protected final @Nullable String fixedPartitionName;

    /**
     * When set (together with {@link #fixedPartitionName}), the writer atomically swaps {@link
     * #fixedPartitionName} (the internal shadow partition holding the new data) with this origin
     * partition name once all input has been written ({@code flush(endOfInput=true)}). Used by the
     * INSERT OVERWRITE new-partition PoC; requires the sink to run with parallelism 1.
     */
    protected final @Nullable String overwriteOriginPartitionName;

    /** Guard to perform the overwrite swap exactly once. */
    private transient boolean overwriteCompleted;

    private transient Connection connection;
    protected transient Table table;
    protected transient FlinkMetricRegistry flinkMetricRegistry;

    protected transient SinkWriterMetricGroup metricGroup;

    private transient Counter numRecordsOutCounter;
    private transient Counter numRecordsOutErrorsCounter;
    private volatile Throwable asyncWriterException;

    public FlinkSinkWriter(
            TablePath tablePath,
            Configuration flussConfig,
            RowType tableRowType,
            MailboxExecutor mailboxExecutor,
            FlussSerializationSchema<InputT> serializationSchema) {
        this(
                tablePath,
                flussConfig,
                tableRowType,
                null,
                mailboxExecutor,
                serializationSchema,
                null);
    }

    public FlinkSinkWriter(
            TablePath tablePath,
            Configuration flussConfig,
            RowType tableRowType,
            @Nullable int[] targetColumns,
            MailboxExecutor mailboxExecutor,
            FlussSerializationSchema<InputT> serializationSchema) {
        this(
                tablePath,
                flussConfig,
                tableRowType,
                targetColumns,
                mailboxExecutor,
                serializationSchema,
                null);
    }

    public FlinkSinkWriter(
            TablePath tablePath,
            Configuration flussConfig,
            RowType tableRowType,
            @Nullable int[] targetColumns,
            MailboxExecutor mailboxExecutor,
            FlussSerializationSchema<InputT> serializationSchema,
            @Nullable String fixedPartitionName) {
        this(
                tablePath,
                flussConfig,
                tableRowType,
                targetColumns,
                mailboxExecutor,
                serializationSchema,
                fixedPartitionName,
                null);
    }

    public FlinkSinkWriter(
            TablePath tablePath,
            Configuration flussConfig,
            RowType tableRowType,
            @Nullable int[] targetColumns,
            MailboxExecutor mailboxExecutor,
            FlussSerializationSchema<InputT> serializationSchema,
            @Nullable String fixedPartitionName,
            @Nullable String overwriteOriginPartitionName) {
        this.tablePath = tablePath;
        this.flussConfig = flussConfig;
        this.targetColumnIndexes = targetColumns;
        this.tableRowType = tableRowType;
        this.mailboxExecutor = mailboxExecutor;
        this.serializationSchema = serializationSchema;
        this.fixedPartitionName = fixedPartitionName;
        this.overwriteOriginPartitionName = overwriteOriginPartitionName;
    }

    public void initialize(SinkWriterMetricGroup metricGroup) {
        LOG.info(
                "Opening Fluss {}, database: {} and table: {}",
                this.getClass().getSimpleName(),
                tablePath.getDatabaseName(),
                tablePath.getTableName());
        this.metricGroup = metricGroup;
        flinkMetricRegistry =
                new FlinkMetricRegistry(
                        metricGroup, Collections.singleton(MetricNames.WRITER_SEND_LATENCY_MS));
        connection = ConnectionFactory.createConnection(flussConfig, flinkMetricRegistry);
        table = connection.getTable(tablePath);
        LOG.info(
                "Current Fluss Schema is {}, input Flink RowType is {}",
                table.getTableInfo().getSchema(),
                tableRowType);
        sanityCheck(table.getTableInfo());

        try {
            this.serializationSchema.open(
                    new SerializerInitContextImpl(
                            table.getTableInfo().getRowType(), tableRowType, false));
        } catch (Exception e) {
            throw new FlussRuntimeException(e);
        }

        initMetrics();
    }

    protected void initMetrics() {
        numRecordsOutCounter = metricGroup.getNumRecordsSendCounter();
        numRecordsOutErrorsCounter = metricGroup.getNumRecordsOutErrorsCounter();
        metricGroup.setCurrentSendTimeGauge(this::computeSendTime);
    }

    @Override
    public void write(InputT inputValue, Context context) throws IOException, InterruptedException {
        checkAsyncException();

        try {
            RowWithOp rowWithOp = serializationSchema.serialize(inputValue);
            OperationType opType = rowWithOp.getOperationType();
            InternalRow row = rowWithOp.getRow();
            if (opType == OperationType.IGNORE) {
                // skip writing the row
                return;
            }
            CompletableFuture<?> writeFuture = writeRow(opType, row);
            writeFuture.whenComplete(
                    (ignored, throwable) -> {
                        if (throwable != null) {
                            if (this.asyncWriterException == null) {
                                this.asyncWriterException = throwable;
                            }

                            // Checking for exceptions from previous writes
                            mailboxExecutor.execute(
                                    this::checkAsyncException, "Update error metric");
                        }
                    });

            numRecordsOutCounter.inc();
        } catch (Exception e) {
            throw new IOException(e.getMessage(), e);
        }
    }

    @Override
    public abstract void flush(boolean endOfInput) throws IOException, InterruptedException;

    /**
     * INSERT OVERWRITE new-partition PoC: once all input has been written and flushed, atomically
     * repoint the origin partition name to the internal shadow partition holding the new data via
     * the swapPartition admin RPC. The old data stays under the internal name for deferred cleanup.
     * Should be called by subclasses at the end of {@code flush(endOfInput)} after all pending
     * writes are acknowledged.
     */
    protected void completeOverwriteIfNeeded(boolean endOfInput) throws IOException {
        if (!endOfInput
                || overwriteCompleted
                || fixedPartitionName == null
                || overwriteOriginPartitionName == null) {
            return;
        }
        try {
            connection
                    .getAdmin()
                    .swapPartition(tablePath, fixedPartitionName, overwriteOriginPartitionName)
                    .get();
            overwriteCompleted = true;
            LOG.info(
                    "Committed INSERT OVERWRITE for table {}: partition '{}' now points to the "
                            + "data written into internal partition '{}'.",
                    tablePath,
                    overwriteOriginPartitionName,
                    fixedPartitionName);
        } catch (Exception e) {
            throw new IOException(
                    String.format(
                            "Failed to swap overwrite partition '%s' with origin partition '%s' for table %s",
                            fixedPartitionName, overwriteOriginPartitionName, tablePath),
                    e);
        }
    }

    abstract CompletableFuture<?> writeRow(OperationType opType, InternalRow internalRow);

    @Override
    public void close() throws Exception {
        try {
            if (table != null) {
                table.close();
            }
        } catch (Exception e) {
            LOG.warn("Exception occurs while closing Fluss Table.", e);
        }
        table = null;

        try {
            if (connection != null) {
                connection.close();
            }
        } catch (Exception e) {
            LOG.warn("Exception occurs while closing Fluss Connection.", e);
        }
        connection = null;

        if (flinkMetricRegistry != null) {
            flinkMetricRegistry.close();
        }
        flinkMetricRegistry = null;

        // Rethrow exception for the case in which close is called before writer() and flush().
        checkAsyncException();

        LOG.info("Finished closing Fluss sink function.");
    }

    private void sanityCheck(TableInfo flussTableInfo) {
        // when it's UpsertSinkWriter, it means it has primary key got from Flink's metadata
        boolean hasPrimaryKey = this instanceof UpsertSinkWriter;
        if (flussTableInfo.hasPrimaryKey() != hasPrimaryKey) {
            throw new ValidationException(
                    String.format(
                            "Primary key constraint is not matched between metadata in Fluss (%s) and Flink (%s).",
                            flussTableInfo.hasPrimaryKey(), hasPrimaryKey));
        }
    }

    private long computeSendTime() {
        if (flinkMetricRegistry == null) {
            return -1;
        }

        Metric writerSendLatencyMs =
                flinkMetricRegistry.getFlussMetric(MetricNames.WRITER_SEND_LATENCY_MS);
        if (writerSendLatencyMs == null) {
            return -1;
        }

        return ((Gauge<Long>) writerSendLatencyMs).getValue();
    }

    /**
     * This method should only be invoked in the mailbox thread since the counter is not volatile.
     * Logic needs to be invoked by write AND flush since we support various semantics.
     */
    protected void checkAsyncException() throws IOException {
        // reset this exception since we could close the writer later on
        Throwable throwable = asyncWriterException;
        if (throwable != null) {
            asyncWriterException = null;
            numRecordsOutErrorsCounter.inc();
            LOG.error("Exception occurs while write row to fluss.", throwable);
            throw new IOException(
                    "One or more Fluss Writer send requests have encountered exception", throwable);
        }
    }

    @VisibleForTesting
    abstract TableWriter getTableWriter();
}
