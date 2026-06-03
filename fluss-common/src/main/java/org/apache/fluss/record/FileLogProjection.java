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

package org.apache.fluss.record;

import org.apache.fluss.annotation.VisibleForTesting;
import org.apache.fluss.compression.ArrowCompressionInfo;
import org.apache.fluss.compression.FlussLZ4BlockOutputStream;
import org.apache.fluss.exception.InvalidColumnProjectionException;
import org.apache.fluss.metadata.SchemaGetter;
import org.apache.fluss.record.FileLogInputStream.FileChannelLogRecordBatch;
import org.apache.fluss.record.bytesview.BytesView;
import org.apache.fluss.record.bytesview.MultiBytesView;
import org.apache.fluss.shaded.arrow.com.google.flatbuffers.FlatBufferBuilder;
import org.apache.fluss.shaded.arrow.org.apache.arrow.flatbuf.Buffer;
import org.apache.fluss.shaded.arrow.org.apache.arrow.flatbuf.FieldNode;
import org.apache.fluss.shaded.arrow.org.apache.arrow.flatbuf.Message;
import org.apache.fluss.shaded.arrow.org.apache.arrow.flatbuf.RecordBatch;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.TypeLayout;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.compression.CompressionUtil;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.ipc.WriteChannel;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.ipc.message.ArrowBodyCompression;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.ipc.message.ArrowBuffer;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.ipc.message.ArrowFieldNode;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.types.pojo.Field;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.types.pojo.Schema;
import org.apache.fluss.types.RowType;
import org.apache.fluss.utils.ArrowUtils;
import org.apache.fluss.utils.IOUtils;
import org.apache.fluss.utils.types.Tuple2;

import com.github.luben.zstd.Zstd;

import javax.annotation.Nullable;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collections;
import java.util.List;

import static org.apache.fluss.record.DefaultLogRecordBatch.APPEND_ONLY_FLAG_MASK;
import static org.apache.fluss.record.LogRecordBatchFormat.LENGTH_OFFSET;
import static org.apache.fluss.record.LogRecordBatchFormat.LOG_MAGIC_VALUE_V0;
import static org.apache.fluss.record.LogRecordBatchFormat.LOG_MAGIC_VALUE_V1;
import static org.apache.fluss.record.LogRecordBatchFormat.LOG_MAGIC_VALUE_V2;
import static org.apache.fluss.record.LogRecordBatchFormat.LOG_OVERHEAD;
import static org.apache.fluss.record.LogRecordBatchFormat.MAGIC_OFFSET;
import static org.apache.fluss.record.LogRecordBatchFormat.V0_RECORD_BATCH_HEADER_SIZE;
import static org.apache.fluss.record.LogRecordBatchFormat.V1_RECORD_BATCH_HEADER_SIZE;
import static org.apache.fluss.record.LogRecordBatchFormat.V2_RECORD_BATCH_HEADER_SIZE;
import static org.apache.fluss.record.LogRecordBatchFormat.attributeOffset;
import static org.apache.fluss.record.LogRecordBatchFormat.recordBatchHeaderSize;
import static org.apache.fluss.record.LogRecordBatchFormat.recordsCountOffset;
import static org.apache.fluss.record.LogRecordBatchFormat.schemaIdOffset;
import static org.apache.fluss.record.LogRecordBatchFormat.statisticsLengthOffset;
import static org.apache.fluss.utils.FileUtils.readFully;
import static org.apache.fluss.utils.FileUtils.readFullyOrFail;
import static org.apache.fluss.utils.Preconditions.checkNotNull;
import static org.apache.fluss.utils.Preconditions.checkState;

/** Column projection util on Arrow format {@link FileLogRecords}. */
public class FileLogProjection {

    // see the arrow binary message format in the page:
    // https://arrow.apache.org/docs/format/Columnar.html#encapsulated-message-format
    private static final int ARROW_IPC_CONTINUATION_LENGTH = 4;
    private static final int ARROW_IPC_METADATA_SIZE_OFFSET = ARROW_IPC_CONTINUATION_LENGTH;
    private static final int ARROW_IPC_METADATA_SIZE_LENGTH = 4;
    private static final int ARROW_HEADER_SIZE =
            ARROW_IPC_CONTINUATION_LENGTH + ARROW_IPC_METADATA_SIZE_LENGTH;

    // the projection cache shared in the TabletServer
    private final ProjectionPushdownCache projectionsCache;

    // shared resources for multiple projections
    private final ByteArrayOutputStream outputStream;
    private final WriteChannel writeChannel;

    /**
     * Buffer to read log records batch header. V1 is larger than V0, so use V1 head buffer can read
     * V0 header even if there is no enough bytes in log file.
     */
    private final ByteBuffer logHeaderBuffer = ByteBuffer.allocate(V2_RECORD_BATCH_HEADER_SIZE);

    private final ByteBuffer arrowHeaderBuffer = ByteBuffer.allocate(ARROW_HEADER_SIZE);
    private ByteBuffer arrowMetadataBuffer;
    private SchemaGetter schemaGetter;
    private long tableId;
    private ArrowCompressionInfo compressionInfo;
    private int[] selectedFieldPositions;
    private int targetSchemaId;

    public FileLogProjection(ProjectionPushdownCache projectionsCache) {
        this.projectionsCache = projectionsCache;
        this.outputStream = new ByteArrayOutputStream();
        this.writeChannel = new WriteChannel(Channels.newChannel(outputStream));
        // fluss use little endian for encoding log records batch
        this.logHeaderBuffer.order(ByteOrder.LITTLE_ENDIAN);
        // arrow force use little endian to encode int32 values
        this.arrowHeaderBuffer.order(ByteOrder.LITTLE_ENDIAN);
    }

    public void setCurrentProjection(
            long tableId,
            SchemaGetter schemaGetter,
            ArrowCompressionInfo compressionInfo,
            int[] selectedFieldPositions,
            int targetSchemaId) {
        this.tableId = tableId;
        this.schemaGetter = schemaGetter;
        this.compressionInfo = compressionInfo;
        this.selectedFieldPositions = selectedFieldPositions;
        this.targetSchemaId = targetSchemaId;
    }

    /**
     * Project a single record batch to a subset of fields. This is used by the filter path where
     * batches are iterated individually rather than as a contiguous file region.
     *
     * @param batch the file channel log record batch to project
     * @return the projected bytes view
     */
    public BytesView projectRecordBatch(FileChannelLogRecordBatch batch) throws IOException {
        FileChannel channel = batch.fileRecords.channel();
        int position = batch.position();

        // Schema ID determines which projection mapping to use (handles schema evolution).
        logHeaderBuffer.rewind();
        readLogHeaderFullyOrFail(channel, logHeaderBuffer, position);
        logHeaderBuffer.rewind();
        byte magic = logHeaderBuffer.get(MAGIC_OFFSET);
        int recordBatchHeaderSize = recordBatchHeaderSize(magic);
        int batchSizeInBytes = LOG_OVERHEAD + logHeaderBuffer.getInt(LENGTH_OFFSET);
        short schemaId = logHeaderBuffer.getShort(schemaIdOffset(magic));

        ProjectionInfo currentProjection = getOrCreateProjectionInfo(schemaId);
        checkNotNull(currentProjection, "There is no projection registered yet.");

        MultiBytesView.Builder builder = MultiBytesView.builder();

        // Empty batches (header-only) can occur for CDC log batches with no changes;
        // return empty projection to preserve offset advancement.
        if (batchSizeInBytes == recordBatchHeaderSize) {
            return builder.build();
        }

        projectSingleBatch(channel, position, currentProjection, builder, Integer.MAX_VALUE);
        return builder.build();
    }

    /**
     * Project the log records to a subset of fields and the size of returned log records shouldn't
     * exceed maxBytes.
     *
     * @return the projected records.
     */
    public BytesViewLogRecords project(FileChannel channel, int start, int end, int maxBytes)
            throws IOException {

        MultiBytesView.Builder builder = MultiBytesView.builder();
        int position = start;

        ProjectionInfo currentProjection = null;
        short prevSchemaId = -1;
        // The condition is an optimization to avoid read log header when there is no enough bytes,
        // So we use V0 header size here for a conservative judgment. In the end, the condition
        // of (position >= end - recordBatchHeaderSize) will ensure the final correctness.
        while (maxBytes > V0_RECORD_BATCH_HEADER_SIZE) {
            if (position > end - V0_RECORD_BATCH_HEADER_SIZE) {
                // the remaining bytes in the file are not enough to read a batch header up to
                // magic.
                return new BytesViewLogRecords(builder.build());
            }
            // read log header
            logHeaderBuffer.rewind();
            readLogHeaderFullyOrFail(channel, logHeaderBuffer, position);

            logHeaderBuffer.rewind();
            byte magic = logHeaderBuffer.get(MAGIC_OFFSET);
            int recordBatchHeaderSize = recordBatchHeaderSize(magic);
            int batchSizeInBytes = LOG_OVERHEAD + logHeaderBuffer.getInt(LENGTH_OFFSET);
            short schemaId = logHeaderBuffer.getShort(schemaIdOffset(magic));

            // reuse projection in the current log file
            if (currentProjection == null || prevSchemaId != schemaId) {
                prevSchemaId = schemaId;
                currentProjection = getOrCreateProjectionInfo(schemaId);
            }

            if (position > end - batchSizeInBytes) {
                // the remaining bytes in the file are not enough to read a full batch
                return new BytesViewLogRecords(builder.build());
            }

            // Return empty batch to push forward log offset. The empty batch was generated when
            // build cdc log batch when there
            // is no cdc log generated for this kv batch. See the comments about the field
            // 'lastOffsetDelta' in DefaultLogRecordBatch.
            if (batchSizeInBytes == recordBatchHeaderSize) {
                builder.addBytes(channel, position, batchSizeInBytes);
                position += batchSizeInBytes;
                continue;
            }

            int newBatchSizeInBytes =
                    projectSingleBatch(channel, position, currentProjection, builder, maxBytes);
            if (newBatchSizeInBytes < 0) {
                // the projected batch exceeds the remaining budget, stop here
                return new BytesViewLogRecords(builder.build());
            }

            maxBytes -= newBatchSizeInBytes;
            position += batchSizeInBytes;
        }

        return new BytesViewLogRecords(builder.build());
    }

    /**
     * Project a single non-empty record batch and append the projected bytes to the builder.
     *
     * <p>The caller must have already read the log header into {@link #logHeaderBuffer} and
     * verified that the batch is non-empty (i.e., batchSizeInBytes != recordBatchHeaderSize).
     *
     * @param channel the file channel to read from
     * @param position the start position of the batch in the file
     * @param currentProjection the projection info for the current schema
     * @param builder the builder to append projected bytes to
     * @param maxBytes the maximum allowed projected batch size; returns -1 if exceeded
     * @return the projected batch size in bytes, or -1 if the projected size exceeds maxBytes
     */
    private int projectSingleBatch(
            FileChannel channel,
            int position,
            ProjectionInfo currentProjection,
            MultiBytesView.Builder builder,
            int maxBytes)
            throws IOException {
        logHeaderBuffer.rewind();
        byte magic = logHeaderBuffer.get(MAGIC_OFFSET);
        int recordBatchHeaderSize = recordBatchHeaderSize(magic);

        boolean isAppendOnly =
                (logHeaderBuffer.get(attributeOffset(magic)) & APPEND_ONLY_FLAG_MASK) > 0;

        // For V1+, skip statistics data between header and records
        int statisticsLength = 0;
        if (magic >= LOG_MAGIC_VALUE_V1) {
            statisticsLength = logHeaderBuffer.getInt(statisticsLengthOffset(magic));
        }
        int recordsStartOffset = recordBatchHeaderSize + statisticsLength;

        final int changeTypeBytes;
        final long arrowHeaderOffset;
        if (isAppendOnly) {
            changeTypeBytes = 0;
            arrowHeaderOffset = position + recordsStartOffset;
        } else {
            changeTypeBytes = logHeaderBuffer.getInt(recordsCountOffset(magic));
            arrowHeaderOffset = position + recordsStartOffset + changeTypeBytes;
        }

        // read arrow header
        arrowHeaderBuffer.rewind();
        readFullyOrFail(channel, arrowHeaderBuffer, arrowHeaderOffset, "arrow header");
        arrowHeaderBuffer.position(ARROW_IPC_METADATA_SIZE_OFFSET);
        int arrowMetadataSize = arrowHeaderBuffer.getInt();

        resizeArrowMetadataBuffer(arrowMetadataSize);
        arrowMetadataBuffer.rewind();
        readFullyOrFail(
                channel,
                arrowMetadataBuffer,
                arrowHeaderOffset + ARROW_HEADER_SIZE,
                "arrow metadata");

        arrowMetadataBuffer.rewind();
        Message metadata = Message.getRootAsMessage(arrowMetadataBuffer);
        ProjectedArrowBatch projectedArrowBatch = projectArrowBatch(metadata, currentProjection);
        long arrowBodyLength = projectedArrowBatch.bodyLength();

        int newBatchSizeInBytes =
                recordBatchHeaderSize
                        + changeTypeBytes
                        + currentProjection.arrowMetadataLength
                        + (int) arrowBodyLength;

        if (newBatchSizeInBytes > maxBytes) {
            return -1;
        }

        // create new arrow batch metadata which already projected
        byte[] headerMetadata =
                serializeArrowRecordBatchMetadata(
                        projectedArrowBatch, arrowBodyLength, currentProjection.bodyCompression);
        checkState(
                headerMetadata.length == currentProjection.arrowMetadataLength,
                "Invalid metadata length");

        // update and copy log batch header
        logHeaderBuffer.position(LENGTH_OFFSET);
        logHeaderBuffer.putInt(newBatchSizeInBytes - LOG_OVERHEAD);

        // For V1+ format, clear statistics information since projection removes statistics
        LogRecordBatchFormat.clearStatisticsFromHeader(logHeaderBuffer, magic);

        logHeaderBuffer.rewind();
        byte[] logHeader = new byte[recordBatchHeaderSize];
        logHeaderBuffer.get(logHeader);

        // build log records
        builder.addBytes(logHeader);
        if (!isAppendOnly) {
            builder.addBytes(channel, position + recordsStartOffset, changeTypeBytes);
        }
        builder.addBytes(headerMetadata);
        final long bufferOffset = arrowHeaderOffset + ARROW_HEADER_SIZE + arrowMetadataSize;
        projectedArrowBatch.buffers.forEach(
                b -> builder.addBytes(channel, bufferOffset + b.getOffset(), (int) b.getSize()));

        // Write NULL column buffers for missing fields (schema evolution)
        if (projectedArrowBatch.nullColumnsBodyLength > 0
                && projectedArrowBatch.nullColumnCompressedData != null) {
            builder.addBytes(projectedArrowBatch.nullColumnCompressedData);
        }

        return newBatchSizeInBytes;
    }

    private ProjectedArrowBatch projectArrowBatch(Message metadata, ProjectionInfo projectionInfo) {
        List<ArrowFieldNode> newNodes = new ArrayList<>();
        List<ArrowBuffer> newBufferLayouts = new ArrayList<>();
        List<ArrowBuffer> selectedBuffers = new ArrayList<>();
        RecordBatch recordBatch = (RecordBatch) metadata.header(new RecordBatch());
        long numRecords = recordBatch.length();

        // Project existing field nodes
        for (int i = projectionInfo.nodesProjection.nextSetBit(0);
                i >= 0;
                i = projectionInfo.nodesProjection.nextSetBit(i + 1)) {
            FieldNode node = recordBatch.nodes(i);
            newNodes.add(new ArrowFieldNode(node.length(), node.nullCount()));
        }

        // Project existing field buffers
        long bodyLength = metadata.bodyLength();
        long newOffset = 0L;
        for (int i = projectionInfo.buffersProjection.nextSetBit(0);
                i >= 0;
                i = projectionInfo.buffersProjection.nextSetBit(i + 1)) {
            Buffer buf = recordBatch.buffers(i);
            long nextOffset =
                    i < projectionInfo.bufferCount - 1
                            ? recordBatch.buffers(i + 1).offset()
                            : bodyLength;
            long paddedLength = nextOffset - buf.offset();
            selectedBuffers.add(new ArrowBuffer(buf.offset(), paddedLength));
            newBufferLayouts.add(new ArrowBuffer(newOffset, buf.length()));
            newOffset += paddedLength;
        }

        // Append NULL columns for missing fields (schema evolution: newly added columns)
        NullColumnsMetadata nullColumns = null;
        if (projectionInfo.nullColumnsArrowSchema != null) {
            nullColumns =
                    appendNullColumns(
                            projectionInfo.nullColumnsArrowSchema,
                            numRecords,
                            newNodes,
                            newBufferLayouts,
                            newOffset);
        }

        return new ProjectedArrowBatch(
                numRecords,
                newNodes,
                newBufferLayouts,
                selectedBuffers,
                nullColumns != null ? nullColumns.bodyLength : 0L,
                nullColumns != null ? nullColumns.bufferData : null);
    }

    /**
     * Append NULL column field nodes, buffer layouts, and buffer data for columns that are missing
     * from the old schema (newly added columns via schema evolution).
     *
     * <p>Each NULL column gets a field node with nullCount = numRecords, a validity buffer
     * (all-zero bits indicating all NULL), and zero-length data/offset buffers. If compression is
     * enabled, the validity buffer data is compressed to match the batch's compression format.
     *
     * @param nullColumnsSchema the Arrow schema of the missing columns
     * @param numRecords the number of records in the batch
     * @param nodes the list to append field nodes to
     * @param bufferLayouts the list to append buffer layouts to
     * @param startOffset the starting offset for buffer layout positions
     * @return metadata about the NULL column buffers (body length and data)
     */
    private NullColumnsMetadata appendNullColumns(
            Schema nullColumnsSchema,
            long numRecords,
            List<ArrowFieldNode> nodes,
            List<ArrowBuffer> bufferLayouts,
            long startOffset) {

        // Flatten the NULL columns schema to get all field nodes (including nested types)
        List<Field> nullFields = new ArrayList<>();
        flattenNullFields(nullColumnsSchema.getFields(), nullFields);

        boolean needsCompression = compressionInfo != ArrowCompressionInfo.NO_COMPRESSION;
        ByteArrayOutputStream nullBufOut = new ByteArrayOutputStream();
        long nullColumnsBodyLength = 0L;
        long newOffset = startOffset;

        for (Field nullField : nullFields) {
            // Add field node: all records are NULL (nullCount = numRecords)
            nodes.add(new ArrowFieldNode(numRecords, numRecords));

            // Add buffer layouts for this NULL field
            int bufferCountForType = TypeLayout.getTypeBufferCount(nullField.getType());
            for (int bufIdx = 0; bufIdx < bufferCountForType; bufIdx++) {
                if (bufIdx == 0) {
                    newOffset =
                            appendNullValidityBuffer(
                                    numRecords,
                                    needsCompression,
                                    bufferLayouts,
                                    nullBufOut,
                                    newOffset);
                    nullColumnsBodyLength = newOffset - startOffset;
                } else {
                    // Data/offset buffers: 0 bytes for all-NULL columns
                    bufferLayouts.add(new ArrowBuffer(newOffset, 0));
                }
            }
        }

        return new NullColumnsMetadata(nullColumnsBodyLength, nullBufOut.toByteArray());
    }

    /**
     * Append a NULL validity buffer (all-zero bits) for a single NULL column.
     *
     * <p>If compression is enabled, the validity buffer data is compressed to match the batch's
     * compression format. Otherwise, raw zero bytes are written.
     *
     * @param numRecords the number of records in the batch
     * @param needsCompression whether compression is enabled for this batch
     * @param bufferLayouts the list to append the buffer layout to
     * @param nullBufOut the output stream to write the buffer data to
     * @param currentOffset the current offset in the body
     * @return the updated offset after appending the validity buffer
     */
    private long appendNullValidityBuffer(
            long numRecords,
            boolean needsCompression,
            List<ArrowBuffer> bufferLayouts,
            ByteArrayOutputStream nullBufOut,
            long currentOffset) {

        // Validity buffer: contains all-zero bits (indicating all NULL)
        long validitySize = (numRecords + 7) / 8;
        long paddedValiditySize = (validitySize + 7) / 8 * 8;

        if (needsCompression) {
            // Compress the validity buffer data
            byte[] uncompressedData = new byte[(int) paddedValiditySize];
            // All bytes are zero by default (all validity bits = 0)
            byte[] compressedData = compressBuffer(uncompressedData, compressionInfo);
            long paddedCompressedSize = (compressedData.length + 7) / 8 * 8;

            // Buffer layout records the compressed size (actual body size)
            bufferLayouts.add(new ArrowBuffer(currentOffset, compressedData.length));
            try {
                nullBufOut.write(compressedData);
                // Pad to 8-byte alignment
                int padding = (int) (paddedCompressedSize - compressedData.length);
                nullBufOut.write(new byte[padding]);
            } catch (IOException e) {
                throw new RuntimeException("Failed to write NULL column buffer data", e);
            }
            return currentOffset + paddedCompressedSize;
        } else {
            // No compression: buffer layout records the uncompressed size
            bufferLayouts.add(new ArrowBuffer(currentOffset, validitySize));
            try {
                nullBufOut.write(new byte[(int) paddedValiditySize]);
            } catch (IOException e) {
                throw new RuntimeException("Failed to write NULL column buffer data", e);
            }
            return currentOffset + paddedValiditySize;
        }
    }

    /** Metadata about NULL column buffers appended during projection. */
    private static final class NullColumnsMetadata {
        final long bodyLength;
        final byte[] bufferData;

        NullColumnsMetadata(long bodyLength, byte[] bufferData) {
            this.bodyLength = bodyLength;
            this.bufferData = bufferData;
        }
    }

    /**
     * Compress a byte array using the given compression info, producing Arrow-compatible compressed
     * output.
     *
     * <p>Arrow compressed buffer format: [4 bytes: uncompressed length (little-endian)][compressed
     * data]
     *
     * <p>We use the underlying Zstd/LZ4 library directly to avoid ArrowBuf lifecycle management
     * issues with Arrow's CompressionCodec API.
     */
    private byte[] compressBuffer(byte[] data, ArrowCompressionInfo compressionInfo) {
        switch (compressionInfo.getCompressionType()) {
            case ZSTD:
                return compressZstd(data, compressionInfo.getCompressionLevel());
            case LZ4_FRAME:
                return compressLz4(data);
            case NONE:
                return data;
            default:
                throw new IllegalArgumentException(
                        "Unsupported compression type: " + compressionInfo.getCompressionType());
        }
    }

    private byte[] compressZstd(byte[] data, int compressionLevel) {
        long maxCompressedSize = Zstd.compressBound(data.length);
        if (Zstd.isError(maxCompressedSize)) {
            throw new RuntimeException(
                    "Zstd compress bound error: " + Zstd.getErrorName(maxCompressedSize));
        }
        // Arrow compressed format: [4 bytes uncompressed length (little-endian)][compressed data]
        int sizeOfUncompressedLength = (int) CompressionUtil.SIZE_OF_UNCOMPRESSED_LENGTH;
        byte[] output = new byte[sizeOfUncompressedLength + (int) maxCompressedSize];
        // Write uncompressed length in little-endian
        output[0] = (byte) data.length;
        output[1] = (byte) (data.length >> 8);
        output[2] = (byte) (data.length >> 16);
        output[3] = (byte) (data.length >> 24);
        long compressedSize =
                Zstd.compressByteArray(
                        output,
                        sizeOfUncompressedLength,
                        (int) maxCompressedSize,
                        data,
                        0,
                        data.length,
                        compressionLevel);
        if (Zstd.isError(compressedSize)) {
            throw new RuntimeException(
                    "Zstd compression error: " + Zstd.getErrorName(compressedSize));
        }
        byte[] result = new byte[sizeOfUncompressedLength + (int) compressedSize];
        System.arraycopy(output, 0, result, 0, result.length);
        return result;
    }

    private byte[] compressLz4(byte[] data) {
        // Use FlussLZ4BlockOutputStream directly to compress the data
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try {
            // Write uncompressed length in little-endian (Arrow compressed format header)
            int sizeOfUncompressedLength = (int) CompressionUtil.SIZE_OF_UNCOMPRESSED_LENGTH;
            byte[] lengthBytes = new byte[sizeOfUncompressedLength];
            lengthBytes[0] = (byte) data.length;
            lengthBytes[1] = (byte) (data.length >> 8);
            lengthBytes[2] = (byte) (data.length >> 16);
            lengthBytes[3] = (byte) (data.length >> 24);
            baos.write(lengthBytes);

            // Compress data using LZ4 block format
            ByteArrayInputStream bais = new ByteArrayInputStream(data);
            try (InputStream in = bais;
                    OutputStream out = new FlussLZ4BlockOutputStream(baos)) {
                IOUtils.copyBytes(in, out);
            }
        } catch (IOException e) {
            throw new RuntimeException("LZ4 compression failed", e);
        }
        return baos.toByteArray();
    }

    /** Flatten fields recursively for NULL column schema (all fields are selected). */
    private void flattenNullFields(List<Field> fields, List<Field> flattened) {
        for (Field field : fields) {
            flattened.add(field);
            if (!field.getChildren().isEmpty()) {
                flattenNullFields(field.getChildren(), flattened);
            }
        }
    }

    /**
     * Serialize metadata of a {@link ArrowRecordBatch}. This avoids to create an instance of {@link
     * ArrowRecordBatch}.
     *
     * @see MessageSerializer#serialize(WriteChannel, ArrowRecordBatch)
     * @see ArrowRecordBatch#writeTo(FlatBufferBuilder)
     */
    private byte[] serializeArrowRecordBatchMetadata(
            ProjectedArrowBatch batch, long arrowBodyLength, ArrowBodyCompression bodyCompression)
            throws IOException {
        outputStream.reset();
        ArrowUtils.serializeArrowRecordBatchMetadata(
                writeChannel,
                batch.numRecords,
                batch.nodes,
                batch.buffersLayout,
                bodyCompression,
                arrowBodyLength);
        return outputStream.toByteArray();
    }

    private void resizeArrowMetadataBuffer(int metadataSize) {
        if (arrowMetadataBuffer == null || arrowMetadataBuffer.capacity() < metadataSize) {
            arrowMetadataBuffer = ByteBuffer.allocate(metadataSize);
            arrowMetadataBuffer.order(ByteOrder.LITTLE_ENDIAN);
        } else {
            arrowMetadataBuffer.limit(metadataSize);
        }
    }

    /** Flatten fields by a pre-order depth-first traversal of the fields in the schema. */
    private void flattenFields(
            List<Field> arrowFields,
            BitSet selectedFields,
            List<Tuple2<Field, Boolean>> flattenedFields) {
        for (int i = 0; i < arrowFields.size(); i++) {
            Field field = arrowFields.get(i);
            boolean selected = selectedFields.get(i);
            flattenedFields.add(Tuple2.of(field, selected));
            List<Field> children = field.getChildren();
            flattenFields(children, fillBitSet(children.size(), selected), flattenedFields);
        }
    }

    private static BitSet toBitSet(int length, int[] selectedIndexes) {
        BitSet bitset = new BitSet(length);
        int prev = -1;
        for (int i : selectedIndexes) {
            if (i < prev) {
                throw new InvalidColumnProjectionException(
                        "The projection indexes should be in field order, but is "
                                + Arrays.toString(selectedIndexes));
            } else if (i == prev) {
                throw new InvalidColumnProjectionException(
                        "The projection indexes should not contain duplicated fields, but is "
                                + Arrays.toString(selectedIndexes));
            } else if (i >= length) {
                // Newly added column not present in old schema; will append NULL vector later.
                continue;
            }
            bitset.set(i);
            prev = i;
        }
        return bitset;
    }

    private static BitSet fillBitSet(int length, boolean value) {
        BitSet bitset = new BitSet(length);
        if (value) {
            bitset.set(0, length);
        } else {
            bitset.clear();
        }
        return bitset;
    }

    /**
     * Read log header fully or fail with EOFException if there is no enough bytes to read a full
     * log header. This handles different log header size for magic v0, v1 and v2.
     */
    static void readLogHeaderFullyOrFail(FileChannel channel, ByteBuffer buffer, int position)
            throws IOException {
        if (position < 0) {
            throw new IllegalArgumentException(
                    "The file channel position cannot be negative, but it is " + position);
        }
        readFully(channel, buffer, position);
        if (buffer.hasRemaining()) {
            int size = buffer.position();
            byte magic = buffer.get(MAGIC_OFFSET);
            if (magic == LOG_MAGIC_VALUE_V0 && size < V0_RECORD_BATCH_HEADER_SIZE) {
                throw new EOFException(
                        String.format(
                                "Failed to read v0 log header from file channel `%s`. Expected to read %d bytes, "
                                        + "but reached end of file after reading %d bytes. Started read from position %d.",
                                channel, V0_RECORD_BATCH_HEADER_SIZE, size, position));
            } else if (magic == LOG_MAGIC_VALUE_V1 && size < V1_RECORD_BATCH_HEADER_SIZE) {
                throw new EOFException(
                        String.format(
                                "Failed to read v1 log header from file channel `%s`. Expected to read %d bytes, "
                                        + "but reached end of file after reading %d bytes. Started read from position %d.",
                                channel, V1_RECORD_BATCH_HEADER_SIZE, size, position));
            } else if (magic == LOG_MAGIC_VALUE_V2 && size < V2_RECORD_BATCH_HEADER_SIZE) {
                throw new EOFException(
                        String.format(
                                "Failed to read v2 log header from file channel `%s`. Expected to read %d bytes, "
                                        + "but reached end of file after reading %d bytes. Started read from position %d.",
                                channel, V2_RECORD_BATCH_HEADER_SIZE, size, position));
            }
        }
    }

    @VisibleForTesting
    ByteBuffer getLogHeaderBuffer() {
        return logHeaderBuffer;
    }

    private ProjectionInfo getOrCreateProjectionInfo(short schemaId) {
        ProjectionInfo cachedProjection =
                projectionsCache.getProjectionInfo(tableId, schemaId, selectedFieldPositions);
        if (cachedProjection == null) {
            cachedProjection = createProjectionInfo(schemaId, selectedFieldPositions);
            projectionsCache.setProjectionInfo(
                    tableId, schemaId, selectedFieldPositions, cachedProjection);
        }
        return cachedProjection;
    }

    private ProjectionInfo createProjectionInfo(short schemaId, int[] selectedFieldPositions) {
        org.apache.fluss.metadata.Schema schema = schemaGetter.getSchema(schemaId);
        RowType rowType = schema.getRowType();
        int oldFieldCount = rowType.getFieldCount();

        // Split selectedFieldPositions into fields that exist in the old schema and fields
        // that are missing (newly added columns via schema evolution).
        int[] existingFields =
                Arrays.stream(selectedFieldPositions).filter(i -> i < oldFieldCount).toArray();
        int[] missingFields =
                Arrays.stream(selectedFieldPositions).filter(i -> i >= oldFieldCount).toArray();

        // initialize the projection util information
        Schema arrowSchema = ArrowUtils.toArrowSchema(rowType);
        BitSet selection = toBitSet(arrowSchema.getFields().size(), selectedFieldPositions);
        List<Tuple2<Field, Boolean>> flattenedFields = new ArrayList<>();
        flattenFields(arrowSchema.getFields(), selection, flattenedFields);
        int totalFieldNodes = flattenedFields.size();
        int[] bufferLayoutCount = new int[totalFieldNodes];
        BitSet nodesProjection = new BitSet(totalFieldNodes);
        int totalBuffers = 0;
        for (int i = 0; i < totalFieldNodes; i++) {
            Field fieldNode = flattenedFields.get(i).f0;
            boolean selected = flattenedFields.get(i).f1;
            nodesProjection.set(i, selected);
            bufferLayoutCount[i] = TypeLayout.getTypeBufferCount(fieldNode.getType());
            totalBuffers += bufferLayoutCount[i];
        }
        BitSet buffersProjection = new BitSet(totalBuffers);
        int bufferIndex = 0;
        for (int i = 0; i < totalFieldNodes; i++) {
            if (nodesProjection.get(i)) {
                buffersProjection.set(bufferIndex, bufferIndex + bufferLayoutCount[i]);
            }
            bufferIndex += bufferLayoutCount[i];
        }

        // Build the projected Arrow schema for existing fields only
        Schema projectedArrowSchema;
        if (existingFields.length > 0) {
            projectedArrowSchema = ArrowUtils.toArrowSchema(rowType.project(existingFields));
        } else {
            // All projected fields are missing from the old schema
            projectedArrowSchema = new Schema(Collections.emptyList());
        }

        // Build the NULL columns Arrow schema for missing fields (from the latest schema)
        Schema nullColumnsArrowSchema = null;
        if (missingFields.length > 0) {
            org.apache.fluss.metadata.Schema latestSchema = schemaGetter.getSchema(targetSchemaId);
            RowType latestRowType = latestSchema.getRowType();
            // Validate that missing fields are within the latest schema's range
            for (int missingField : missingFields) {
                if (missingField >= latestRowType.getFieldCount()) {
                    throw new InvalidColumnProjectionException(
                            "Projected fields "
                                    + Arrays.toString(selectedFieldPositions)
                                    + " is out of bound for schema with "
                                    + latestRowType.getFieldCount()
                                    + " fields.");
                }
            }
            nullColumnsArrowSchema = ArrowUtils.toArrowSchema(latestRowType.project(missingFields));
        }

        ArrowBodyCompression bodyCompression =
                CompressionUtil.createBodyCompression(compressionInfo.createCompressionCodec());

        // Estimate metadata length using the combined schema (existing + NULL columns),
        // since adding two independent estimates would double-count flatbuffers overhead.
        Schema combinedSchema;
        if (nullColumnsArrowSchema != null) {
            List<Field> combinedFields = new ArrayList<>(projectedArrowSchema.getFields());
            combinedFields.addAll(nullColumnsArrowSchema.getFields());
            combinedSchema = new Schema(combinedFields);
        } else {
            combinedSchema = projectedArrowSchema;
        }
        int metadataLength =
                ArrowUtils.estimateArrowMetadataLength(combinedSchema, bodyCompression);
        return new ProjectionInfo(
                nodesProjection,
                buffersProjection,
                bufferIndex,
                metadataLength,
                bodyCompression,
                selectedFieldPositions,
                existingFields,
                missingFields,
                nullColumnsArrowSchema);
    }

    /** Projection pushdown information for a specific schema and selected fields. */
    public static final class ProjectionInfo {
        final BitSet nodesProjection;
        final BitSet buffersProjection;
        final int bufferCount;
        final int arrowMetadataLength;
        final ArrowBodyCompression bodyCompression;
        final int[] selectedFieldPositions;

        /** Field indexes in selectedFieldPositions that exist in the old schema. */
        final int[] existingFields;

        /**
         * Field indexes in selectedFieldPositions that are missing from the old schema (newly added
         * columns via schema evolution).
         */
        final int[] missingFields;

        /** Arrow schema of the missing columns, used to construct NULL column vectors. */
        @Nullable final Schema nullColumnsArrowSchema;

        private ProjectionInfo(
                BitSet nodesProjection,
                BitSet buffersProjection,
                int bufferCount,
                int arrowMetadataLength,
                ArrowBodyCompression bodyCompression,
                int[] selectedFieldPositions,
                int[] existingFields,
                int[] missingFields,
                @Nullable Schema nullColumnsArrowSchema) {
            this.nodesProjection = nodesProjection;
            this.buffersProjection = buffersProjection;
            this.bufferCount = bufferCount;
            this.arrowMetadataLength = arrowMetadataLength;
            this.bodyCompression = bodyCompression;
            this.selectedFieldPositions = selectedFieldPositions;
            this.existingFields = existingFields;
            this.missingFields = missingFields;
            this.nullColumnsArrowSchema = nullColumnsArrowSchema;
        }
    }

    /** Metadata of a projected arrow record batch. */
    static final class ProjectedArrowBatch {
        /** Number of records. */
        final long numRecords;

        /** The projected nodes of {@link ArrowRecordBatch#getNodes()}. */
        final List<ArrowFieldNode> nodes;

        /** The new buffer layouts of the {@link #buffers}. */
        final List<ArrowBuffer> buffersLayout;

        /** The projected buffer positions of {@link ArrowRecordBatch#getBuffers()}. */
        final List<ArrowBuffer> buffers;

        /** Total body length of NULL column buffers (validity bitmaps with all-zero bits). */
        final long nullColumnsBodyLength;

        /** Compressed NULL column buffer data to append to the body. */
        @Nullable final byte[] nullColumnCompressedData;

        public ProjectedArrowBatch(
                long numRecords,
                List<ArrowFieldNode> nodes,
                List<ArrowBuffer> buffersLayout,
                List<ArrowBuffer> buffers,
                long nullColumnsBodyLength,
                @Nullable byte[] nullColumnCompressedData) {
            this.numRecords = numRecords;
            this.nodes = nodes;
            this.buffersLayout = buffersLayout;
            this.buffers = buffers;
            this.nullColumnsBodyLength = nullColumnsBodyLength;
            this.nullColumnCompressedData = nullColumnCompressedData;
        }

        public long bodyLength() {
            long bodyLength = 0;
            for (ArrowBuffer buffer : buffers) {
                bodyLength += buffer.getSize();
            }
            bodyLength += nullColumnsBodyLength;
            return bodyLength;
        }
    }
}
