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

package org.apache.fluss.client.table.writer;

import org.apache.fluss.client.write.WriterClient;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;

import javax.annotation.Nullable;

/** API for configuring and creating {@link AppendWriter}. */
public class TableAppend implements Append {

    private final TablePath tablePath;
    private final TableInfo tableInfo;
    private final WriterClient writerClient;
    private final @Nullable String fixedPartitionName;

    public TableAppend(TablePath tablePath, TableInfo tableInfo, WriterClient writerClient) {
        this(tablePath, tableInfo, writerClient, null);
    }

    private TableAppend(
            TablePath tablePath,
            TableInfo tableInfo,
            WriterClient writerClient,
            @Nullable String fixedPartitionName) {
        this.tablePath = tablePath;
        this.tableInfo = tableInfo;
        this.writerClient = writerClient;
        this.fixedPartitionName = fixedPartitionName;
    }

    @Override
    public Append toPartition(String partitionName) {
        return new TableAppend(tablePath, tableInfo, writerClient, partitionName);
    }

    @Override
    public AppendWriter createWriter() {
        return new AppendWriterImpl(tablePath, tableInfo, writerClient, fixedPartitionName);
    }

    @Override
    public <T> TypedAppendWriter<T> createTypedWriter(Class<T> pojoClass) {
        return new TypedAppendWriterImpl<>(createWriter(), pojoClass, tableInfo);
    }
}
