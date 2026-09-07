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

package org.apache.fluss.memory;

import org.apache.fluss.annotation.Internal;

/**
 * Internal unchecked signal thrown by a paged output view backed by a bounded {@link
 * MemorySegmentPool} when the view already holds every page of the pool and needs one more.
 *
 * <p>Since the pool returns pages only when the batch holding them completes, and that batch is the
 * one using this view, the allocation can never succeed — neither by waiting nor by retrying the
 * same batch. Callers on the write path should translate this signal into a protocol-level
 * non-retryable error (e.g. {@code WalRecordBatchTooLargeException}) instead of letting it escape.
 */
@Internal
public class MemorySegmentPoolExhaustedException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public MemorySegmentPoolExhaustedException(String message) {
        super(message);
    }
}
