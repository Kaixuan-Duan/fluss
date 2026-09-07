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

package org.apache.fluss.exception;

import org.apache.fluss.annotation.PublicEvolving;

/**
 * Thrown when generating the WAL (CDC log) for a primary-key write batch timed out waiting for
 * pages from the tablet server's shared memory pool.
 *
 * <p>Unlike {@link WalRecordBatchTooLargeException}, the timed-out batch does not hold the whole
 * pool: the remaining pages belong to other in-flight batches and may become available once those
 * batches complete, so retrying after a backoff may succeed. This exception is therefore retriable.
 *
 * @since 1.1
 */
@PublicEvolving
public class MemoryPoolTimeoutException extends RetriableException {

    private static final long serialVersionUID = 1L;

    public MemoryPoolTimeoutException(String message) {
        super(message);
    }

    public MemoryPoolTimeoutException(String message, Throwable cause) {
        super(message, cause);
    }
}
