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
 * Thrown when the WAL (CDC log) batch generated for a single primary-key write batch requires more
 * memory than the tablet server's shared memory pool can ever provide.
 *
 * <p>Since the pool's pages are only returned when the batch holding them completes, a batch that
 * already holds every page of the pool and needs one more can never complete. Retrying such a batch
 * is pointless, so this exception is not retriable: the client should fail the batch immediately.
 * It can be mitigated by reducing {@code client.writer.batch-size} or increasing {@code
 * server.buffer.memory-size}.
 *
 * @since 1.1
 */
@PublicEvolving
public class WalRecordBatchTooLargeException extends ApiException {

    private static final long serialVersionUID = 1L;

    public WalRecordBatchTooLargeException(String message) {
        super(message);
    }
}
