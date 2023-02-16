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

package org.apache.arrow.dataset.substrait;

import java.io.IOException;

import org.apache.arrow.c.ArrowArrayStream;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.ipc.ArrowReader;

public class SubstraitConsumer implements AutoCloseable {
  private final BufferAllocator allocator;

  public SubstraitConsumer(BufferAllocator allocator) {
    this.allocator = allocator;
  }

  public ArrowReader runQuery(String plan) throws IOException {
    try (ArrowArrayStream arrowArrayStream = ArrowArrayStream.allocateNew(this.allocator)) {
      if (!JniWrapper.get().executeSerializedPlan(plan, arrowArrayStream.memoryAddress())) {
        throw new IllegalArgumentException("Review Substrait plan definition.");
      }
      return Data.importArrayStream(this.allocator, arrowArrayStream);
    }
  }

  @Override
  public void close() throws Exception {
    //FIXME! Review what objects will be closed here
  }
}
