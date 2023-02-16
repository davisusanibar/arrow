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

import java.nio.file.Paths;

import org.apache.arrow.c.ArrowArrayStream;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestSubstraitConsumer {
  private RootAllocator allocator = null;

  @Before
  public void setUp() {
    allocator = new RootAllocator(Long.MAX_VALUE);
  }

  @After
  public void tearDown() {
    allocator.close();
  }

  protected RootAllocator rootAllocator() {
    return allocator;
  }

  @Test
  public void testSubstraitConsumer() throws Exception {
    try (ArrowArrayStream arrowArrayStream = ArrowArrayStream.allocateNew(rootAllocator())) {
      System.out.println(getSubstraitPlan(getAbsolutePathOfParquetFiles("binary.parquet")));
      if (!org.apache.arrow.dataset.substrait.JniWrapper.get().executeSerializedPlan(getSubstraitPlan(getAbsolutePathOfParquetFiles("binary.parquet")), arrowArrayStream.memoryAddress())) {
        System.out.println("Review JNI Wrapper");
      }
      try (ArrowReader arrowReader = Data.importArrayStream(rootAllocator(), arrowArrayStream)){
        System.out.println("Value is: " + arrowReader.loadNextBatch());  // False
        System.out.println(arrowReader.getVectorSchemaRoot().contentToTSVString());
        // It prints:
//        foo     __fragment_index        __batch_index   __last_in_fragment      __filename
//        [B@1e34c607     0       0       true    /Users/dsusanibar/voltron/jiraarrow/fork/arrow/cpp/submodules/parquet-testing/data/binary.parquet
//        [B@5215cd9a     0       0       true    /Users/dsusanibar/voltron/jiraarrow/fork/arrow/cpp/submodules/parquet-testing/data/binary.parquet
//        [B@36b6964d     0       0       true    /Users/dsusanibar/voltron/jiraarrow/fork/arrow/cpp/submodules/parquet-testing/data/binary.parquet
//        [B@31198ceb     0       0       true    /Users/dsusanibar/voltron/jiraarrow/fork/arrow/cpp/submodules/parquet-testing/data/binary.parquet
//        [B@9257031      0       0       true    /Users/dsusanibar/voltron/jiraarrow/fork/arrow/cpp/submodules/parquet-testing/data/binary.parquet
//        [B@75201592     0       0       true    /Users/dsusanibar/voltron/jiraarrow/fork/arrow/cpp/submodules/parquet-testing/data/binary.parquet
//        [B@7726e185     0       0       true    /Users/dsusanibar/voltron/jiraarrow/fork/arrow/cpp/submodules/parquet-testing/data/binary.parquet
//        [B@aa5455e      0       0       true    /Users/dsusanibar/voltron/jiraarrow/fork/arrow/cpp/submodules/parquet-testing/data/binary.parquet
//        [B@282308c3     0       0       true    /Users/dsusanibar/voltron/jiraarrow/fork/arrow/cpp/submodules/parquet-testing/data/binary.parquet
//        [B@5dda14d0     0       0       true    /Users/dsusanibar/voltron/jiraarrow/fork/arrow/cpp/submodules/parquet-testing/data/binary.parquet
//        [B@1db0ec27     0       0       true    /Users/dsusanibar/voltron/jiraarrow/fork/arrow/cpp/submodules/parquet-testing/data/binary.parquet
//        [B@3d9fc57a     0       0       true    /Users/dsusanibar/voltron/jiraarrow/fork/arrow/cpp/submodules/parquet-testing/data/binary.parquet
        System.out.println("Schema: " + arrowReader.getVectorSchemaRoot().getSchema());
        // It prints: Schema<foo: Binary, __fragment_index: Int(32, true), __batch_index: Int(32, true), __last_in_fragment: Bool, __filename: Utf8>
        System.out.println("Rows: " + arrowReader.getVectorSchemaRoot().getRowCount());
      }
    }
  }

  public static String getAbsolutePathOfParquetFiles(String fileName){
    return java.nio.file.Path.of(
        Paths.get(".")
            .toAbsolutePath()
            .getParent()
            .getParent()
            .getParent()
            .toString(),
        "cpp", "submodules", "parquet-testing", "data", fileName).toString();
  }

  public static String getSubstraitPlan(String absolutePathOfParquetFiles){
    return "{\n" +
        "    \"version\": { \"major_number\": 9999, \"minor_number\": 9999, \"patch_number\": 9999 },\n" +
        "    \"relations\": [\n" +
        "      {\"rel\": {\n" +
        "        \"read\": {\n" +
        "          \"base_schema\": {\n" +
        "            \"struct\": {\n" +
        "              \"types\": [\n" +
        "                         {\"binary\": {}}\n" +
        "                       ]\n" +
        "            },\n" +
        "            \"names\": [\n" +
        "                      \"foo\"\n" +
        "                      ]\n" +
        "          },\n" +
        "          \"local_files\": {\n" +
        "            \"items\": [\n" +
        "              {\n" +
        "                \"uri_file\": \"file://" + absolutePathOfParquetFiles + "\",\n" +
        "                \"parquet\": {}\n" +
        "              }\n" +
        "            ]\n" +
        "          }\n" +
        "        }\n" +
        "      }}\n" +
        "    ]\n" +
        "  }";
  }
}
