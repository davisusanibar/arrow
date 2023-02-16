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

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.junit.After;
import static org.junit.Assert.assertTrue;
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
  public void testRunQuery() throws Exception {
    String plan = getSubstraitPlan(getAbsolutePathOfParquetFiles("binary.parquet"));
    try (SubstraitConsumer sub = new SubstraitConsumer(rootAllocator());
         ArrowReader reader = sub.runQuery(plan)) {
      while (reader.loadNextBatch()){
        System.out.println(reader.getVectorSchemaRoot().contentToTSVString());
        // FIXME! Define a new way to validate assert
        assertTrue(reader.getVectorSchemaRoot().getRowCount() > 0);
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
