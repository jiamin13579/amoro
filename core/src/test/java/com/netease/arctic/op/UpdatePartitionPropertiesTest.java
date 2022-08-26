/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.netease.arctic.op;

import com.netease.arctic.TableTestBase;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.util.StructLikeMap;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

public class UpdatePartitionPropertiesTest extends TableTestBase {

  @Test
  public void testUpdatePartitionProperties() {
    StructLikeMap<Map<String, String>> partitionProperties = testTable.partitionProperty();
    Assert.assertEquals(0, partitionProperties.size());
    StructLike p0 = GenericRecord.create(SPEC.partitionType());
    p0.set(0, 1200);
    testTable.updatePartitionProperties(null).set(p0, "key", "value").commit();
    partitionProperties = testTable.partitionProperty();
    Assert.assertEquals(1, partitionProperties.size());
    Assert.assertEquals("value", partitionProperties.get(p0).get("key"));
  }

  public void testUpdatePartitionPropertiesInTx() {
    StructLikeMap<Map<String, String>> partitionProperties = testTable.partitionProperty();
    Transaction transaction = testTable.newTransaction();
    Assert.assertEquals(0, partitionProperties.size());
    StructLike p0 = GenericRecord.create(SPEC.partitionType());
    p0.set(0, 1200);
    testTable.updatePartitionProperties(transaction).set(p0, "key", "value").commit();
    partitionProperties = testTable.partitionProperty();
    Assert.assertEquals(0, partitionProperties.size());
    transaction.commitTransaction();
    partitionProperties = testTable.partitionProperty();
    Assert.assertEquals(1, partitionProperties.size());
    Assert.assertEquals("value", partitionProperties.get(p0).get("key"));
  }
}